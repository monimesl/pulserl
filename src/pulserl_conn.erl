%%%-------------------------------------------------------------------
%%% @author Alpha Umaru Shaw <shawalpha5@gmail.com>
%%% @doc
%%%
%%% @end
%%% Copyright: (C) 2020, Skulup Ltd
%%%-------------------------------------------------------------------
-module(pulserl_conn).


-behaviour(gen_server).

-include("pulserl.hrl").
-include("pulsar_api.hrl").


%% API
-export([start_link/1]).

-export([create/1, close/1]).

-export([register_handler/3, send_simple_command/2, send_payload_command/4]).

%% gen_server callbacks
-export([init/1,
  handle_call/3,
  handle_cast/2,
  handle_info/2,
  terminate/2,
  code_change/3]).

-define(NO_CONNECTION_ERROR, {error, no_connection}).
-define(LOST_CONNECTION_ERROR, {error, lost_connection}).
-define(CLOSED_CONNECTION_ERROR, {error, closed_connection}).

-define(RECONNECT_INTERVAL, 1000).
-define(STATE_READY, ready).


%%%===================================================================
%%% API
%%%===================================================================

send_simple_command(Pid, Command) when is_tuple(Command) ->
  gen_server:call(Pid, {send_command, Command}, timer:seconds(10)).

send_payload_command(Pid, Command, Metadata, Payload) when is_tuple(Command) ->
  gen_server:cast(Pid, {send_command, {payload, Command, Metadata, Payload}}).

register_handler(Pid, Handler, Type) when is_pid(Handler), Type == producer; Type == consumer ->
  gen_server:call(Pid, {register_handler, Handler, Type}).

create(Options) ->
  supervisor:start_child(pulserl_conn_sup, [Options]).

close(Pid) ->
  gen_server:cast(Pid, close).

%%%===================================================================
%%% Gen Server API
%%%===================================================================

start_link(Args) ->
  gen_server:start_link(?MODULE, Args, []).


%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

-record(state, {
  state,
  socket,
  data_buffer = <<>>,
  socket_address,
  logical_address,
  tcp_options,
  %% Server's
  server_version,
  protocol_version,
  max_message_size,
  reconnect_timer,
  %% conn states
  request_id = 1,
  producer_id = 1,
  consumer_id = 1,
  consumers = dict:new(),
  producers = dict:new(),
  waiters = dict:new(),
  waiter_monitor2Id = dict:new()

}).


init(Opts) ->
  State = #state{
    logical_address = proplists:get_value(logical_address, Opts),
    tcp_options = [
      {nodelay, proplists:get_value(nodelay, Opts, true)},
      {keepalive, proplists:get_value(keepalive, Opts, true)}
    ]
  },
  case create_connection(State) of
    {ok, NewState} ->
      case perform_handshake(NewState) of
        {ok, #state{socket = Socket} = Ns} ->
          Socket2 = activate_socket(Socket),
          {ok, notify_client_of_up(Ns#state{socket = Socket2})};
        {error, Reason} ->
          {stop, Reason}
      end;
    {error, Reason} ->
      {stop, Reason}
  end.


%% Call callback
handle_call({register_handler, Consumer, consumer}, _From,
    #state{consumer_id = Id} = State) ->
  erlang:monitor(process, Consumer),
  Consumers = dict:store(Id, Consumer, State#state.consumers),
  {reply, Id, State#state{consumer_id = Id + 1, consumers = Consumers}};

handle_call({register_handler, Producer, producer}, _From,
    #state{producer_id = Id} = State) ->
  erlang:monitor(process, Producer),
  Producers = dict:store(Id, Producer, State#state.producers),
  {reply, Id, State#state{producer_id = Id + 1, producers = Producers}};

handle_call({send_command, PulsarCommand}, From, State) ->
  case send_internal(PulsarCommand, State) of
    {reply, Reply, NewState} ->
      %% Ack now. It could be an error or a command that expects no response
      {reply, Reply, NewState};
    {noreply, WaiterId, NewState} when WaiterId /= undefined ->
      %% Track the request for later response
      {WaiterPid, _WaiterTag} = From,
      WaiterMonitorRef = erlang:monitor(process, WaiterPid),
      Waiters = dict:store(WaiterId, From, NewState#state.waiters),
      WaiterMonitor2Id = dict:store(WaiterMonitorRef, WaiterId, NewState#state.waiter_monitor2Id),
      {noreply, NewState#state{waiters = Waiters, waiter_monitor2Id = WaiterMonitor2Id}}
  end;

handle_call(Request, _From, State) ->
  error_logger:warning_msg("Unexpected call: ~p in ~p(~p)", [Request, ?MODULE, self()]),
  {reply, ok, State}.


%% Cast callback
handle_cast({send_command, {payload, Command, _Metadata, _Payload} = PulsarCommand}, State) ->
  case send_internal(PulsarCommand, State) of
    {reply, {error, _} = Error, NewState} ->
      {noreply, handle_error_prod_cons_send(Command, Error, NewState)};
    {_, _, NewState} ->
      {noreply, NewState}
  end;

handle_cast(close, State) ->
  error_logger:info_msg("Closing the clonnection: ~p", [self()]),
  {stop, normal, broadcast_error(?CLOSED_CONNECTION_ERROR, State)};

handle_cast(Request, State) ->
  error_logger:warning_msg("Unexpected cast: ~p in ~p(~p)", [Request, ?MODULE, self()]),
  {noreply, State}.


%% Info callback
handle_info({tcp, Sock, Data}, #state{socket = Sock, data_buffer = DataBuffer} = State) ->
  NewDataBuffer = iolist_to_binary([DataBuffer, Data]),
  NewState = State#state{data_buffer = NewDataBuffer},
  {noreply, handle_broker_data(NewState)};

handle_info({timeout, TimerRef, reconnect},
    #state{logical_address = LogicalAddress, reconnect_timer = TimerRef} = State) ->
  error_logger:info_msg("Reconnecting to ~s", [LogicalAddress]),
  {noreply, reconnect_process(State)};

handle_info({tcp_closed, Sock}, #state{socket = Sock} = State) ->
  {noreply, broadcast_error_then_reconnect({error, ?LOST_CONNECTION_ERROR}, State)};

handle_info({'DOWN', MonitorRef, process, MonitoredPid, _}, State) ->
  NewState =
    %% Remove the dead process from the waiters dicts
  case dict:take(MonitorRef, State#state.waiter_monitor2Id) of
    {WaiterId, NewWaiter_monitor2Id} ->
      NewWaiters = dict:erase(WaiterId, State#state.waiters),
      State#state{waiters = NewWaiters, waiter_monitor2Id = NewWaiter_monitor2Id};
    _ ->
      %% Oops! The dead process is a consumer/producer. Whatever it is, remove it.
      RemovalFun = fun(_, Pid) -> MonitoredPid /= Pid end,
      Producer = dict:erase(RemovalFun, State#state.producers),
      Consumers = dict:filter(RemovalFun, State#state.consumers),
      State#state{consumers = Consumers, producers = Producer}
  end,
  {noreply, NewState};


handle_info(Info, State) ->
  error_logger:warning_msg("Unexpected message: ~p in ~p(~p)", [Info, ?MODULE, self()]),
  {noreply, State}.


terminate(_Reason, _State) ->
  ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

handle_broker_data(State) ->
  case read_complete_frame(State) of
    {false, NewState} ->
      %% Frame is incomplete
      NewState;
    {FrameData, NewState} ->
      NewState2 = handle_command(FrameData, NewState),
      handle_broker_data(NewState2)
  end.


-define(FRAME_LENGTH_INDICATOR_BYTE_SIZE, 4).

%% @Read https://pulsar.apache.org/docs/en/develop-binary-protocol/#framing
read_complete_frame(#state{data_buffer = DataBuffer} = State) ->
  if byte_size(DataBuffer) > ?FRAME_LENGTH_INDICATOR_BYTE_SIZE ->
    {FrameLength, Rest} = commands:read_size(DataBuffer),
    if byte_size(Rest) >= FrameLength ->
      <<Frame:FrameLength/binary, NewDataBuffer/binary>> = Rest,
      CompleteFrame = <<FrameLength:32/unsigned-integer, Frame/binary>>,
      {CompleteFrame, State#state{data_buffer = NewDataBuffer}};
      true ->
        {false, State}
    end;
    true ->
      {false, State}
  end.

handle_command(Data, State) ->
  {Command, HeadersAndPayload} = commands:decode(Data),
  case Command of
    #'CommandPing'{} ->
      send_pong_to_broker(State);
    #'CommandSuccess'{} ->
      handle_success(Command, State);
    #'CommandError'{} ->
      handle_error(Command, State);
    #'CommandSendError'{} ->
      handle_send_error(Command, State);
    #'CommandLookupTopicResponse'{} ->
      handle_lookup_topic(Command, State);
    #'CommandPartitionedTopicMetadataResponse'{} ->
      handle_partition_topic_metadata(Command, State);
    #'CommandCloseProducer'{} ->
      handle_close_producer(Command, State);
    #'CommandCloseConsumer'{} ->
      handle_close_consumer(Command, State);
    #'CommandSendReceipt'{} ->
      handle_send_receipt(Command, State);
    #'CommandProducerSuccess'{} ->
      handle_producer_success(Command, State);
    #'CommandMessage'{} ->
      handle_message(Command, HeadersAndPayload, State);
    _ ->
      error({handler_not_implemented, Command})
  end.


handle_success(#'CommandSuccess'{request_id = RequestId} = Command, State) ->
  send_reply_to_request_waiter(RequestId, Command, State).

handle_error(#'CommandError'{request_id = RequestId, error = Error, message = Msg}, State) ->
  send_reply_to_request_waiter(RequestId, {error, {Error, Msg}}, State).

handle_send_receipt(#'CommandSendReceipt'{
  producer_id = ProdId, sequence_id = SequenceId,
  message_id = MessageId}, State) ->
  fetch_producer_by_id(ProdId, State,
    fun(Pid) when is_pid(Pid) ->
      safe_send(Pid, {ack_received, SequenceId, MessageId});
      (_) ->
        error_logger:warning_msg("The producer with id: ~p "
        "not found when send receipt is received", [ProdId])
    end).

handle_send_error(#'CommandSendError'{
  producer_id = ProdId,
  sequence_id = SequenceId,
  error = Error,
  message = Message}, State) ->
  fetch_producer_by_id(ProdId, State,
    fun(Pid) when is_pid(Pid) ->
      safe_send(Pid, {send_error, SequenceId, {error, {Error, Message}}});
      (_) ->
        error_logger:warning_msg("The producer with id: ~p "
        "not found when when send failed", [ProdId])
    end).

handle_producer_success(#'CommandProducerSuccess'{
  request_id = RequestId} = Response, State) ->
  send_reply_to_request_waiter(RequestId, Response, State).


handle_close_consumer(#'CommandCloseConsumer'{consumer_id = ConsumerId}, State) ->
  fetch_consumer_by_id(ConsumerId, State,
    fun(Pid) when is_pid(Pid) ->
      pulserl_consumer:close(Pid, true);
      (_) ->
        error_logger:warning_msg("The consumer with id: ~p "
        "not found while closing", [ConsumerId])
    end).

handle_close_producer(#'CommandCloseProducer'{producer_id = ProdId}, State) ->
  fetch_producer_by_id(ProdId, State,
    fun(Pid) when is_pid(Pid) ->
      pulserl_producer:close(Pid, true);
      (_) ->
        error_logger:warning_msg("The producer with id: ~p "
        "not found while closing", [ProdId])
    end).


handle_lookup_topic(#'CommandLookupTopicResponse'{
  request_id = RequestId} = Response, State) ->
  send_reply_to_request_waiter(RequestId, Response, State).


handle_partition_topic_metadata(
    #'CommandPartitionedTopicMetadataResponse'{
      request_id = RequestId} = Response, State) ->
  send_reply_to_request_waiter(RequestId, Response, State).

handle_error_prod_cons_send(#'CommandSend'{
  producer_id = ProdId, sequence_id = SequenceId}, {error, Reason}, State) ->
  handle_send_error(#'CommandSendError'{
    producer_id = ProdId, sequence_id = SequenceId, error = Reason
  }, State).


handle_message(#'CommandMessage'{
  consumer_id = ConsumerId,
  message_id = MsgId,
  redelivery_count = RedeliveryCount},
    HeadersAndPayload, State) ->
  fetch_consumer_by_id(ConsumerId, State,
    fun(Pid) when is_pid(Pid) ->
      safe_send(Pid, {new_message, MsgId, RedeliveryCount, HeadersAndPayload});
      (_) ->
        error_logger:warning_msg("The consumer with id: ~p "
        "not found when message arrive", [ConsumerId])
    end).

fetch_consumer_by_id(ConsumerId, State, Callback) ->
  case dict:find(ConsumerId, State#state.consumers) of
    {ok, ProducerPid} ->
      Callback(ProducerPid);
    _ ->
      Callback(undefined)
  end,
  State.

fetch_producer_by_id(ProducerId, State, Callback) ->
  case dict:find(ProducerId, State#state.producers) of
    {ok, ProducerPid} ->
      Callback(ProducerPid);
    _ ->
      Callback(undefined)
  end,
  State.


send_reply_to_all_request_waiters(Reply, State) ->
  lists:foldl(
    fun(RequestId, State0) ->
      send_reply_to_request_waiter(RequestId, Reply, State0)
    end, State, dict:fetch_keys(State#state.waiters)).

send_reply_to_request_waiter(WaiterId, Reply, State) ->
  case dict:take(WaiterId, State#state.waiters) of
    {WaiterTag, NewWaiters} ->
      send_to_waiter(Reply, WaiterTag, State),
      State#state{waiters = NewWaiters};
    _ ->
      error_logger:warning_msg("A reply was sent but it's "
      "not associated with any waiter"),
      State
  end.


send_to_waiter(Reply, {_To, _Tag} = Client, State) ->
  gen_server:reply(Client, Reply), State.


send_pong_to_broker(#state{socket = Sock} = State) ->
  case do_send_internal(#'CommandPong'{}, false, State) of
    {_, {error, _} = Error, NewState} ->
      error_logger:info_msg("Pong reply failed for connection(~p, ~p)."
      "Error: ~p", [Sock, self(), Error]),
      NewState;
    {_, _, NewState} ->
      NewState
  end.


send_internal(_, #state{state = undefined, socket = undefined} = State) ->
  %% We haven't connected yet, It's very likely a reconnection is going on.
  %% We return error immediately to make sure our message queue is not
  %% overflowed and not to keep clients hanging almost indefinitely
  {reply, ?NO_CONNECTION_ERROR, State};
send_internal(RequestCommand, #state{request_id = RequestId} = State) ->
  {RequestCommand2, CmdRequestId, NextRequestId} =
    case RequestCommand of
      {payload, _, _, _} ->
        {RequestCommand, false, RequestId};
      _ ->
        %% Only simple commands may have request id
        case commands:set_request_id(RequestCommand, RequestId) of
          false ->
            {RequestCommand, false, RequestId};
          Command2 ->
            {Command2, RequestId, RequestId + 1}
        end
    end,
  do_send_internal(RequestCommand2, CmdRequestId, State#state{request_id = NextRequestId}).


do_send_internal({payload, Command, Metadata, Payload}, RequestId, State) ->
  RequestData = commands:encode(Command, Metadata, Payload),
  do_send_internal2(RequestData, RequestId, State);
do_send_internal(Command, RequestId, State) ->
  RequestData = commands:encode(Command),
  do_send_internal2(RequestData, RequestId, State).

do_send_internal2(RequestData, RequestId, #state{socket = Sock} = State) ->
  case gen_tcp:send(Sock, RequestData) of
    ok ->
      if RequestId /= false ->
        {noreply, RequestId, State};
        true ->
          %% The waiter is not expecting a response; respond immediately
          {reply, ok, State}
      end;
    {error, _Reason} = Error ->
      NewState = broadcast_error_then_reconnect(Error, State),
      {reply, Error, NewState}
  end.


schedule_reconnect_process(State) ->
  TimerRef = erlang:start_timer(?RECONNECT_INTERVAL, self(), reconnect),
  State#state{reconnect_timer = TimerRef}.


reconnect_process(State) ->
  case (case create_connection(State) of
          {ok, NewState} ->
            case perform_handshake(NewState) of
              {ok, #state{socket = Socket} = Ns} ->
                error_logger:info_msg("Connection to ~s is re-established!",
                  [Ns#state.logical_address]),
                notify_client_of_up(Ns#state{socket = activate_socket(Socket)});
              {error, _} ->
                ok
            end;
          {error, _} ->
            ok
        end)
  of
    #state{} = State2 -> State2;
    _ -> schedule_reconnect_process(State)
  end.

create_connection(#state{logical_address = LogicalAddress} = State) ->
  Addresses = pulserl_utils:logical_to_physical_addresses(LogicalAddress),
  connect_to_brokers(State, Addresses).


connect_to_brokers(State, [{IpAddress, Port} | T]) ->
  Result = connect_to_broker(State, IpAddress, Port, 2),
  case T of
    [] -> Result;
    _ -> connect_to_brokers(State, T)
  end.


connect_to_broker(State, IpAddress, Port, Attempts) ->
  TcpOptions = [
    binary, {active, false}
    | State#state.tcp_options],
  case gen_tcp:connect(IpAddress, Port, TcpOptions) of
    {ok, Socket} ->
      {ok, State#state{
        socket_address = {IpAddress, Port},
        socket = optimize_socket(Socket)}};
    {error, Reason} ->
      error_logger:error_msg("Unable to connect to broker at: ~s. "
      "Reason: ~p", [pulserl_utils:sock_address_to_string(IpAddress, Port), Reason]),
      case Attempts of
        0 -> {error, Reason};
        _ ->
          timer:sleep(200),
          connect_to_broker(State, IpAddress, Port, Attempts - 1)
      end
  end.


perform_handshake(#state{socket = Socket} = State) ->
  ConnectCommand = commands:new_connect(),
  case send_internal(ConnectCommand, State) of
    {_, {error, Reason}, _} ->
      {error, Reason};
    {_, _, NewState} ->
      case gen_tcp:recv(Socket, 0) of
        {ok, Data} ->
          {Command, _} = commands:decode(Data),
          case Command of
            #'CommandConnected'{
              server_version = ServerVsn,
              protocol_version = ProcVsn,
              max_message_size = MaxMessageSize} ->
              {ok, NewState#state{state = ?STATE_READY,
                server_version = ServerVsn,
                protocol_version = ProcVsn,
                max_message_size = MaxMessageSize
              }};
            #'CommandError'{
              error = Error, message = ErrorMessage
            } ->
              {error, Error, ErrorMessage}
          end;
        {error, Reason} ->
          error_logger:error_msg("Error making the connect handsake. Reason: ~p", [Reason]),
          {error, Reason}
      end
  end.


activate_socket(Sock) ->
  ok = inet:setopts(Sock, [{active, true}]),
  Sock.

optimize_socket(Sock) ->
  {ok, [{sndbuf, SndBufferSize}, {recbuf, RecBufferSize}]} =
    inet:getopts(Sock, [sndbuf, recbuf]), %% assert
  ok = inet:setopts(Sock, [{buffer, max(RecBufferSize, SndBufferSize)}]),
  Sock.

broadcast_error_then_reconnect(Error, State) ->
  NewState = broadcast_error(Error, State),
  schedule_reconnect_process(NewState).

broadcast_error(Error, State) ->
  NewState = notify_client_of_down(Error, State),
  %% Then notify all pending waiters of the error
  send_reply_to_all_request_waiters(Error, NewState).


notify_client_of_up(#state{socket_address = SockAddr, socket = Socket} = State) ->
  erlang:send(pulserl_client, {connection_up, SockAddr, Socket, self()}),
  notify_parties(connection_up, dict:to_list(State#state.producers)),
  notify_parties(connection_up, dict:to_list(State#state.consumers)),
  State.

notify_client_of_down(Error, #state{socket_address = SockAddr, socket = Socket} = State) ->
  error_logger:error_msg("Connection(~p in ~p) error: ~p", [Socket, self(), Error]),
  erlang:send(pulserl_client, {connection_down, SockAddr, Socket, self()}),
  notify_parties(connection_down, dict:to_list(State#state.producers)),
  notify_parties(connection_down, dict:to_list(State#state.consumers)),
  State#state{state = undefined, socket = undefined}.

notify_parties(ConnectionState, Parties) ->
  [safe_send(Pid, ConnectionState) || {_, Pid} <- Parties].

safe_send(To, Msg) ->
  try To ! Msg catch _:_ -> Msg end.