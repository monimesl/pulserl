%%%-------------------------------------------------------------------
%%% @author Alpha Umaru Shaw <shawalpha5@gmail.com>
%%% @doc
%%%
%%% @end
%%% Company: Skulup Ltd
%%% Copyright: (C) 2019
%%%-------------------------------------------------------------------
-module(pulserl_conn).
-author("Alpha Umaru Shaw").


-behaviour(gen_server).

-include("pulserl.hrl").
-include("pulsar_api.hrl").


%% API
-export([create/1, start_link/1, stop/1]).

-export([register_handler/3, sync_send/2, sync_send_payload/4, async_send_payload/4]).

%% gen_server callbacks
-export([init/1,
	handle_call/3,
	handle_cast/2,
	handle_info/2,
	terminate/2,
	code_change/3]).

-define(RECONNECT_INTERVAL, 1000).
-define(STATE_READY, ready).


%%%===================================================================
%%% API
%%%===================================================================


register_handler(Pid, Handler, Type) when is_pid(Handler), Type == producer; Type == consumer ->
	gen_server:call(Pid, {register_handler, Handler, Type}).

sync_send(Pid, Command) when is_tuple(Command) ->
	gen_server:call(Pid, {send_command, Command}, timer:seconds(10)).

sync_send_payload(Pid, Command, Metadata, Payload) when is_tuple(Command) ->
	gen_server:call(Pid, {send_command, {payload, Command, Metadata, Payload}}, timer:seconds(10)).


async_send_payload(Pid, Command, Metadata, Payload) when is_tuple(Command) ->
	gen_server:cast(Pid, {send_command, {payload, Command, Metadata, Payload}}).


create(Args) ->
	supervisor:start_child(pulserl_conn_sup, [Args]).


start_link(Args) ->
	gen_server:start_link(?MODULE, Args, []).


stop(Pid) ->
	gen_server:cast(Pid, stop).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

-record(state, {
	state,
	socket,
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
	inflight_requests = dict:new()
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
		{TrackingId, wait, NewState} when TrackingId /= undefined ->
			%% Track the request for later response
			IReqs = dict:store(TrackingId, From, NewState#state.inflight_requests),
			{noreply, NewState#state{inflight_requests = IReqs}};
		{_, Reply, NewState} ->
			%% Ack now. It could be an error
			%% or a command that expects no response
			{reply, Reply, NewState}
	end;

handle_call(_Request, _From, State) ->
	{reply, ok, State}.


%% Cast callback
handle_cast({send_command, PulsarCommand}, State) ->
	case send_internal(PulsarCommand, State) of
		{TrackingId, wait, NewState} ->
			%% Track the request for later response
			IReqs = dict:store(TrackingId, undefined,
				NewState#state.inflight_requests),
			{noreply, NewState#state{inflight_requests = IReqs}};
		{_, {error, _} = Error, NewState} ->
			{_, Command, _, _} = PulsarCommand,
			{noreply, handle_producer_command_error(Command, Error, NewState)}
	end;


handle_cast(stop, State) ->
	error_logger:warning_msg("Stopping: ~p", [self()]),
	{stop, normal, State};

handle_cast(_Request, State) ->
	{noreply, State}.


%% Info callback
handle_info({tcp, Sock, Data}, #state{socket = Sock} = State) ->
	{noreply, handle_server_response(Data, State)};

handle_info({timeout, TimerRef, reconnect},
		#state{logical_address = LogicalAddress, reconnect_timer = TimerRef} = State) ->
	error_logger:info_msg("Reconnecting to ~s", [LogicalAddress]),
	{noreply, reconnect_process(State)};

handle_info({tcp_closed, Sock}, #state{socket = Sock} = State) ->
	NewState = notify_client_of_down(tcp_closed, State),
	{noreply, schedule_reconnect_process(NewState)};

handle_info({'DOWN', _HandlerMonitorRef, process, Pid, _},
		#state{consumers = Consumers, producers = Producers} = State) ->
	Consumers2 = dict:erase(Pid, Consumers),
	Producer2 = dict:erase(Pid, Producers),
	{noreply, State#state{consumers = Consumers2, producers = Producer2}};


handle_info(Info, State) ->
	error_logger:warning_msg("Unexpected message: ~p", [Info]),
	{noreply, State}.


terminate(_Reason, _State) ->
	ok.

code_change(_OldVsn, State, _Extra) ->
	{ok, State}.


%%%===================================================================
%%% Internal functions
%%%===================================================================

handle_producer_command(ProducerId, ReqCommand, RespCommand, State) ->
	send_command_to_producer(ProducerId, ReqCommand, RespCommand, State).


handle_producer_command_error(#'CommandSend'{
	producer_id = ProdId} = Command, Error, State) ->
	send_command_to_producer(ProdId, Command, Error, State).


send_command_to_producer(ProdId, Command, Response, State) ->
	case dict:find(ProdId, State#state.producers) of
		{ok, ProducerPid} ->
			ProducerPid ! {on_command, Command, Response};
		_ ->
			error_logger:warning_msg("Response arrived but the "
			"producer is not available")
	end,
	State.


handle_server_response(Data, #state{inflight_requests = InReqs} = State) ->
	lists:foldl(
		fun(ResponseCmd, State0) ->
			case ResponseCmd of
				#'CommandPing'{} ->
					send_pong(State0);
				#'CommandCloseProducer'{producer_id = ProducerId} ->
					handle_producer_command(ProducerId, undefined, ResponseCmd, State0);
				_ ->
					TrackingId = tracking_id_from_response_command(ResponseCmd),
					case dict:take(TrackingId, InReqs) of
						{TrackingData, InReqs2} ->
							State1 = State0#state{inflight_requests = InReqs2},
							case TrackingData of
								undefined ->
									case TrackingId of
										{producer, ProducerId, _} ->
											handle_producer_command(ProducerId, undefined, ResponseCmd, State1)
									end;
								From ->
									reply_to_client(ResponseCmd, From, State1)
							end;
						error ->
							error_logger:warning_msg("The response tracking Id: "
							"~p wasn't mapped to any receiver. Current trackables: ~p", [
								TrackingId, dict:size(State0#state.inflight_requests)]),
							State0
					end
			end
		end, State, commands:decode_unwrap(Data)).



reply_to_client(Response, {_To, _Tag} = Client, State) ->
	gen_server:reply(Client, Response), State.


send_pong(#state{socket = Sock} = State) ->
	case do_send_internal(undefined, #'CommandPong'{}, State) of
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
	{undefined, {error, no_connection}, State};
send_internal(RequestCommand, #state{request_id = RequestId} = State) ->
	{RequestTrackingId, RequestCommand2, NextReqId2} =
		case RequestCommand of
			{payload, Command, Metadata, Payload} ->
				case commands:set_request_id(Command, RequestId) of
					false ->
						{TrackingId, NextReqId} = tracking_id_from_request_command(Command, RequestId),
						{TrackingId, RequestCommand, NextReqId};
					Command2 ->
						{RequestId, {payload, Command2, Metadata, Payload}, RequestId + 1}
				end;
			_ -> % Simple command
				case commands:set_request_id(RequestCommand, RequestId) of
					false ->
						{TrackingId, NextReqId} = tracking_id_from_request_command(RequestCommand, RequestId),
						{TrackingId, RequestCommand, NextReqId};
					Command2 ->
						{RequestId, Command2, RequestId + 1}
				end
		end,
	do_send_internal(RequestTrackingId, RequestCommand2, State#state{request_id = NextReqId2}).



do_send_internal(TrackingId, {payload, Command, Metadata, Payload}, State) ->
	BaseCommand = commands:wrap(Command),
	Data = commands:payload_encode(BaseCommand, Metadata, Payload),
	do_send_internal2(TrackingId, Data, State);
do_send_internal(TrackingId, Command, State) ->
	BaseCommand = commands:wrap(Command),
	Data = commands:simple_encode(BaseCommand),
	do_send_internal2(TrackingId, Data, State).

do_send_internal2(TrackingId, Data, #state{socket = Sock} = State) ->
	case gen_tcp:send(Sock, Data) of
		ok ->
			{TrackingId, wait, State};
		{error, Reason} = Error ->
			NewState = notify_client_of_down(Reason, State),
			{TrackingId, Error, schedule_reconnect_process(NewState)}
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
	ConnectCommand = commands:cmd_connect(),
	case send_internal(ConnectCommand, State) of
		{_, wait, NewState} ->
			case gen_tcp:recv(Socket, 0) of
				{ok, Data} ->
					case commands:decode_unwrap(Data) of
						[#'CommandConnected'{
							server_version = ServerVsn,
							protocol_version = ProcVsn,
							max_message_size = MaxMessageSize}] ->
							{ok, NewState#state{state = ?STATE_READY,
								server_version = ServerVsn,
								protocol_version = ProcVsn,
								max_message_size = MaxMessageSize
							}};
						[#'CommandError'{
							error = Error, message = ErrorMessage
						}] ->
							{error, Error, ErrorMessage}
					end;
				{error, Reason} ->
					error_logger:error_msg("Error making the connect handsake. Reason: ~p", [Reason]),
					{error, Reason}
			end;
		{_, {error, Reason}, _} ->
			{error, Reason}
	end.


activate_socket(Sock) ->
	ok = inet:setopts(Sock, [{active, true}]),
	Sock.

optimize_socket(Sock) ->
	{ok, [{sndbuf, SndBufferSize}, {recbuf, RecBufferSize}]} =
		inet:getopts(Sock, [sndbuf, recbuf]), %% assert
	ok = inet:setopts(Sock, [{buffer, max(RecBufferSize, SndBufferSize)}]),
	Sock.

notify_client_of_up(#state{socket_address = SockAddr, socket = Socket} = State) ->
	erlang:send(pulserl_client, {connection_up, SockAddr, Socket, self()}),
	State.

notify_client_of_down(Reason, #state{socket_address = SockAddr, socket = Socket} = State) ->
	error_logger:error_msg("Connection(~p in ~p) closed. Reason: ~p", [Socket, self(), Reason]),
	erlang:send(pulserl_client, {connection_down, SockAddr, Socket, self()}),
	State#state{state = undefined, socket = undefined}.


tracking_id_from_request_command(Command, ReqId) when is_integer(ReqId) ->
	case Command of
		#'CommandSend'{producer_id = PrId, sequence_id = SeqId} ->
			{{producer, PrId, SeqId}, ReqId};
		_ ->
			{ReqId, ReqId + 1}
	end.


tracking_id_from_response_command(ResponseCommand) ->
	case commands:get_request_id(ResponseCommand) of
		false ->
			case ResponseCommand of
				#'CommandSendReceipt'{producer_id = PrId, sequence_id = SeqId} ->
					{producer, PrId, SeqId}
			end;
		Id -> Id
	end.