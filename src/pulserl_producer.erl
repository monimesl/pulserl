%%%-------------------------------------------------------------------
%%% @author Alpha Umaru Shaw <shawalpha5@gmail.com>
%%% @doc
%%%
%%% @end
%%% Copyright: (C) 2020, Skulup Ltd
%%%-------------------------------------------------------------------
-module(pulserl_producer).

-include("pulserl.hrl").
-include("pulsar_api.hrl").

-behaviour(gen_server).

%% gen_Server API
-export([start_link/2]).
%% gen_server callbacks
-export([init/1,
  handle_call/3,
  handle_cast/2,
  handle_info/2,
  terminate/2,
  code_change/3]).

%% Producer API
-export([create/2, close/1, close/2]).
-export([send/3, sync_send/3, new_message/1, new_message/2, new_message/3, new_message/4, new_message/5]).

-define(STATE_READY, ready).
-define(CALL_TIMEOUT, 300000).
-define(INFO_SEND_BATCH, send_batch).
-define(INFO_ACK_TIMEOUT, ack_timeout).
-define(INFO_REINITIALIZE, try_reinitialize).
-define(ERROR_SEND_TIMEOUT, {error, send_timeout}).
-define(ERROR_PRODUCER_DOWN, {error, producer_down}).
-define(ERROR_PRODUCER_CLOSED, {error, producer_closed}).
-define(ERROR_PRODUCER_NOT_READY, {error, producer_not_ready}).
-define(ERROR_PRODUCER_QUEUE_IS_FULL, {error, producer_queue_full}).

-define(SINGLE_ROUTING, single_routing).
-define(ROUND_ROBIN_ROUTING, round_robin_routing).

%%--------------------------------------------------------------------
%% @doc Creates a new message to produce
%%--------------------------------------------------------------------
new_message(Value) ->
  new_message(?UNDEF, Value).

%%--------------------------------------------------------------------
%% @doc Creates a new message to produce
%%--------------------------------------------------------------------
new_message(Key, Value) ->
  new_message(Key, Value, ?UNDEF).

%%--------------------------------------------------------------------
%% @doc Creates a new message to produce
%%--------------------------------------------------------------------
new_message(Key, Value, Properties) ->
  new_message(Key, Value, Properties, erlwater_time:milliseconds()).

%%--------------------------------------------------------------------
%% @doc Creates a new message to produce
%%--------------------------------------------------------------------
new_message(Key, Value, Properties, EventTime) ->
  new_message(Key, Value, Properties, EventTime, ?UNDEF).

%%--------------------------------------------------------------------
%% @doc Creates a new message to produce
%%--------------------------------------------------------------------
-spec(new_message(
    Key :: key() | undefined,
    Value :: value(),
    Properties :: properties(),
    EventTime :: integer() | undefined,
    DeliverAtTime :: integer() | undefined) ->
  ok | reference()).
new_message(Key, Value, Properties, EventTime, DeliverAtTime) ->
  #prodMessage{
    key = case Key of ?UNDEF -> ?UNDEF; _ -> erlwater:to_binary(Key) end,
    value = erlwater:to_binary(Value),
    event_time = erlwater:to_integer(EventTime),
    deliverAtTime = DeliverAtTime,
    properties = Properties
  }.


%%--------------------------------------------------------------------
%% @doc Send a message asynchronously
%%--------------------------------------------------------------------
send(Pid, #prodMessage{} = Message, Callback) when is_pid(Pid) ->
  {CallerFun, CallReturn} =
    if is_function(Callback) ->
      {fun() -> gen_server:call(Pid, {send_message, Callback, Message}, ?CALL_TIMEOUT) end, ok};
      true ->
        ClientRef = erlang:make_ref(),
        {fun() ->
          gen_server:call(Pid, {send_message, {self(), ClientRef}, Message}, ?CALL_TIMEOUT) end, ClientRef}
    end,
  try
    case CallerFun() of
      ok ->
        CallReturn;
      {error, _} = Error ->
        Error;
      {redirect, ChildProducerPid} ->
        send(ChildProducerPid, Message, Callback)
    end
  catch
    _:{timeout, _} ->
      {error, timeout};
    _:Reason ->
      {error, Reason}
  end.

%%--------------------------------------------------------------------
%% @doc Send a message synchronously
%%--------------------------------------------------------------------
-spec(sync_send(Pid :: pid(), Message :: #prodMessage{}, Timeout :: pos_integer()) ->
  #messageId{} | {error, Reason :: term()}).
sync_send(Pid, #prodMessage{} = Message, Timeout) when is_pid(Pid)
  andalso (is_integer(Timeout) orelse Timeout == ?UNDEF) ->
  ClientRef = erlang:make_ref(),
  MonitorRef = erlang:monitor(process, Pid),
  Timeout2 = case Timeout of ?UNDEF -> ?CALL_TIMEOUT; T -> T end,
  case gen_server:call(Pid, {send_message, {self(), ClientRef}, Message}, Timeout2 + 2000) of
    {redirect, ChildProducerPid} ->
      erlang:demonitor(MonitorRef, [flush]),
      sync_send(ChildProducerPid, Message, Timeout2);
    ok ->
      receive
        {ClientRef, Reply} ->
          erlang:demonitor(MonitorRef, [flush]),
          Reply;
        {'DOWN', MonitorRef, process, _Pid, _Reason} ->
          ?ERROR_PRODUCER_DOWN
      after Timeout2 ->
        erlang:demonitor(MonitorRef, [flush]),
        {error, {timeout, ClientRef}}
      end;
    Other ->
      Other
  end.

create(#topic{} = Topic, Options) ->
  case whereis(pulserl_client) of
    ?UNDEF ->
      ?ERROR_CLIENT_NOT_STARTED;
    _ ->
      Options2 = validate_options(Options),
      supervisor:start_child(pulserl_producer_sup, [Topic, Options2])
  end.


%%--------------------------------------------------------------------
%% @doc

%% @end
%%--------------------------------------------------------------------
close(Pid) ->
  close(Pid, false).

close(Pid, AttemptRestart) ->
  gen_server:cast(Pid, {close, AttemptRestart}).


%%%===================================================================
%%% gen_server API
%%%===================================================================

start_link(#topic{} = Topic, Options) ->
  gen_server:start_link(?MODULE, [Topic, Options], []).


%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

-record(metrics, {
  sent_messages = 0,
  ack_messages = 0
}).

-record(state, {
  state,
  connection :: pid(),
  producer_id :: integer(),
  %% start of producer options
  producer_name :: string(),
  producer_properties = [] :: list() | #{},
  send_timeout :: integer(), %% [0, ...], 0 means no timeout and retry forever
  producer_initial_sequence_id :: integer(),
  %% end of producer options
  topic :: #topic{},
  sequence_id :: integer(),
  partition_count = 0 :: non_neg_integer(),
  partition_to_child = dict:new(),
  child_to_partition = dict:new(),
  partition_routing_mode :: atom() | {atom(), atom()},
  partition_to_route_to :: non_neg_integer(),
  batching_requests = queue:new(),
  pending_requests = queue:new(),
  seqId2waiters = #{} :: #{},
  batch_send_timer,
  re_init_timer,
  %%Config
  options :: list(),
  batch_enable :: boolean(),
  max_batched_messages :: integer(),
  batch_max_delay_ms :: integer(),
  max_pending_requests :: integer(),
  max_pending_requests_across_partitions :: integer(),
  block_on_full_queue :: boolean(),
  send_timeout_timer :: reference(),
  metrics = #metrics{} :: #metrics{}
}).


init([#topic{} = Topic, Opts]) ->
  process_flag(trap_exit, true),
  ProducerOpts = proplists:get_value(producer, Opts, []),
  State = #state{
    topic = Topic,
    options = Opts,
    producer_name = proplists:get_value(name, ProducerOpts),
    producer_properties = proplists:get_value(properties, ProducerOpts, []),
    producer_initial_sequence_id = proplists:get_value(initial_sequence_id, ProducerOpts, -1) + 1,
    partition_routing_mode = proplists:get_value(routing_mode, Opts, ?ROUND_ROBIN_ROUTING),
    batch_enable = proplists:get_value(batch_enable, Opts, true),
    send_timeout = proplists:get_value(send_timeout, ProducerOpts, 30000),
    batch_max_delay_ms = proplists:get_value(batch_max_delay_ms, Opts, 10),
    max_batched_messages = proplists:get_value(batch_max_messages, Opts, 1000),
    max_pending_requests = proplists:get_value(max_pending_requests, Opts, 50000),
    max_pending_requests_across_partitions = proplists:get_value(
      max_pending_requests_across_partitions, Opts, 100000)
  },
  case initialize(State#state{
    sequence_id = State#state.producer_initial_sequence_id}) of
    {error, Reason} ->
      {stop, Reason};
    NewState ->
      {ok, notify_instance_provider_of_state(NewState, producer_up)}
  end.

handle_call(_, _From, #state{state = ?UNDEF} = State) ->
  %% I'm not ready yet
  {reply, ?ERROR_PRODUCER_NOT_READY, State};

%% The parent
handle_call({send_message, _ClientFrom, #prodMessage{key = Key}}, _From,
    #state{partition_count = PartitionCount} = State) when PartitionCount > 0 ->
  %% This producer is the partition parent.
  %% We choose the child producer and redirect
  %% the client to it.
  {Replay, State3} =
    case choose_partition_producer(Key, State) of
      {{ok, Pid}, State2} ->
        {{redirect, Pid}, State2};
      {_, State2} ->
        {?ERROR_PRODUCER_NOT_READY, State2}
    end,
  {reply, Replay, State3};

%% The child/no-child-producer
handle_call({send_message, ClientFrom, Message}, _From,
    #state{pending_requests = PendingReqs, max_pending_requests = MaxPendingReqs} = State) ->
  case queue:len(PendingReqs) < MaxPendingReqs of
    true ->
      {Reply, State2} = send_message(Message, ClientFrom, State),
      {reply, Reply, increment_sent_metric(State2, 1)};
    _ ->
      {reply, ?ERROR_PRODUCER_QUEUE_IS_FULL, State}
  end;
handle_call(Request, _From, State) ->
  error_logger:warning_msg("Unexpected call: ~p in ~p(~p)", [Request, ?MODULE, self()]),
  {reply, ok, State}.

%% The producer was ask to close
handle_cast({close, AttemptRestart}, State) ->
  case AttemptRestart of
    true ->
      error_logger:info_msg("Temporariliy closing producer: ~p",
        [topic_utils:to_string(State#state.topic)]),
      State2 = close_children(State, AttemptRestart),
      State3 = send_reply_to_all_waiters(?ERROR_PRODUCER_CLOSED, State2),
      {noreply, reinitialize(State3#state{state = ?UNDEF})};
    _ ->
      error_logger:info_msg("Producer(~p) at: ~p is permanelty closing",
        [self(), topic_utils:to_string(State#state.topic)]),
      State2 = send_reply_to_all_waiters(?ERROR_PRODUCER_CLOSED, State),
      {close, normal, close_children(State2, AttemptRestart)}
  end;

handle_cast(Request, State) ->
  error_logger:warning_msg("Unexpected Cast: ~p", [Request]),
  {noreply, State}.

handle_info({ack_received, SequenceId, MessageId},
    #state{topic = Topic, metrics = Metrics,
      seqId2waiters = SeqId2Waiters,
      pending_requests = PendingReqs} = State) ->
  Reply = pulserl_utils:new_message_id(Topic, MessageId),
  {_, PendingReqs2} = queue:out(PendingReqs),
  State2 = State#state{pending_requests = PendingReqs2},
  State3 = case maps:get(SequenceId, SeqId2Waiters, ?UNDEF) of
             Waiters when is_list(Waiters) ->
               NewAckCount = Metrics#metrics.ack_messages + length(Waiters),
               State2#state{metrics = Metrics#metrics{ack_messages = NewAckCount}};
             _ ->
               State2
           end,
  {noreply, send_reply_to_waiter(SequenceId, Reply, State3)};
handle_info({send_error, SequenceId, {error, _} = Error}, State) ->
  {noreply, send_reply_to_waiter(SequenceId, Error, State)};

handle_info(?INFO_SEND_BATCH, State) ->
  State2 = send_batch_if_possible(true, State),
  {noreply, start_send_batch_timer(State2)};

handle_info(?INFO_ACK_TIMEOUT, State) ->
  {NewDelay, State2} = handle_ack_timer_timeout(State),
  {noreply, start_ack_timeout_timer(State2, NewDelay)};

%% Our connection is down. We stop the scheduled
%% re-initialization attempts and try again after
%% a `connection_up` message
handle_info(connection_down, State) ->
  State2 = send_reply_to_all_waiters(?ERROR_PRODUCER_CLOSED, State),
  State3 = cancel_all_timers(State2),
  {noreply, State3#state{state = ?UNDEF}};

%% Try re-initialization again
handle_info(connection_up, State) ->
  {noreply, reinitialize(State)};

%% Last reinitialization failed. Still trying..
handle_info(?INFO_REINITIALIZE, State) ->
  {noreply, reinitialize(State)};

handle_info({'DOWN', _ConnMonitorRef, process, _Pid, _},
    #state{} = State) ->
  %% This shouldn't happen as we design the connection to avoid crashing
  {stop, normal, send_reply_to_all_waiters(?ERROR_PRODUCER_CLOSED, State)};

handle_info({'EXIT', Pid, Reason}, State) ->
  case Reason of
    normal -> {noreply, State};
    _ ->
      case maybe_child_producer_exited(Pid, Reason, State) of
        {error, Reason} ->
          {stop, Reason, State};
        #state{} = NewState ->
          {noreply, NewState}
      end
  end;

handle_info(Info, State) ->
  error_logger:warning_msg("Unexpected Info: ~p", [Info]),
  {noreply, State}.


terminate(_Reason, State) ->
  notify_instance_provider_of_state(State, producer_down).


code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

handle_ack_timer_timeout(#state{
  send_timeout = SendTimeout,
  pending_requests = PendingRequests} = State) ->
  case queue:out(PendingRequests) of
    {empty, _} ->
      {SendTimeout, State};
    {{value, {CreateTime, {SeqId, _}}}, PendingRequests2} ->
      Diff = erlwater_time:milliseconds() - CreateTime,
      if Diff >= SendTimeout ->
        State2 = State#state{pending_requests = PendingRequests2},
        {SendTimeout, send_reply_to_waiter(SeqId, ?ERROR_SEND_TIMEOUT, State2)};
        true ->
          {SendTimeout - Diff, State}
      end
  end.

send_reply_to_waiter(SequenceId, Reply,
    #state{seqId2waiters = SeqId2Waiters} = State) ->
  case maps:get(SequenceId, SeqId2Waiters, ?UNDEF) of
    ?UNDEF ->
      if State#state.send_timeout > 0 ->
        error_logger:warning_msg("Received a command receipt for in "
        "producer: ~p but couldn't see any waiter for it; sequence-id = ~p.",
          [self(), SequenceId]);
        true ->
          error_logger:info_msg("Received a command receipt for timed-out message; "
          "sequence-id = ~p", [SequenceId])
      end,
      State;
    Waiters ->
      State2 = send_replies_to_waiters(Reply, Waiters, State),
      State2#state{seqId2waiters = maps:remove(SequenceId, SeqId2Waiters)}
  end.


choose_partition_producer(Key,
    #state{partition_count = PartitionCount} = State) ->
  {Partition, State2} =
    case State#state.partition_routing_mode of
      {M, F} ->
        Int = erlang:apply(M, F, [Key, PartitionCount]),
        if (is_integer(Int) andalso Int >= 0 andalso Int < PartitionCount) ->
          {Int, State};
          true ->
            error_logger:error_msg("The custom router mode: ~p produces invalid valid: ~p. "
            "It mut return an integer in the range: [0, ~p)", [{M, F}, PartitionCount]),
            {rand:uniform(PartitionCount) - 1, State}
        end;
      Mode ->
        if is_binary(Key) ->
          {pulserl_utils:hash_key(Key, PartitionCount), State};
          true ->
            Part = State#state.partition_to_route_to,
            case Mode of
              ?SINGLE_ROUTING ->
                case Part of
                  ?UNDEF ->
                    Part2 = rand:uniform(PartitionCount) - 1,
                    {Part2, State#state{partition_to_route_to = Part2}};
                  _ ->
                    {Part, State}
                end;
              _ ->
                %% Round robin
                case Part of
                  ?UNDEF ->
                    {0, State#state{partition_to_route_to = 0}};
                  _ ->
                    Next = (Part + 1) rem PartitionCount,
                    {Part, State#state{partition_to_route_to = Next}}
                end
            end
        end
    end,
  {dict:find(Partition, State2#state.partition_to_child), State2}.


send_message(Message, From, State) ->
  Request = {From, Message},
  if State#state.batch_enable andalso
    Message#prodMessage.deliverAtTime == ?UNDEF ->
    case add_request_to_batch(Request, State) of
      {notfull, NewRequestQueue} ->
        %% Was added but still the `pending_requests` has some space.
        %% Tell the client to chill whilst; we'll send the response
        %% when either the `batch_max_delay_ms` timeouts or
        %% `pending_requests` is reached.
        {ok, send_batch_if_possible(false, State#state{batching_requests = NewRequestQueue})};
      {full, NewRequestQueue} ->
        %% The new request just fills the `pending_requests` queue.
        %% Send `ok` to the client and trigger a batch send if none is in progress
        {ok, send_batch_if_possible(false, State#state{batching_requests = NewRequestQueue})};
      {fulled, NewRequestQueue} ->
        %% The `pending_requests` is already fulled,
        {ok, send_batch_if_possible(false, State#state{batching_requests = NewRequestQueue})}
    end;
    true ->
      %% No batching. Send directly
      {ok, send_message(Request, State)}
  end.


send_batch_if_possible(ForceBatch, #state{
  max_batched_messages = MaxBatchedMessages,
  batching_requests = RequestQueue} = State) ->
  NumberOfMessagesToBatch =
    if ForceBatch ->
      erlang:min(MaxBatchedMessages, queue:len(RequestQueue));
      true ->
        MaxBatchedMessages
    end,
  case get_messages_to_batch(NumberOfMessagesToBatch, State) of
    {[], _} -> State; %% Not enough messages
    {NextBatch, NewRequestQueue} ->
      State2 = State#state{
        batching_requests = NewRequestQueue},
      send_batch_messages(NextBatch, State2)
  end.


get_messages_to_batch(0, #state{batching_requests = BatchingRequests}) ->
  {[], BatchingRequests};
get_messages_to_batch(Size, #state{batching_requests = BatchingRequests} = State) ->
  case queue:len(BatchingRequests) >= Size of
    true ->
      {NextBatchQueue, RemainingQueue} = queue:split(Size, BatchingRequests),
      update_pending_count_across_partitions(
        State, - queue:len(NextBatchQueue)),
      {queue:to_list(NextBatchQueue), RemainingQueue};
    _ ->
      {[], BatchingRequests}
  end.

resend_messages(#state{
  topic = Topic,
  pending_requests = PendingReqs,
  seqId2waiters = SeqId2Waiters} = State) ->
  Len = queue:len(PendingReqs),
  if Len > 0 ->
    {SeqId2Waiters1, RequestsToResend1} = lists:foldl(
      fun({_, {SeqId0, Requests0}}, {SeqId2Waiters0, RequestsToResend0}) ->
        {maps:remove(SeqId0, SeqId2Waiters0), Requests0 ++ RequestsToResend0}
      end, {SeqId2Waiters, []}, queue:to_list(PendingReqs)),
    error_logger:info_msg("Re-sending ~p messages from producer=~p, topic=~p",
      [length(RequestsToResend1), self(), topic_utils:to_string(Topic)]),
    send_batch_messages(RequestsToResend1, State#state{
      seqId2waiters = SeqId2Waiters1,
      pending_requests = queue:new()});
    true ->
      State
  end.

send_message({_, #prodMessage{value = Payload} = Msg} = Request,
    #state{sequence_id = SeqId} = State) ->
  {SendCmd, Metadata} = commands:new_send(State#state.producer_id,
    State#state.producer_name, SeqId, Msg#prodMessage.key, Msg#prodMessage.event_time,
    %% `num_messages_in_batch` must be undefined for non-batch messages
    ?UNDEF, Msg#prodMessage.deliverAtTime, Payload),
  do_send_message(SeqId, [Request], SendCmd, Metadata, Payload, State).

do_send_message(SeqId, Requests, SendCmd, Metadata, Payload,
    #state{connection = Cnx,
      pending_requests = PendingRequests,
      seqId2waiters = SeqId2Requests} = State) ->
  pulserl_conn:send_payload_command(Cnx, SendCmd, Metadata, Payload),
  SeqId2Requests2 = maps:put(SeqId, [From || {From, _} <- Requests], SeqId2Requests),
  PendingRequests2 = queue:in({erlwater_time:milliseconds(), {SeqId, Requests}}, PendingRequests),
  State#state{sequence_id = SeqId + 1,
    seqId2waiters = SeqId2Requests2,
    pending_requests = PendingRequests2}.

send_batch_messages([Request], State) ->
  %% Only one request. Don't batch it
  send_message(Request, State);

send_batch_messages(RequestsToBatch, #state{
  sequence_id = SeqId,
  producer_id = ProducerId,
  producer_name = ProducerName} = State) ->
  {FinalSeqId, BatchPayload2} = lists:foldl(
    fun({_, Msg}, {SeqId0, BatchPayload0}) ->
      Payload = Msg#prodMessage.value,
      SingleMsgMeta =
        #'SingleMessageMetadata'{
          sequence_id = SeqId0,
          payload_size = byte_size(Payload),
          partition_key = Msg#prodMessage.key,
          event_time = Msg#prodMessage.event_time,
          properties = commands:to_con_prod_metadata(Msg#prodMessage.properties)
        },
      SerializedSingleMsgMeta = pulsar_api:encode_msg(SingleMsgMeta),
      SerializedSingleMsgMetaSize = byte_size(SerializedSingleMsgMeta),
      BatchPayload1 = erlang:iolist_to_binary([BatchPayload0,
        pulserl_io:write_int32(SerializedSingleMsgMetaSize),
        SerializedSingleMsgMeta, Payload
      ]),
      {SeqId0 + 1, BatchPayload1}
    end, {SeqId, <<>>}, RequestsToBatch),
  [{_, FirstMsg} | _] = RequestsToBatch,
  SizeOfBatch = length(RequestsToBatch),
  {SendCmd, Metadata} = commands:new_send(ProducerId, ProducerName,
    SeqId, ?UNDEF, FirstMsg#prodMessage.event_time,
    SizeOfBatch, ?UNDEF, BatchPayload2
  ),
  do_send_message(SeqId, RequestsToBatch,
    SendCmd, Metadata, BatchPayload2,
    State#state{sequence_id = FinalSeqId}).

increment_sent_metric(#state{metrics = Metrics} = State, Increment) ->
  State#state{metrics = Metrics#metrics{
    sent_messages = Metrics#metrics.sent_messages + Increment}
  }.

send_reply_to_all_waiters(Reply, State) ->
  SentRequestWaiters = [Waiters || {_, Waiters} <- maps:to_list(State#state.seqId2waiters)],
  PendingRequestWaiters = [Waiter || {Waiter, _} <- queue:to_list(State#state.batching_requests)],
  send_replies_to_waiters(Reply, lists:flatten(SentRequestWaiters) ++ PendingRequestWaiters, State).

send_replies_to_waiters(Reply, Waiters, State) ->
  lists:foreach(
    fun(Client) ->
      try
        case Client of
          {Pid, Tag} ->
            Pid ! {Tag, Reply};
          Fun when is_function(Fun) ->
            apply(Fun, [Reply])
        end
      catch
        _:Reason ->
          error_logger:error_msg("Error(~p) on replying to "
          "the client.", [{Reason}])
      end
    end, Waiters),
  State.


add_request_to_batch(Request, #state{
  batching_requests = BatchingRequests,
  max_pending_requests = MaxPendingMessages,
  max_pending_requests_across_partitions = MaxPendingPartitionedMessages
} = State) ->
  RequestQueueLen = queue:len(BatchingRequests),
  case (RequestQueueLen < MaxPendingMessages)
    andalso (MaxPendingPartitionedMessages >
      %% Increment by zero to read current count
    update_pending_count_across_partitions(State, 0)) of
    true ->
      %% Add to the `pending_requests` queue
      {CrossPartitionsPendingLen2, BatchRequestsLen2, NewBatchRequests} = add_request_to_batch2(Request, State),
      %% Check again if it's still not full
      if (BatchRequestsLen2 < MaxPendingMessages) andalso (MaxPendingPartitionedMessages > CrossPartitionsPendingLen2) ->
        {notfull, NewBatchRequests};
        true ->
          {full, NewBatchRequests}
      end;
    _ ->
      {_, _, NewBatchRequests} = add_request_to_batch2(Request, State),
      {fulled, NewBatchRequests}
  end.


add_request_to_batch2(Request, #state{batching_requests = RequestQueue} = State) ->
  NewRequestQueue = queue:in(Request, RequestQueue),
  {update_pending_count_across_partitions(State, 1),
    queue:len(NewRequestQueue), NewRequestQueue}.


update_pending_count_across_partitions(
    #state{topic = Topic}, Increment) ->
  if Topic#topic.parent /= ?UNDEF ->
    %% this is a producer to one of the partition
    TopicName = topic_utils:to_string(Topic#topic.parent),
    Update =
      if Increment < 0 ->
        {2, Increment, 0, 0};
        true -> {2, Increment}
      end,
    ets:update_counter(partition_pending_messages, TopicName, Update, {TopicName, 0});
    true ->
      -1 %% Make sure it's below zero for non partitioned topics
  end.


maybe_child_producer_exited(ExitedPid, Reason, State) ->
  case dict:find(ExitedPid, State#state.child_to_partition) of
    {ok, Partition} ->
      error_logger:warning_msg("Producer(~p) to '~s' exited abnormally due to reason."
      " '~p'. Restarting...", [ExitedPid, topic_utils:partition_str(
        State#state.topic, Partition), Reason]),
      State2 = State#state{
        partition_to_child = dict:erase(Partition, State#state.partition_to_child),
        child_to_partition = dict:erase(ExitedPid, State#state.child_to_partition)
      },
      case create_inner_producer(Partition, State2) of
        {_NewPid, #state{} = NewState} ->
          error_logger:info_msg("Producer to '~s' restarted.",
            [topic_utils:partition_str(State#state.topic, Partition)]),
          NewState;
        {error, NewReason} = Error ->
          error_logger:error_msg("Producer to '~s' restart failed. Reason: ~p",
            [topic_utils:partition_str(State#state.topic, Partition), NewReason]),
          Error
      end;
    error ->
      %% We're told to exit by our parent
      {error, Reason}
  end.

reinitialize(#state{state = ?UNDEF} = State) ->
  Topic = topic_utils:to_string(State#state.topic),
  case initialize(State) of
    {error, Reason} ->
      error_logger:error_msg("Producer: ~p re-initialization failed: ~p", [Topic, Reason]),
      State#state{re_init_timer = erlang:send_after(500, self(), ?INFO_REINITIALIZE)};
    NewState ->
      error_logger:info_msg("Producer: ~p re-initialization completes", [Topic]),
      resend_messages(NewState)
  end;
reinitialize(State) ->
  State.

initialize(#state{state = ?STATE_READY} = State) ->
  State;
initialize(#state{topic = Topic} = State) ->
  Result =
    case topic_utils:is_partitioned(Topic) of
      true ->
        initialize_self(State);
      _ ->
        case pulserl_client:get_partitioned_topic_meta(Topic) of
          #partitionMeta{partitions = PartitionCount} ->
            State2 = State#state{partition_count = PartitionCount},
            if PartitionCount == 0 ->
              initialize_self(State2);
              true ->
                initialize_children(State2)
            end;
          {error, _} = Error -> Error
        end
    end,
  case Result of
    #state{} = State3 ->
      State3#state{state = ?STATE_READY};
    _ -> Result
  end.

initialize_self(#state{topic = Topic} = State) ->
  case pulserl_client:get_broker_address(Topic) of
    LogicalAddress when is_list(LogicalAddress) ->
      case pulserl_client:get_broker_connection(LogicalAddress) of
        {ok, Pid} ->
          Id = pulserl_conn:register_handler(Pid, self(), producer),
          case establish_producer(State#state{
            connection = Pid, producer_id = Id
          }) of
            #state{send_timeout = SendTimeout} = State2 ->
              State3 = start_ack_timeout_timer(State2, SendTimeout),
              start_send_batch_timer(State3);
            {error, _} = Error ->
              Error
          end;
        {error, _} = Error ->
          Error
      end;
    {error, _} = Error ->
      Error
  end.

initialize_children(#state{partition_count = NumberOfPartitions} = State) ->
  {State2, Err} = lists:foldl(
    fun(PartitionIndex, {State0, Error}) ->
      if Error == ?UNDEF ->
        case create_inner_producer(PartitionIndex, State0) of
          {_, #state{} = S2} ->
            {S2, Error};
          Error0 ->
            {State0, Error0}
        end;
        true ->
          {State0, Error}
      end
    end, {State, ?UNDEF}, lists:seq(0, NumberOfPartitions - 1)),
  case Err of
    {error, _} = Err ->
      [pulserl_producer:close(Pid, false) || {_, Pid} <- dict:fetch_keys(State2#state.child_to_partition)],
      Err;
    ?UNDEF ->
      State2
  end.


establish_producer(#state{topic = Topic} = State) ->
  Command = #'CommandProducer'{
    topic = topic_utils:to_string(Topic),
    producer_id = State#state.producer_id,
    producer_name = State#state.producer_name,
    metadata = commands:to_con_prod_metadata(State#state.producer_properties)
  },
  case pulserl_conn:send_simple_command(
    State#state.connection, Command
  ) of
    {error, _} = Err ->
      Err;
    #'CommandProducerSuccess'{
      producer_name = ProducerName,
      last_sequence_id = LSeqId
    } ->
      State#state{
        producer_name = ProducerName,
        sequence_id =
        case LSeqId >= 0 of
          true -> LSeqId + 1;
          _ -> State#state.sequence_id
        end
      }
  end.

start_ack_timeout_timer(#state{send_timeout = 0} = State, _Delay) ->
  State;
start_ack_timeout_timer(State, Delay) ->
  Delay2 = erlang:min(Delay, State#state.send_timeout),
  State#state{
    send_timeout_timer = erlang:send_after(Delay2, self(), ?INFO_ACK_TIMEOUT)
  }.

start_send_batch_timer(#state{batch_enable = false} = State) ->
  State;
start_send_batch_timer(#state{batch_max_delay_ms = BatchDelay} = State) ->
  State#state{
    batch_send_timer = erlang:send_after(BatchDelay, self(), ?INFO_SEND_BATCH)
  }.

create_inner_producer(Index, State) ->
  create_inner_producer(3, Index, State).


create_inner_producer(Retries, Index,
    #state{topic = Topic, options = Opts} = State) ->
  PartitionedTopic = topic_utils:new_partition(Topic, Index),
  case pulserl_producer:start_link(PartitionedTopic, Opts) of
    {ok, Pid} ->
      {Pid, State#state{
        partition_to_child = dict:store(Index, Pid, State#state.partition_to_child),
        child_to_partition = dict:store(Pid, Index, State#state.child_to_partition)
      }};
    {error, _} = Error ->
      case Retries > 0 of
        true -> create_inner_producer(Retries - 1, Index, State);
        _ -> Error
      end
  end.

cancel_all_timers(#state{} = State) ->
  State#state{
    re_init_timer = do_cancel_timer(State#state.re_init_timer),
    batch_send_timer = do_cancel_timer(State#state.batch_send_timer),
    send_timeout_timer = do_cancel_timer(State#state.send_timeout_timer)
  }.

do_cancel_timer(TimeRef) when is_reference(TimeRef) ->
  erlang:cancel_timer(TimeRef);
do_cancel_timer(Data) ->
  Data.

close_children(State, AttemptRestart) ->
  lists:foreach(
    fun(Pid) ->
      pulserl_producer:close(Pid, AttemptRestart)
    end, dict:fetch_keys(State#state.child_to_partition)),
  State#state{
    child_to_partition = dict:new(),
    partition_to_child = dict:new()
  }.

%% Check whether this producer is:
%% 1  -> a non-partitioned producer or
%%
%% 2  -> a parent of a some partitioned producers.
%%       Request sent to this parent are routed to the necessary child producer
is_single_or_parent(Topic) ->
  not topic_utils:is_partitioned(Topic).

notify_instance_provider_of_state(
    #state{topic = Topic} = State,
    Event) ->
  case is_single_or_parent(Topic) of
    true ->
      erlang:send(pulserl_instance_registry, {Event, self(), Topic});
    _ ->
      ok
  end,
  State.

validate_options(Options) when is_list(Options) ->
  erlwater_assertions:is_proplist(Options),
  lists:foreach(
    fun({producer, Options2} = Opt) ->
      validate_options(Options2),
      Opt;
      ({name, _V} = Opt) ->
        erlwater_assertions:is_string(Opt);
      ({properties, _V} = Opt) ->
        erlwater_assertions:is_proplist(Opt);
      ({send_timeout, _V} = Opt) ->
        erlwater_assertions:is_non_negative_int(Opt);
      ({initial_sequence_id, _V} = Opt) ->
        erlwater_assertions:is_non_negative_int(Opt);
      ({batch_enable, _} = Opt) ->
        erlwater_assertions:is_boolean(Opt);
      ({routing_mode, Val} = Opt) ->
        case Val of
          ?SINGLE_ROUTING ->
            Opt;
          ?ROUND_ROBIN_ROUTING ->
            Opt;
          {M, F} when is_atom(M) andalso is_atom(F) ->
            Opt;
          _ ->
            error(invalid_option, [Val])
        end;
      ({block_on_full_queue, _V} = Opt) ->
        erlwater_assertions:is_boolean(Opt);
      ({batch_max_messages, _V} = Opt) ->
        erlwater_assertions:is_positive_int(Opt);
      ({batch_max_delay_ms, _V} = Opt) ->
        erlwater_assertions:is_positive_int(Opt);
      ({max_pending_requests, _V} = Opt) ->
        erlwater_assertions:is_positive_int(Opt);
      ({max_pending_requests_across_partitions, _V} = Opt) ->
        erlwater_assertions:is_positive_int(Opt);
      (Opt) ->
        error(unknown_producer_options, [Opt])
    end,
    Options),
  Options.