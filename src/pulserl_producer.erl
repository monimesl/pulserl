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
-export([produce/3, sync_produce/2, sync_produce/3, new_message/1, new_message/2, new_message/3, new_message/4, new_message/5]).


%%--------------------------------------------------------------------
%% @doc

%% @end
%%--------------------------------------------------------------------
produce(Pid, #prodMessage{} = Message, Callback) ->
  {CallerFun, CallReturn} =
    if is_function(Callback) ->
      {fun() -> gen_server:call(Pid, {send_message, Callback, Message}) end, ok};
      true ->
        ClientRef = erlang:make_ref(),
        {fun() -> gen_server:call(Pid, {send_message, {self(), ClientRef}, Message}) end, ClientRef}
    end,
  try
    case CallerFun() of
      ok ->
        CallReturn;
      {error, _} = Error ->
        Error;
      {redirect, ChildProducerPid} ->
        produce(ChildProducerPid, Message, Callback)
    end
  catch
    _:{timeout, _} ->
      {error, {producer_error, timeout}};
    _:Reason ->
      {error, {producer_error, Reason}}
  end.

%%--------------------------------------------------------------------
%% @doc

%% @end
%%--------------------------------------------------------------------
sync_produce(Pid, #prodMessage{} = Message) ->
  sync_produce(Pid, Message, ?UNDEF).

sync_produce(Pid, #prodMessage{} = Message, ?UNDEF) ->
  sync_produce(Pid, Message, timer:seconds(10));

sync_produce(Pid, #prodMessage{} = Message, Timeout) ->
  ClientRef = erlang:make_ref(),
  MonitorRef = erlang:monitor(process, Pid),
  case gen_server:call(Pid, {send_message, {self(), ClientRef}, Message}) of
    {redirect, ChildProducerPid} ->
      erlang:demonitor(MonitorRef, [flush]),
      sync_produce(ChildProducerPid, Message, Timeout);
    ok ->
      receive
        {ClientRef, Reply} ->
          erlang:demonitor(MonitorRef, [flush]),
          Reply;
        {'DOWN', MonitorRef, process, _Pid, Reason} ->
          {error, {producer_down, Reason}}
      after Timeout ->
        erlang:demonitor(MonitorRef, [flush]),
        {error, timeout}
      end;
    Other ->
      Other
  end.

%%--------------------------------------------------------------------
%% @doc

%% @end
%%--------------------------------------------------------------------
new_message(Value) ->
  new_message(<<>>, Value).

%%--------------------------------------------------------------------
%% @doc

%% @end
%%--------------------------------------------------------------------
new_message(Key, Value) ->
  new_message(Key, Value, []).

%%--------------------------------------------------------------------
%% @doc

%% @end
%%--------------------------------------------------------------------
new_message(Key, Value, Properties) ->
  new_message(Key, Value, Properties, erlwater_time:milliseconds()).

%%--------------------------------------------------------------------
%% @doc

%% @end
%%--------------------------------------------------------------------
new_message(Key, Value, Properties, EventTime) ->
  new_message(Key, Value, Properties, EventTime, ?UNDEF).

new_message(Key, Value, Properties, EventTime, DeliverAtTime) ->
  Key2 = case Key of ?UNDEF -> <<>>; _ -> Key end,
  #prodMessage{
    key = erlwater:to_binary(Key2),
    value = erlwater:to_binary(Value),
    event_time = erlwater:to_integer(EventTime),
    deliverAtTime = DeliverAtTime,
    properties = Properties
  }.

%%--------------------------------------------------------------------
%% @doc

%% @end
%%--------------------------------------------------------------------
create(Topic, Options) when is_list(Topic) ->
  create(list_to_binary(Topic), Options);

create(Topic, Options) when is_binary(Topic) ->
  create(topic_utils:parse(Topic), Options);

create(#topic{} = Topic, Options) ->
  Options2 = validate_options(Options),
  supervisor:start_child(pulserl_producer_sup, [Topic, Options2]).


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

-define(STATE_READY, ready).
-define(ERROR_PRODUCER_CLOSED, {error, producer_closed}).
-define(ERROR_PRODUCER_NOT_READY, {error, producer_not_ready}).

-record(state, {
  state,
  connection :: pid(),
  producer_id :: integer(),
  %% start of producer options
  producer_name :: string(),
  producer_properties = [] :: list(),
  producer_initial_sequence_id :: integer(),
  %% end of producer options
  topic :: #topic{},
  partition_count = 0 :: non_neg_integer(),
  partition_to_child = dict:new(),
  child_to_partition = dict:new(),
  request_queue = queue:new(),
  pending_requests = dict:new(),
  batch_send_timer,
  sequence_id :: integer(),
  re_init_timer,
  %%Config
  options :: list(),
  batch_enable :: boolean(),
  max_batched_messages :: integer(),
  batch_max_delay_ms :: integer(),
  max_pending_messages :: integer(),
  max_pending_messages_across_partitions :: integer(),
  block_on_full_queue :: boolean()
}).


init([#topic{} = Topic, Opts]) ->
  process_flag(trap_exit, true),
  ProducerOpts = proplists:get_value(producer, Opts, []),
  State = #state{
    topic = Topic,
    options = Opts,
    producer_name = proplists:get_value(name, ProducerOpts),
    producer_properties = proplists:get_value(properties, ProducerOpts, []),
    producer_initial_sequence_id = proplists:get_value(initial_sequence_id, ProducerOpts, 0),
    batch_enable = proplists:get_value(batch_enable, Opts, true),
    batch_max_delay_ms = proplists:get_value(batch_max_delay_ms, Opts, 10),
    max_batched_messages = proplists:get_value(batch_max_messages, Opts, 1000),
    max_pending_messages = proplists:get_value(max_pending_messages, Opts, 1000),
    block_on_full_queue = proplists:get_value(block_on_full_queue, Opts, true),
    max_pending_messages_across_partitions = proplists:get_value(
      max_pending_messages_across_partitions, Opts, 50000)
  },
  {InitSeqId, SeqId} = case State#state.producer_initial_sequence_id of 0 -> {0, 0}; I ->
    {I, I + 1} end,
  case initialize(State#state{producer_initial_sequence_id = InitSeqId, sequence_id = SeqId}) of
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
  Replay =
    case choose_partition_producer(Key, State) of
      {ok, Pid} -> {redirect, Pid};
      _ -> ?ERROR_PRODUCER_NOT_READY
    end,
  {reply, Replay, State};

%% The child/no-child-producer
handle_call({send_message, ClientFrom, Message}, _From, State) ->
  {Reply, NewState} = may_be_produce_message(Message, ClientFrom, State),
  {reply, Reply, NewState};
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

handle_info({ack_received, SequenceId, MessageId}, #state{topic = Topic} = State) ->
  Reply = pulserl_utils:new_message_id(Topic, MessageId),
  {noreply, send_reply_to_producer_waiter(SequenceId, Reply, State)};

handle_info({send_error, SequenceId, {error, _} = Error}, State) ->
  {noreply, send_reply_to_producer_waiter(SequenceId, Error, State)};

handle_info({timeout, _TimerRef, send_batch},
    #state{batch_enable = true,
      max_batched_messages = BatchMaxMessages,
      request_queue = RequestQueue} = State) ->
  BatchRequestsLen = queue:len(RequestQueue),
  if BatchRequestsLen > 0 ->
    Size = erlang:min(BatchMaxMessages, queue:len(RequestQueue)),
    {NextBatch, NewRequestQueue} = next_requests_batch(State, Size),
    State2 = State#state{request_queue = NewRequestQueue},
    {noreply, send_batch_messages(NextBatch, start_batch_timer(State2))};
    true ->
      {noreply, start_batch_timer(State)}
  end;

%% Our connection is down. We stop the scheduled
%% re-initialization attempts and try again after
%% a `connection_up` message
handle_info(connection_down, State) ->
  NewState = send_reply_to_all_waiters(?ERROR_PRODUCER_CLOSED, State),
  if NewState#state.re_init_timer /= ?UNDEF ->
    erlang:cancel_timer(NewState#state.re_init_timer),
    {noreply, NewState#state{state = ?UNDEF, re_init_timer = ?UNDEF}};
    true ->
      {noreply, NewState}
  end;

%% Try re-initialization again
handle_info(connection_up, State) ->
  {noreply, reinitialize(State)};

%% Last reinitialization failed. Still trying..
handle_info(try_reinitialize, State) ->
  {noreply, reinitialize(State)};


handle_info({'DOWN', _ConnMonitorRef, process, _Pid, _},
    #state{} = State) ->
  %% This shouldn't happen as we design the connection to avoid crashing
  {stop, normal, send_reply_to_all_waiters(?ERROR_PRODUCER_CLOSED, State)};

handle_info({'EXIT', Pid, Reason}, State) ->
  case Reason of
    normal -> {noreply, State};
    _ ->
      case maybe_inner_producer_exited(Pid, Reason, State) of
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

send_reply_to_producer_waiter(SequenceId, Reply,
    #state{pending_requests = PendingRequests} = State) ->
  case dict:take(SequenceId, PendingRequests) of
    {SucceededPendingRequests, NewPendingRequests} ->
      State2 = send_replies_to_waiters(Reply, SucceededPendingRequests, State),
      State2#state{pending_requests = NewPendingRequests};
    _ ->
      error_logger:warning_msg("Couldn't see the waiter in the waiting list of "
      "producer: ~p during reply with sequence id: ~p", [self(), SequenceId]),
      State
  end.


choose_partition_producer(Key, State) ->
  Partition = pulserl_utils:hash_key(Key, State#state.partition_count),
  dict:find(Partition, State#state.partition_to_child).


%% @Todo add logic for blocking on queue full
may_be_produce_message(Message, From, State) ->
  Request = {From, Message},
  if State#state.batch_enable andalso Message#prodMessage.deliverAtTime == ?UNDEF ->
    case add_to_request_to_batch(Request, State) of
      {notfull, NewRequestQueue} ->
        %% Was added but still the `pending_requests` has some space.
        %% Tell the client to chill whilst; we'll send the response
        %% when either the `batch_max_delay_ms` timeouts or
        %% `pending_requests` is reached.
        {ok, may_be_trigger_batch(State#state{request_queue = NewRequestQueue})};
      {full, NewRequestQueue} ->
        %% The new request just fills the `pending_requests` queue.
        %% Send `ok` to the client and trigger a batch send if none is in progress
        {ok, may_be_trigger_batch(State#state{request_queue = NewRequestQueue})};
      {fulled, NewRequestQueue} ->
        %% The `pending_requests` is already fulled,
        {ok, may_be_trigger_batch(State#state{request_queue = NewRequestQueue})}
    end;
    true ->
      %% No batching. Send directly
      {ok, send_message(Request, State)}
  end.


may_be_trigger_batch(#state{
  max_batched_messages = MaxBatchMessages} = State) ->
  case next_requests_batch(State, MaxBatchMessages) of
    {[], _} -> State;
    {NextBatch, NewRequestQueue} ->
      NewState = State#state{
        request_queue = NewRequestQueue},
      send_batch_messages(NextBatch, NewState)
  end.


next_requests_batch(#state{request_queue = RequestQueue}, 0) ->
  {[], RequestQueue};
next_requests_batch(#state{request_queue = RequestQueue} = State, Size) ->
  case queue:len(RequestQueue) >= Size of
    true ->
      {NextBatchQueue, RemainingQueue} = queue:split(Size, RequestQueue),
      update_pending_count_across_partitions(
        State, - queue:len(NextBatchQueue)),
      {queue:to_list(NextBatchQueue), RemainingQueue};
    _ ->
      {[], RequestQueue}
  end.

send_message({_, #prodMessage{value = Payload} = Msg} = Request,
    #state{connection = Cnx, sequence_id = SeqId} = State) ->
  {SendCmd, Metadata} = commands:new_send(State#state.producer_id,
    State#state.producer_name, SeqId, Msg#prodMessage.key, Msg#prodMessage.event_time,
    %% `num_messages_in_batch` must be undefined for non-batch messages
    ?UNDEF, Msg#prodMessage.deliverAtTime, Payload),
  PendingRequests = dict:store(SeqId, [Request], State#state.pending_requests),
  NewState = State#state{sequence_id = SeqId + 1, pending_requests = PendingRequests},
  pulserl_conn:send_payload_command(Cnx, SendCmd, Metadata, Payload),
  NewState.


send_batch_messages([Request], State) ->
  %% Only one request. Don't batch it
  send_message(Request, State);

send_batch_messages(Batch, #state{
  connection = Cnx,
  sequence_id = SeqId,
  producer_id = ProducerId,
  producer_name = ProducerName} = State) ->
  {FinalSeqId, BatchPayload} = lists:foldl(
    fun({_, Msg}, {SeqId0, BatchBuffer0}) ->
      Payload = Msg#prodMessage.value,
      SingleMsgMeta =
        #'SingleMessageMetadata'{
          sequence_id = SeqId0,
          payload_size = byte_size(Payload),
          partition_key = Msg#prodMessage.key,
          properties = Msg#prodMessage.properties,
          event_time = Msg#prodMessage.event_time
        },
      SerializedSingleMsgMeta = pulsar_api:encode_msg(SingleMsgMeta),
      SerializedSingleMsgMetaSize = byte_size(SerializedSingleMsgMeta),
      BatchBuffer1 = erlang:iolist_to_binary([BatchBuffer0,
        pulserl_io:to_4bytes(SerializedSingleMsgMetaSize),
        SerializedSingleMsgMeta, Payload
      ]),
      {SeqId + 1, BatchBuffer1}
    end, {SeqId, <<>>}, Batch),
  [{_, FirstMsg} | _] = Batch,
  {SendCmd, Metadata} = commands:new_send(ProducerId, ProducerName, FinalSeqId,
    ?UNDEF, FirstMsg#prodMessage.event_time,
    length(Batch), ?UNDEF, BatchPayload
  ),
  PendingRequests = dict:store(FinalSeqId, Batch, State#state.pending_requests),
  NewState = State#state{sequence_id = FinalSeqId, pending_requests = PendingRequests},
  pulserl_conn:send_payload_command(Cnx, SendCmd, Metadata, BatchPayload),
  NewState.


send_reply_to_all_waiters(Reply, State) ->
  BatchReqs = queue:to_list(State#state.request_queue),
  PendingReqs = [From || {_, From} <- dict:to_list(State#state.pending_requests)],
  send_replies_to_waiters(Reply, PendingReqs ++ BatchReqs, State).


send_replies_to_waiters(Reply, Requests, State) ->
  lists:foreach(
    fun({Client, _}) ->
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
    end, Requests),
  State.


add_to_request_to_batch(Request, #state{
  request_queue = RequestQueue,
  max_pending_messages = MaxPendingMessages,
  max_pending_messages_across_partitions = MaxPendingPartitionedMessages
} = State) ->
  RequestQueueLen = queue:len(RequestQueue),
  case (RequestQueueLen < MaxPendingMessages)
    andalso (MaxPendingPartitionedMessages >
      %% Increment by zero to read current count
    update_pending_count_across_partitions(State, 0)) of
    true ->
      %% Add to the `pending_requests` queue
      {CrossPartitionsPendingLen2, BatchRequestsLen2, NewBatchRequests} = add_to_request_to_batch2(Request, State),
      %% Check again if it's still not full
      if (BatchRequestsLen2 < MaxPendingMessages) andalso (MaxPendingPartitionedMessages > CrossPartitionsPendingLen2) ->
        {notfull, NewBatchRequests};
        true ->
          {full, NewBatchRequests}
      end;
    _ ->
      {_, _, NewBatchRequests} = add_to_request_to_batch2(Request, State),
      {fulled, NewBatchRequests}
  end.


add_to_request_to_batch2(Request, #state{request_queue = RequestQueue} = State) ->
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


maybe_inner_producer_exited(ExitedPid, Reason, State) ->
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


reinitialize(State) ->
  Topic = topic_utils:to_string(State#state.topic),
  case initialize(State) of
    {error, Reason} ->
      error_logger:error_msg("Producer: ~p re-initialization failed: ~p", [Topic, Reason]),
      State#state{re_init_timer = erlang:send_after(500, self(), try_reinitialize)};
    NewState ->
      error_logger:info_msg("Producer: ~p re-initialization completes", [Topic]),
      NewState#state{state = ?STATE_READY}
  end.


initialize(#state{topic = Topic} = State) ->
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
  end.


initialize_self(#state{topic = Topic} = State) ->
  case pulserl_client:get_broker_address(Topic) of
    LogicalAddress when is_list(LogicalAddress) ->
      case pulserl_client:get_broker_connection(LogicalAddress) of
        {ok, Pid} ->
          Id = pulserl_conn:register_handler(Pid, self(), producer),
          establish_producer(State#state{
            connection = Pid, producer_id = Id
          });
        {error, _} = Error ->
          Error
      end;
    {error, _} = Error ->
      Error
  end.

initialize_children(#state{partition_count = NumberOfPartitions} = State) ->
  {NewState, Err} = lists:foldl(
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
    ?UNDEF ->
      %% Initialize the parent straight away
      NewState#state{state = ?STATE_READY};
    {error, _} = Err ->
      [pulserl_producer:close(Pid, false) || {_, Pid} <- dict:fetch_keys(NewState#state.child_to_partition)],
      Err
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
      NewState = State#state{
        state = ?STATE_READY,
        producer_name = ProducerName,
        sequence_id =
        case LSeqId >= 0 of
          true -> LSeqId + 1;
          _ -> State#state.sequence_id
        end
      },
      if NewState#state.batch_enable ->
        start_batch_timer(NewState);
        true ->
          NewState
      end
  end.


start_batch_timer(#state{batch_max_delay_ms = BatchDelay} = State) ->
  State#state{
    batch_send_timer = erlang:start_timer(BatchDelay, self(), send_batch)
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
      ({initial_sequence_id, _V} = Opt) ->
        erlwater_assertions:is_non_negative_int(Opt);
      ({batch_enable, _} = Opt) ->
        erlwater_assertions:is_boolean(Opt);
      ({block_on_full_queue, _V} = Opt) ->
        erlwater_assertions:is_boolean(Opt);
      ({batch_max_messages, _V} = Opt) ->
        erlwater_assertions:is_positive_int(Opt);
      ({batch_max_delay_ms, _V} = Opt) ->
        erlwater_assertions:is_positive_int(Opt);
      ({max_pending_messages, _V} = Opt) ->
        erlwater_assertions:is_positive_int(Opt);
      ({max_pending_messages_across_partitions, _V} = Opt) ->
        erlwater_assertions:is_positive_int(Opt);
      (Opt) ->
        error(unknown_producer_options, [Opt])
    end,
    Options),
  Options.