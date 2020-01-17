%%%-------------------------------------------------------------------
%%% @author Alpha Umaru Shaw <shawalpha5@gmail.com>
%%% @doc
%%%
%%% @end
%%% Company: Skulup Ltd
%%% Copyright: (C) 2019
%%%-------------------------------------------------------------------
-module(pulserl_producer).
-author("Alpha Umaru Shaw").

-include("pulserl.hrl").
-include("pulserl_topics.hrl").
-include("pulsar_api.hrl").

-behaviour(gen_server).

%% API
-export([create/2, produce/3, sync_produce/3]).
-export([start_link/2, inform/2, close/2]).


%% gen_server callbacks
-export([init/1,
  handle_call/3,
  handle_cast/2,
  handle_info/2,
  terminate/2,
  code_change/3]).


produce(Pid, #prod_message{} = Message, Callback) ->
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
      {redirect, PartitionProducerPid} ->
        produce(PartitionProducerPid, Message, Callback)
    end
  catch
    _:{timeout, _} ->
      {error, {producer_error, timeout}};
    _:Reason ->
      {error, {producer_error, Reason}}
  end.


sync_produce(Pid, #prod_message{} = Message, Timeout) ->
  ClientRef = erlang:make_ref(),
  MonitorRef = erlang:monitor(process, Pid),
  case gen_server:call(Pid, {send_message, {self(), ClientRef}, Message}) of
    {redirect, SpecificPartitionProducer} ->
      erlang:demonitor(MonitorRef, [flush]),
      sync_produce(SpecificPartitionProducer, Message, Timeout);
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


create(#topic{} = Topic, Options) ->
  Options2 = validate_options(Options),
  supervisor:start_child(pulserl_producer_sup, [Topic, Options2]).


validate_options(Options) when is_list(Options) ->
  erlwater_assertions:is_proplist(Options),
  lists:foreach(
    fun({batch_enable, _} = Opt) ->
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
      ({initial_sequence_id, _V} = Opt) ->
        erlwater_assertions:is_non_negative_int(Opt);
      ({producer_name, _V} = Opt) ->
        erlwater_assertions:is_string(Opt);
      ({properties, _V} = Opt) ->
        erlwater_assertions:is_proplist(Opt);
      (Opt) ->
        error(unknown_producer_options, [Opt])
    end,
    Options),
  Options.



start_link(#topic{} = Topic, Options) ->
  gen_server:start_link(?MODULE, [Topic, Options], []).

inform(Pid, Information) ->
  gen_server:cast(Pid, Information).

close(Pid, Restart) ->
  gen_server:cast(Pid, {close, Restart}).


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
  partition_count :: integer(),
  partition_to_child = dict:new(),
  child_to_partition = dict:new(),
  batch_requests = queue:new(),
  pending_requests = dict:new(),
  batch_send_timer,
  sequence_id :: integer(),
  re_init_timer,
  %%Config
  options :: list(),
  batch_enable :: boolean(),
  batch_max_messages :: integer(),
  batch_max_delay_ms :: integer(),
  max_pending_messages :: integer(),
  max_pending_messages_across_partitions :: integer(),
  block_on_full_queue :: boolean(),
  initial_sequence_id :: integer(),
  producer_name :: string(),
  properties = [] :: list(),
  topic :: #topic{}
}).


init([#topic{} = Topic, Opts]) ->
  process_flag(trap_exit, true),
  State = #state{
    topic = Topic,
    options = Opts,
    batch_enable = proplists:get_value(batch_enable, Opts, true),
    batch_max_delay_ms = proplists:get_value(batch_max_delay_ms, Opts, 1),
    batch_max_messages = proplists:get_value(batch_max_messages, Opts, 1000),
    max_pending_messages = proplists:get_value(max_pending_messages, Opts, 1000),
    block_on_full_queue = proplists:get_value(block_on_full_queue, Opts, true),
    initial_sequence_id = proplists:get_value(initial_sequence_id, Opts),
    producer_name = proplists:get_value(producer_name, Opts),
    properties = proplists:get_value(properties, Opts, []),
    max_pending_messages_across_partitions = proplists:get_value(
      max_pending_messages_across_partitions, Opts, 50000)
  },
  {InitSeqId, SeqId} = case State#state.initial_sequence_id of undefined -> {0, 0}; I ->
    {I, I + 1} end,
  case initialize(State#state{initial_sequence_id = InitSeqId, sequence_id = SeqId}) of
    {error, Reason} ->
      {stop, Reason};
    NewState ->
      erlang:send(instance_provider, {producer_up, self(), Topic}),
      {ok, NewState}
  end.


handle_call({send_message, _ClientFrom, _}, _From, #state{state = undefined} = State) ->
  {reply, ?ERROR_PRODUCER_NOT_READY, State};

handle_call({send_message, _ClientFrom, #prod_message{key = Key}}, _From,
    #state{partition_count = PartitionCount} = State)
  when is_integer(PartitionCount), PartitionCount > 0 ->
  %% This producer is the partition parent.
  %% We choose the child producer and redirect
  %% the client to it.
  Replay =
    case choose_partition_producer(Key, State) of
      {ok, Pid} -> {redirect, Pid};
      _ -> ?ERROR_PRODUCER_NOT_READY
    end,
  {reply, Replay, State};

handle_call({send_message, ClientFrom, Message}, _From, State) ->
  {Reply, NewState} = may_be_produce_message(Message, ClientFrom, State),
  {reply, Reply, NewState};
handle_call(Request, _From, State) ->
  error_logger:warning_msg("Unexpected call: ~p in ~p(~p)", [Request, ?MODULE, self()]),
  {reply, ok, State}.

%% The producer was ask to close
handle_cast({on_command, _, #'CommandCloseProducer'{}}, State) ->
  State2 = send_reply_to_all_waiters(?ERROR_PRODUCER_CLOSED, State),
  {noreply, try_reinitialize(State2#state{state = undefined})};


handle_cast({close, Restart}, State) ->
  case Restart of
    true ->
      error_logger:info_msg("Temporariliy closing producer: ~p",
        [topic_utils:to_string(State#state.topic)]),
      State2 = close_children(State, Restart),
      State3 = send_reply_to_all_waiters(?ERROR_PRODUCER_CLOSED, State2),
      {noreply, try_reinitialize(State3#state{state = undefined})};
    _ ->
      error_logger:info_msg("Producer(~p) at: ~p is permanelty closing",
        [self(), topic_utils:to_string(State#state.topic)]),
      State2 = send_reply_to_all_waiters(?ERROR_PRODUCER_CLOSED, State),
      {close, normal, close_children(State2, Restart)}
  end;

handle_cast({ack_received, SequenceId, LedgerId, EntryId},
    #state{topic = Topic} = State) ->
  Reply = pulserl_utils:new_message_id(Topic, LedgerId, EntryId),
  {noreply, send_reply_to_producer_waiter(SequenceId, Reply, State)};

handle_cast({send_error, SequenceId, {error, _} = Error}, State) ->
  {noreply, send_reply_to_producer_waiter(SequenceId, Error, State)};


handle_cast(Request, State) ->
  error_logger:warning_msg("Unexpected Cast: ~p", [Request]),
  {noreply, State}.


handle_info({timeout, _TimerRef, send_batch},
    #state{batch_enable = true,
      batch_max_messages = BatchMaxMessages,
      batch_requests = BatchRequests} = State) ->
  BatchRequestsLen = queue:len(BatchRequests),
  if BatchRequestsLen > 0 ->
    Size = erlang:min(BatchMaxMessages, queue:len(BatchRequests)),
    {NextBatch, NewBatchRequests} = next_request_batch(State, Size),
    State2 = State#state{batch_requests = NewBatchRequests},
    {noreply, send_batch_messages(NextBatch, start_batch_timer(State2))};
    true ->
      {noreply, start_batch_timer(State)}
  end;

%% Last reinitialization failed. Still trying..
handle_info(try_reinitialize, State) ->
  {noreply, try_reinitialize(State)};

%% Our connection is down. We stop the scheduled
%% re-initialization attempts and try again after
%% a `connection_up` message
handle_info(connection_down, State) ->
  if State#state.re_init_timer /= undefined ->
    erlang:cancel_timer(State#state.re_init_timer),
    {noreply, State#state{state = undefined, re_init_timer = undefined}};
    true ->
      {noreply, State}
  end;

%% Try re-initialization again
handle_info(connection_up, State) ->
  {noreply, try_reinitialize(State)};


handle_info({'DOWN', _ConnMonitorRef, process, _Pid, _},
    #state{} = State) ->
  %% This hardly happens as we design the
  %% connection to avoid frequent death
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


terminate(_Reason, #state{topic = Topic} = _State) ->
  erlang:send(instance_provider, {producer_down, self(), Topic}),
  ok.


code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
send_reply_to_producer_waiter(SequenceId, Reply,
    #state{pending_requests = PendingRequests} = State) ->
  {SucceededPendingRequests, PendingRequests2} = dict:take(SequenceId, PendingRequests),
  State2 = send_replies_to_waiters(Reply, SucceededPendingRequests, State),
  State2#state{pending_requests = PendingRequests2}.


choose_partition_producer(Key, State) ->
  Partition = pulserl_utils:hash_key(Key, State#state.partition_count),
  dict:find(Partition, State#state.partition_to_child).


%% @Todo add logic for blocking on queue full
may_be_produce_message(Message, From, State) ->
  Request = {From, Message},
  if State#state.batch_enable ->
    case add_to_pending_or_blocking(Request, State) of
      {notfull, NewBatchRequests} ->
        %% Was added but still the `pending_requests` has some space.
        %% Tell the client to chill whilst; we'll send the response
        %% when either the `batch_max_delay_ms` timeouts or
        %% `pending_requests` is reached.
        {ok, may_be_trigger_batch(State#state{batch_requests = NewBatchRequests})};
      {full, NewBatchRequests} ->
        %% The new request just fills the `pending_requests` queue.
        %% Send `ok` to the client and trigger a batch send if none is in progress
        {ok, may_be_trigger_batch(State#state{batch_requests = NewBatchRequests})};
      {fulled, NewBatchRequests} ->
        %% The `pending_requests` is already fulled,
        {ok, may_be_trigger_batch(State#state{batch_requests = NewBatchRequests})}
    end;
    true ->
      %% Batching not enabled
      {ok, send_message(Request, State)}
  end.


may_be_trigger_batch(#state{
  batch_max_messages = MaxBatchMessages} = State) ->
  case next_request_batch(State, MaxBatchMessages) of
    {[], _} -> State;
    {NextBatch, NewBatchReqs} ->
      NewState = State#state{
        batch_requests = NewBatchReqs},
      send_batch_messages(NextBatch, NewState)
  end.


next_request_batch(#state{batch_requests = BatchRequests}, 0) ->
  {[], BatchRequests};
next_request_batch(#state{batch_requests = BatchRequests} = State, Size) ->
  case queue:len(BatchRequests) >= Size of
    true ->
      {BatchableReqQueue, RemainingQueue} = queue:split(Size, BatchRequests),
      update_pending_messages_count_across_partitions(
        State, - queue:len(BatchableReqQueue)),
      {queue:to_list(BatchableReqQueue), RemainingQueue};
    _ ->
      {[], BatchRequests}
  end.

send_message({_, #prod_message{value = Payload} = Msg} = Request,
    #state{connection = Cnx, sequence_id = SeqId} = State) ->
  {SendCmd, Metadata} = commands:new_send(State#state.producer_id,
    State#state.producer_name, SeqId, Msg#prod_message.key, Msg#prod_message.event_time,
    %% `num_messages_in_batch` must be undefined for non-batch messages
    undefined, Payload),
  PendingRequests = dict:store(SeqId, [Request], State#state.pending_requests),
  NewState = State#state{sequence_id = SeqId + 1, pending_requests = PendingRequests},
  pulserl_conn:send_payload_command(Cnx, SendCmd, Metadata, Payload),
  NewState.


send_batch_messages(Batch, #state{
  connection = Cnx,
  sequence_id = SeqId,
  producer_id = ProducerId,
  producer_name = ProducerName} = State) ->
  {FinalSeqId, BatchPayload} = lists:foldl(
    fun({_, Msg}, {SeqId0, BatchBuffer0}) ->
      Payload = Msg#prod_message.value,
      SingleMsgMeta =
        #'SingleMessageMetadata'{
          sequence_id = SeqId0,
          payload_size = byte_size(Payload),
          partition_key = Msg#prod_message.key,
          properties = Msg#prod_message.properties,
          event_time = Msg#prod_message.event_time
        },
      SerializedSingleMsgMeta = pulsar_api:encode_msg(SingleMsgMeta),
      SerializedSingleMsgMetaSize = byte_size(SerializedSingleMsgMeta),
      BatchBuffer1 = erlang:iolist_to_binary([BatchBuffer0,
        commands:to_4bytes(SerializedSingleMsgMetaSize),
        SerializedSingleMsgMeta, Payload
      ]),
      {SeqId + 1, BatchBuffer1}
    end, {SeqId, <<>>}, Batch),
  [{_, FirstMsg} | _] = Batch,
  {SendCmd, Metadata} = commands:new_send(ProducerId, ProducerName, FinalSeqId,
    undefined, FirstMsg#prod_message.event_time,
    length(Batch), BatchPayload
  ),
  PendingRequests = dict:store(FinalSeqId, Batch, State#state.pending_requests),
  NewState = State#state{sequence_id = FinalSeqId, pending_requests = PendingRequests},
  pulserl_conn:send_payload_command(Cnx, SendCmd, Metadata, BatchPayload),
  NewState.


send_reply_to_all_waiters(Reply, State) ->
  BatchReqs = queue:to_list(State#state.batch_requests),
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
        _:Reason:Stacktrace ->
          error_logger:error_msg("Error(~p) on replying to "
          "the client.", [{Reason, Stacktrace}])
      end
    end, Requests),
  State.


add_to_pending_or_blocking(Request, #state{
  batch_requests = BatchRequests,
  max_pending_messages = MaxPendingMessages,
  max_pending_messages_across_partitions = MaxPendingPartitionedMessages
} = State) ->
  BatchRequestsLen = queue:len(BatchRequests),
  case (BatchRequestsLen < MaxPendingMessages)
    andalso (MaxPendingPartitionedMessages >
      %% Increment by zero to read
    update_pending_messages_count_across_partitions(State, 0)) of
    true ->
      %% Add to the `pending_requests` queue
      {CrossPartitionsPendingLen2, BatchRequestsLen2, NewBatchRequests} = enqueue_request(Request, State),
      %% Check again if it's still not full
      if (BatchRequestsLen2 < MaxPendingMessages) andalso (MaxPendingPartitionedMessages > CrossPartitionsPendingLen2) ->
        {notfull, NewBatchRequests};
        true ->
          {full, NewBatchRequests}
      end;
    _ ->
      {_, _, NewBatchRequests} = enqueue_request(Request, State),
      {fulled, NewBatchRequests}
  end.


enqueue_request(Request, #state{batch_requests = BatchRequests} = State) ->
  NewBatchRequests = queue:in(Request, BatchRequests),
  {update_pending_messages_count_across_partitions(State, 1),
    queue:len(NewBatchRequests), NewBatchRequests}.


update_pending_messages_count_across_partitions(
    #state{topic = Topic}, Increment) ->
  if Topic#topic.parent /= undefined ->
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
      " '~p'. Restarting...", [ExitedPid, topic_utils:new_partition_str(
        State#state.topic, Partition), Reason]),
      State2 = State#state{
        partition_to_child = dict:erase(Partition, State#state.partition_to_child),
        child_to_partition = dict:erase(ExitedPid, State#state.child_to_partition)
      },
      case create_inner_producer(Partition, State2) of
        {_NewPid, #state{} = NewState} ->
          error_logger:info_msg("Producer to '~s' restarted.",
            [topic_utils:new_partition_str(State#state.topic, Partition)]),
          NewState;
        {error, NewReason} = Error ->
          error_logger:error_msg("Producer to '~s' restart failed. Reason: ~p",
            [topic_utils:new_partition_str(State#state.topic, Partition), NewReason]),
          Error
      end;
    error ->
      %% We're told to exit by our parent
      {error, Reason}
  end.


try_reinitialize(State) ->
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
      do_simple_initialization(State);
    _ ->
      do_initialization(State)
  end.


do_initialization(#state{topic = Topic} = State) ->
  case pulserl_client:get_partitioned_topic_meta(Topic) of
    #partition_meta{partitions = PartitionCount} ->
      State2 = State#state{partition_count = PartitionCount},
      case PartitionCount of
        0 ->
          do_simple_initialization(State2);
        _ ->
          case create_inner_producers(State2) of
            {ok, NewState2} ->
              NewState2#state{state = ?STATE_READY};
            {error, _} = Error ->
              Error
          end
      end;
    {error, _} = Error -> Error
  end.


do_simple_initialization(#state{topic = Topic} = State) ->
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

create_inner_producers(#state{partition_count = Total} = State) ->
  {NewState, Err} = lists:foldl(
    fun(Index, {S, Error}) ->
      if Error == undefined ->
        case create_inner_producer(Index, S) of
          {_, #state{} = S2} ->
            {S2, Error};
          Error0 ->
            {S, Error0}
        end;
        true ->
          {S, Error}
      end
    end, {State, undefined}, lists:seq(0, Total - 1)),
  case Err of
    undefined ->
      {ok, NewState};
    {error, _} = Err ->
      [pulserl_producer:close(Pid, false) || {_, Pid} <- dict:fetch_keys(NewState#state.child_to_partition)],
      Err
  end.


establish_producer(#state{topic = Topic} = State) ->
  Command = #'CommandProducer'{
    topic = topic_utils:to_string(Topic),
    producer_id = State#state.producer_id,
    producer_name = State#state.producer_name},
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


close_children(State, Restart) ->
  lists:foreach(
    fun(Pid) ->
      pulserl_producer:close(Pid, Restart)
    end, dict:fetch_keys(State#state.child_to_partition)),
  State#state{
    child_to_partition = dict:new(),
    partition_to_child = dict:new()
  }.