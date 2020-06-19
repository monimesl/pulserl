%%%-------------------------------------------------------------------
%%% @author Alpha Umaru Shaw <shawalpha5@gmail.com>
%%% @doc
%%%
%%% @end
%%% Copyright: (C) 2020, Skulup Ltd
%%%-------------------------------------------------------------------
-module(pulserl_consumer).

-include("pulserl.hrl").
-include("pulsar_api.hrl").

-behaviour(gen_server).

%% Producer API
%% gen_Server API
-export([start_link/2]).
%% Consumer API
-export([create/2, close/1, close/2]).
-export([seek/2]).
-export([receive_message/1]).
-export([acknowledge/2, acknowledge/3]).
-export([negative_acknowledge/2]).

%% gen_server callbacks
-export([init/1,
  handle_call/3,
  handle_cast/2,
  handle_info/2,
  terminate/2,
  code_change/3]).


-define(POS_LATEST, latest).
-define(POS_EARLIEST, earliest).

-define(SHARED_SUBSCRIPTION, shared).
-define(FAILOVER_SUBSCRIPTION, failover).
-define(EXCLUSIVE_SUBSCRIPTION, exclusive).
-define(KEY_SHARED_SUBSCRIPTION, key_shared).


receive_message(Pid) ->
  case gen_server:call(Pid, receive_message) of
    {redirect, Children} ->
      try_receive_from_all_if_any(Children);
    Other ->
      Other
  end.

acknowledge(Pid, #'message'{id = MessageId}) ->
  acknowledge(Pid, MessageId, false);

acknowledge(Pid, #'messageId'{} = MessageId) ->
  acknowledge(Pid, MessageId, false).

acknowledge(Pid, #'message'{id = MessageId}, Cumulative) ->
  acknowledge(Pid, MessageId, Cumulative);

acknowledge(Pid, #'messageId'{} = MessageId, Cumulative) ->
  call(Pid, {acknowledge, MessageId, Cumulative}).

negative_acknowledge(Pid, #'message'{id = MessageId}) ->
  negative_acknowledge(Pid, MessageId);

negative_acknowledge(Pid, #messageId{} = MessageId) ->
  call(Pid, {negative_acknowledge, MessageId}).

seek(Pid, Time) when is_pid(Pid) andalso is_integer(Time) ->
  gen_server:call(Pid, {seek, Time});

seek(Pid, {LedgerId, EntryId} = MsgId) when is_pid(Pid) andalso is_integer(LedgerId) andalso is_integer(EntryId) ->
  gen_server:call(Pid, {seek, MsgId}).


call(Pid, Message) ->
  case gen_server:call(Pid, Message) of
    {redirect, ChildConsumerPid} ->
      call(ChildConsumerPid, Message);
    Other ->
      Other
  end.

create(TopicName, Options) when is_list(TopicName) ->
  create(topic_utils:parse(TopicName), Options);

create(#topic{} = Topic, Options) ->
  Options2 = validate_options(Options),
  supervisor:start_child(pulserl_consumer_sup, [Topic, Options2]).

start_link(#topic{} = Topic, Options) ->
  gen_server:start_link(?MODULE, [Topic, Options], []).


close(Pid) ->
  close(Pid, false).

close(Pid, AttemptRestart) ->
  gen_server:cast(Pid, {close, AttemptRestart}).

try_receive_from_all_if_any([]) ->
  {ok, false};
try_receive_from_all_if_any([Child | Rest]) ->
  case receive_message(Child) of
    {ok, false} ->
      try_receive_from_all_if_any(Rest);
    Other ->
      Other
  end.

send_message_to_dead_letter(Pid, Message) ->
  gen_server:call(Pid, {send_to_dead_letter_topic, Message}).


-define(SERVER, ?MODULE).
-define(STATE_READY, ready).
-define(SEND_ACKNOWLEDGMENTS, send_acknowledgments).
-define(ACKNOWLEDGMENT_TIMEOUT, acknowledgment_timeout).

-define(ERROR_CONSUMER_CLOSED, {error, consumer_closed}).
-define(ERROR_CONSUMER_NOT_READY, {error, consumer_not_ready}).
-define(ERROR_CONSUMER_ID_NOT_KNOWN_HERE, {error, id_not_known_here}).
-define(ERROR_CONSUMER_NOT_FROM_PARTITIONED_TOPIC, {error, id_not_from_partitioned_topic}).
-define(ERROR_CONSUMER_CUMULATIVE_ACK_INVALID, {error, <<"Cannot use cumulative ack with shared subscription type">>}).

-record(state, {
  state,
  connection :: pid(),
  subscription :: string(),
  subscription_type :: atom(),
  consumer_id :: integer(),
  consumer_name :: string(),
  initial_position :: atom(),
  partition_count = 0 :: non_neg_integer(),
  partition_to_child = dict:new(),
  child_to_partition = dict:new(),
  options :: list(),
  topic :: #topic{},
  %%
  parent_consumer :: pid(),
  init_count = 0, %% Number of times the initialization has ran
  re_init_timer,
  flow_permits = 0,
  queue_size = 1000,
  queue_refill_threshold = 1000,
  last_dequeued_message_id,
  message_queue = queue:new() :: queue:queue(),
  %% Dead Letter Policy
  dead_letter_topic_producer :: pid(),
  dead_letter_topic_messages = ?UNDEF :: #{},
  dead_letter_topic_max_redeliver_count :: integer(),
  dead_letter_topic_name :: string(),
  retry_letter_topic_name :: string(),

  next_consumer_partition = 0,
  %%% For acknowledgements
  batch_ack_trackers = #{} :: #{},
  un_ack_message_ids = sets:new(),

  acknowledgment_timeout_timer :: reference(),
  acknowledgment_timeout = 100 :: non_neg_integer(),

  neg_acknowledgment_delay = 100,  %% Must be >= 100
  neg_acknowledgment_messages = [],
  neg_acknowledgment_redelivery_timer :: reference(),
  neg_acknowledgment_redelivery_delay = 100 :: non_neg_integer(),

  acknowledgments_send_timer :: reference(),
  acknowledgments_send_tick = 100 :: non_neg_integer(),
  max_pending_acknowledgments = 1000 :: non_neg_integer(),
  pending_acknowledgments = gb_sets:new() %%% Note :: The ids need to be sorted.
}).


%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([#topic{} = Topic, Opts]) ->
  process_flag(trap_exit, true),
  ParentConsumer = proplists:get_value(parent_consumer, Opts),
  QueueSize = erlang:max(proplists:get_value(queue_size, Opts, 1000), 1),
  SubscriptionName = proplists:get_value(subscription_name, Opts, "pulserl"),
  SubscriptionType = proplists:get_value(subscription_type, Opts, ?SHARED_SUBSCRIPTION),
  NegAckDelay = erlang:max(proplists:get_value(neg_acknowledgment_delay, Opts, 100), 100),
  DeadLetterMaxRedeliveryCount = erlang:max(proplists:get_value(dead_letter_topic_max_redeliver_count, Opts, 0), 0),
  State = #state{
    topic = Topic,
    options = Opts,
    queue_size = QueueSize,
    parent_consumer = ParentConsumer,
    subscription = SubscriptionName,
    subscription_type = SubscriptionType,
    consumer_name = proplists:get_value(consumer_name, Opts),
    initial_position = proplists:get_value(initial_position, Opts, ?POS_LATEST),
    acknowledgments_send_tick = proplists:get_value(acknowledgments_send_tick, Opts, 100),
    max_pending_acknowledgments = proplists:get_value(max_pending_acknowledgments, Opts, 1000),
    queue_refill_threshold = erlang:min(proplists:get_value(queue_refill_threshold, Opts, QueueSize div 2), 1),
    %% Ack
    acknowledgment_timeout = erlang:max(proplists:get_value(acknowledgment_timeout, Opts, 100), 100),
    %% Negative Ack
    neg_acknowledgment_delay = NegAckDelay,
    neg_acknowledgment_redelivery_delay = NegAckDelay div 4,
    %% Dead Letter Policy
    dead_letter_topic_max_redeliver_count = DeadLetterMaxRedeliveryCount,
    dead_letter_topic_name =
    if ParentConsumer == ?UNDEF ->
      dead_letter_topic_name(Opts, Topic, SubscriptionName);
      true ->
        ?UNDEF
    end,
    dead_letter_topic_messages =
    if DeadLetterMaxRedeliveryCount > 0 andalso is_pid(ParentConsumer)
      andalso SubscriptionType == ?SHARED_SUBSCRIPTION ->
      #{};
      true ->
        %% Disable
        ?UNDEF
    end
  },
  case initialize(State) of
    {error, Reason} ->
      {stop, Reason};
    State1 ->
      State2 = start_acknowledgments_send_timer(State1),
      State3 = start_acknowledgment_timeout_timer(State2),
      {ok, notify_instance_provider_of_state(State3, consumer_up)}
  end.

handle_call(_, _From, #state{state = ?UNDEF} = State) ->
  %% I'm not ready yet
  {reply, ?ERROR_CONSUMER_NOT_READY, State};

handle_call({acknowledge, _, true}, _From, #state{subscription_type = ?SHARED_SUBSCRIPTION} = State) ->
  {reply, ?ERROR_CONSUMER_CUMULATIVE_ACK_INVALID, State};

%% The parent
handle_call({acknowledge, #messageId{topic = Topic} = MsgId, _Cumulative},
    _From, #state{parent_consumer = ?UNDEF, partition_count = Pc, topic = ParentTopic} = State) when Pc > 0 ->
  case topic_utils:partition_of(ParentTopic, Topic) of
    false ->
      {reply, ?ERROR_CONSUMER_ID_NOT_KNOWN_HERE, State};
    _ ->
      {Reply, NewState} = redirect_to_the_child_partition(MsgId, State),
      {reply, Reply, NewState}
  end;

%% The child/no-child-consumer
handle_call({acknowledge, #messageId{topic = TopicStr} = MsgId, Cumulative}, _From, #state{topic = Topic} = State) ->
  case TopicStr == topic_utils:to_string(Topic) of
    false ->
      {reply, ?ERROR_CONSUMER_ID_NOT_KNOWN_HERE, State};
    _ ->
      case topic_utils:is_persistent(Topic) of
        true ->
          {Reply, NewState} = handle_acknowledgement(MsgId, Cumulative, State),
          {reply, Reply, NewState};
        _ ->
          {reply, ok, untrack_message(MsgId, Cumulative, State)}
      end
  end;

%% The parent
handle_call({negative_acknowledge, #messageId{topic = Topic} = MsgId},
    _From, #state{parent_consumer = ?UNDEF, partition_count = Pc, topic = ParentTopic} = State) when Pc > 0 ->
  case topic_utils:partition_of(ParentTopic, Topic) of
    false ->
      {reply, ?ERROR_CONSUMER_ID_NOT_KNOWN_HERE, State};
    _ ->
      {Reply, NewState} = redirect_to_the_child_partition(MsgId, State),
      {reply, Reply, NewState}
  end;

%% The child/no-child-consumer
handle_call({negative_acknowledge, #messageId{topic = TopicStr} = MsgId}, _From, #state{topic = Topic} = State) ->
  case TopicStr == topic_utils:to_string(Topic) of
    false ->
      {reply, ?ERROR_CONSUMER_ID_NOT_KNOWN_HERE, State};
    _ ->
      {reply, ok, handle_negative_acknowledgement(MsgId, State)}
  end;


handle_call(receive_message, _From,
    #state{partition_count = PartitionCount} = State) when PartitionCount > 0 ->
  %% This consumer is the partition parent.
  %% Redirect the client to all the consumers.
  {Replay, NextState} =
    case get_all_partition_consumers(?UNDEF, State) of
      {[], NewState} ->
        {?ERROR_CONSUMER_NOT_READY, NewState};
      {Pids, NewState} ->
        {{redirect, Pids}, NewState}
    end,
  {reply, Replay, NextState};
handle_call(receive_message, _From, #state{} = State) ->
  {Reply, NewState} = handle_receive_message(State),
  {reply, Reply, NewState};


handle_call({seek, Target}, _From, #state{partition_count = Pc} = State) ->
  Res =
    if Pc == 0 ->
      Command = commands:new_seek(State#state.consumer_id, Target),
      pulserl_conn:send_simple_command(State#state.connection, Command);
      true ->
        foreach_child(
          fun(Pid) ->
            seek(Pid, Target)
          end, State)
    end,
  {reply, Res, State};

handle_call({send_to_dead_letter_topic, Message}, _From, State) ->
  {Reply, State2} = do_send_to_dead_letter_topic(Message, State),
  {reply, Reply, State2};

handle_call(_Request, _From, State) ->
  {reply, ok, State}.

handle_cast({close, AttemptRestart}, #state{partition_count = PartitionCount} = State) ->
  if AttemptRestart ->
    NewState =
      if PartitionCount > 0 ->
        error_logger:info_msg("Restarting ~p partitioned consumers of ~p",
          [PartitionCount, self()]),
        close_children(State);
        true ->
          error_logger:info_msg("Restarting consumer(~p) with subscription [~p]",
            [self(), State#state.subscription]),
          try_reinitialize(State)
      end,
    {noreply, NewState};
    true ->
      error_logger:info_msg("Consumer(~p) with subscription [~p] is permanelty closing",
        [self(), State#state.subscription]),
      {close, normal, close_children(State)}
  end;

handle_cast(Request, State) ->
  error_logger:warning_msg("Unexpected Cast: ~p", [Request]),
  {noreply, State}.


handle_info({new_message, MsgId, RedeliveryCount, HeadersAndPayload},
    #state{topic = Topic} = State) ->
  MessageId = pulserl_utils:new_message_id(Topic, MsgId),
  case commands:parse_metadata(HeadersAndPayload) of
    {error, _} = Error ->
      {noreply, handle_message_error(MessageId, Error, State)};
    {Metadata, Payload} ->
      {MetadataAndMessages, NewState} =
        case commands:has_messages_in_batch(Metadata) of
          false ->
            {[{Metadata, pulserl_utils:new_message(Topic, MessageId, Metadata, Payload, RedeliveryCount)}], State};
          _ ->
            SingleMetaAndPayloads = payload_to_messages(Metadata, Payload),
            BatchSize = length(SingleMetaAndPayloads),
            LastBatchIndex = BatchSize - 1,
            {SingleMetaAndMessages, _} = lists:foldr(
              %% Fold from the right to make sure we don't change the list order.
              %% Also, the index assignment starts from the end, `BatchSize - 1`
              fun({SingleMetadata, ActualPayload}, {MessageList, BatchIndex}) ->
                BatchMessageId = pulserl_utils:new_message_id(Topic, MsgId, BatchIndex, BatchSize),
                {
                  [{
                    SingleMetadata, pulserl_utils:new_message(Topic, BatchMessageId, Metadata,
                      SingleMetadata, ActualPayload, RedeliveryCount)
                  } | MessageList],
                  BatchIndex - 1
                }
              end,
              {[], LastBatchIndex}, SingleMetaAndPayloads),
            [{_, #message{id = IdOfFirstMessage}} | _] = SingleMetaAndMessages,
            NewTrackerKey = message_id_2_batch_ack_tracker_key(IdOfFirstMessage),
            BatchTracker = sets:from_list(lists:seq(0, LastBatchIndex)),
            NewBatchAckTrackers = maps:put(NewTrackerKey, BatchTracker, State#state.batch_ack_trackers),
            {SingleMetaAndMessages, State#state{batch_ack_trackers = NewBatchAckTrackers}}
        end,
      {noreply, handle_messages(MetadataAndMessages, NewState)}
  end;

%% Our connection is down. We stop all scheduled
%% operations (re-initialization, acknowledgments)
%% and try again after a `connection_up` message
handle_info(connection_down, State) ->
  {noreply, cancel_all_timers(State)};

%% Starts the schedulers again
handle_info(connection_up, State) ->
  State2 = try_reinitialize(State),
  State3 = start_acknowledgments_send_timer(State2),
  {noreply, State3};

%% Last re-initialization failed. Try again!!
handle_info(try_reinitialize, State) ->
  {noreply, try_reinitialize(State)};

handle_info(redeliver_neg_ack_messages, State) ->
  {noreply, trigger_redelivery_of_neg_acknowledgements(State)};

handle_info(?ACKNOWLEDGMENT_TIMEOUT, State) ->
  State2 = send_message_to_dead_letter_topic(State),
  {noreply, start_acknowledgment_timeout_timer(State2)};

handle_info(?SEND_ACKNOWLEDGMENTS, State) ->
  NewState =
    case do_send_pending_acknowledgements(State) of
      #state{} = State2 ->
        State2;
      _ ->
        State
    end,
  {noreply, start_acknowledgments_send_timer(NewState)};

handle_info({'EXIT', Pid, Reason}, State) ->
  case Reason of
    normal -> {noreply, State};
    _ ->
      case maybe_inner_consumer_exited(Pid, Reason, State) of
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
  notify_instance_provider_of_state(State, consumer_down),
  ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

cancel_all_timers(#state{} = State) ->
  State#state{
    re_init_timer = do_cancel_timer(State#state.re_init_timer),
    acknowledgment_timeout_timer = do_cancel_timer(State#state.acknowledgment_timeout_timer),
    acknowledgments_send_timer = do_cancel_timer(State#state.acknowledgments_send_timer),
    neg_acknowledgment_redelivery_timer = do_cancel_timer(State#state.neg_acknowledgment_redelivery_timer)
  }.

do_cancel_timer(?UNDEF) ->
  ?UNDEF;
do_cancel_timer(TimeRef) ->
  erlang:cancel_timer(TimeRef).


start_acknowledgments_send_timer(#state{acknowledgments_send_tick = 0} = State) ->
  State;
start_acknowledgments_send_timer(#state{acknowledgments_send_tick = Interval} = State) ->
  Timer = erlang:send_after(Interval, self(), ?SEND_ACKNOWLEDGMENTS),
  State#state{acknowledgments_send_timer = Timer}.

start_acknowledgment_timeout_timer(#state{acknowledgment_timeout = 0} = State) ->
  State;
start_acknowledgment_timeout_timer(#state{acknowledgment_timeout = Interval} = State) ->
  Timer = erlang:send_after(Interval, self(), ?ACKNOWLEDGMENT_TIMEOUT),
  State#state{acknowledgment_timeout_timer = Timer}.


redirect_to_the_child_partition(#messageId{partition = Partition}, State) ->
  case choose_partition_consumer(Partition, State) of
    {ok, Pid} ->
      {{redirect, Pid}, State};
    _ ->
      {?ERROR_CONSUMER_NOT_READY, State}
  end.

handle_negative_acknowledgement(#messageId{} = MsgId,
    #state{
      neg_acknowledgment_delay = NegAckDelay,
      neg_acknowledgment_redelivery_timer = NegAckTimer,
      neg_acknowledgment_messages = NegAckMessages} = State) ->
  NegAckMsg = {erlwater_time:milliseconds() + NegAckDelay, MsgId},
  NewState0 = State#state{neg_acknowledgment_messages = [NegAckMsg | NegAckMessages]},
  NewState1 = untrack_message(MsgId, false, NewState0),
  if NegAckTimer == ?UNDEF ->
    set_new_unack_redelivery_timer(NewState1);
    true ->
      NewState1
  end.

set_new_unack_redelivery_timer(#state{
  neg_acknowledgment_redelivery_delay = NegAckInterval} = State) ->
  TimerRef = erlang:send_after(NegAckInterval, self(), redeliver_neg_ack_messages),
  State#state{neg_acknowledgment_redelivery_timer = TimerRef}.


trigger_redelivery_of_neg_acknowledgements(State) ->
  State2 = send_message_to_dead_letter_topic(State),
  trigger_redelivery_of_neg_ack_messages1(State2).

trigger_redelivery_of_neg_ack_messages1(State) ->
  NowMillis = erlwater_time:milliseconds(),
  {MessagesToRedeliver, RestOfNegAcknowledgments} = lists:partition(
    fun({WakeUpTime, _MsgId}) ->
      WakeUpTime =< NowMillis
    end, State#state.neg_acknowledgment_messages),
  NewState = State#state{neg_acknowledgment_messages = RestOfNegAcknowledgments},
  NewState2 = redeliver_un_ack_messages([MsgId || {_, MsgId} <- MessagesToRedeliver], NewState),
  if RestOfNegAcknowledgments /= [] ->
    set_new_unack_redelivery_timer(NewState2);
    true ->
      %% Close the timer; a new timer will be set on a neg_ack
      NewState2#state{neg_acknowledgment_redelivery_timer = ?UNDEF}
  end.


redeliver_un_ack_messages([], State) ->
  State;
redeliver_un_ack_messages(MessageIds,
    #state{connection = Cnx, consumer_id = ConsumerId} = State) ->
  Command =
    case State#state.subscription_type of
      ?SHARED_SUBSCRIPTION ->
        commands:new_redeliver_un_ack_messages(ConsumerId, MessageIds);
      ?KEY_SHARED_SUBSCRIPTION ->
        commands:new_redeliver_un_ack_messages(ConsumerId, MessageIds);
      _ ->
        commands:new_redeliver_un_ack_messages(ConsumerId, [])
    end,
  case pulserl_conn:send_simple_command(Cnx, Command) of
    {error, _} = Error ->
      error_logger:error_msg("Error: ~p on sending redeliver "
      "messages command", [Error]);
    _ ->
      ok
  end,
  State.

handle_acknowledgement(MsgId, Cumulative, State) ->
  if MsgId#messageId.batch /= ?UNDEF ->
    handle_batch_acknowledgement(MsgId, Cumulative, State);
    true ->
      handle_simple_acknowledgement(MsgId, Cumulative, State)
  end.

handle_simple_acknowledgement(#messageId{} = MsgId, Cumulative, State) ->
  NewState = untrack_message(MsgId, Cumulative, State),
  {ok, send_acknowledgment(MsgId, Cumulative, NewState)}.

handle_batch_acknowledgement(
    #messageId{batch = #batch{index = Index}} = MsgId,
    Cumulative, State) ->
  TrackerKey = message_id_2_batch_ack_tracker_key(MsgId),
  case maps:find(TrackerKey, State#state.batch_ack_trackers) of
    {ok, Tracker} ->
      NewIndicesTracker = sets:del_element(Index, Tracker),
      %% Messages in a batch has the same `messageId`; what makes them different
      %% is their batch indices. We've to make sure the batch `messageId` is
      %% acknowledge only when one of the following holds
      %%    1 => after the different indices has been acknowledged.
      %%    2 => the message is cumulatively ack
      case (sets:is_empty(NewIndicesTracker) orelse Cumulative) of
        true ->
          %% All the batch `messageId` indices has been requested for ack or we've a cumulative ack.
          %% Now we commit the acknowledgment of their shared specific `messageId`.
          State2 = send_acknowledgment(MsgId, Cumulative, State),
          State3 = untrack_message(MsgId, Cumulative, State2),
          {ok, remove_from_batch_tracker(MsgId, State3)};
        false ->
          State2 = untrack_message(MsgId, Cumulative, State),
          %% The batch is not empty or the acknowledgement is not cumulative,
          NewBatchTracker = maps:put(TrackerKey, NewIndicesTracker, State2#state.batch_ack_trackers),
          {ok, State2#state{batch_ack_trackers = NewBatchTracker}}
      end;
    _ ->
      %% If we don't see the tracker, it means its has been
      %% deleted after a cumulative/(or last index) acknowledgement
      error_logger:warning_msg("The batch that the message id: ~p "
      "belongs to has already been acknowledged", [MsgId]),
      {ok, untrack_message(MsgId, Cumulative, State)}
  end.


send_acknowledgment(MsgId, true, State) ->
  %% Don't delay cumulative ack. Send it now
  do_send_ack_now(MsgId, true, State);
send_acknowledgment(MsgId, Cumulative,
    #state{acknowledgments_send_tick = 0} = State) ->
  %% No ack timer set. Send it now
  do_send_ack_now(MsgId, Cumulative, State);
send_acknowledgment(MsgId, Cumulative,
    #state{max_pending_acknowledgments = 0} = State) ->
  %% No max pending ack count was set. Send it now
  do_send_ack_now(MsgId, Cumulative, State);

%% Ack timer/max pending is set
send_acknowledgment(MsgId, _Cumulative,
    #state{
      pending_acknowledgments = PendingAcknowledgments,
      max_pending_acknowledgments = MaxPendingAcknowledgments} = State) ->
  PendingAcknowledgments2 = gb_sets:add(MsgId, PendingAcknowledgments),
  NewState = State#state{pending_acknowledgments = PendingAcknowledgments2},
  case gb_sets:size(PendingAcknowledgments2) >= MaxPendingAcknowledgments of
    true ->
      do_send_pending_acknowledgements(NewState);
    _ ->
      NewState
  end.

do_send_ack_now(#messageId{} = MsgId, Cumulative,
    #state{consumer_id = ConsumerId} = State) ->
  NewState =
    case State#state.dead_letter_topic_messages of
      ?UNDEF -> State;
      Map ->
        State#state{dead_letter_topic_messages = maps:remove(MsgId, Map)}
    end,
  AckSendCommand = commands:new_ack(ConsumerId, MsgId, Cumulative),
  send_actual_command(AckSendCommand, [MsgId], NewState).

do_send_pending_acknowledgements(
    #state{
      consumer_id = ConsumerId,
      pending_acknowledgments = PendingAcknowledgments} = State) ->
  case gb_sets:is_empty(PendingAcknowledgments) of
    true ->
      State;
    _ ->
      MessageIds = gb_sets:to_list(PendingAcknowledgments),
      AckSendCommand = commands:new_ack(ConsumerId, MessageIds),
      NewState = send_actual_command(AckSendCommand, MessageIds, State),
      NewState#state{pending_acknowledgments = gb_sets:new()}
  end.

send_actual_command(#'CommandAck'{} = Command, MessageIds,
    #state{connection = Cnx} = State) when is_list(MessageIds) ->
  case pulserl_conn:send_simple_command(Cnx, Command) of
    {error, _} = Error ->
      lists:foldl(
        fun(MsgId, State0) ->
          on_message_ack_fail(Error, MsgId, State0)
        end, State, MessageIds);
    _ ->
      State
  end.

on_message_ack_fail(_Error, #messageId{}, State) ->
  State.

message_id_2_batch_ack_tracker_key(#messageId{ledger_id = LedgerId, entry_id = EntryId}) ->
  {LedgerId, EntryId}.

handle_receive_message(#state{message_queue = MessageQueue} = State) ->
  case queue:out(MessageQueue) of
    {{value, Message}, NewMessageQueue} ->
      NewState = State#state{
        last_dequeued_message_id =
        Message#message.id,
        message_queue = NewMessageQueue},
      NewState1 = increment_flow_permits(NewState),
      NewState2 = track_message(Message#message.id, NewState1),
      {{ok, Message}, NewState2};
    {empty, MessageQueue} ->
      {{ok, false}, State}
  end.


handle_messages(MetadataAndMessages, State) ->
  {NewMessageQueue, DeadLetterMsgMap} = lists:foldl(
    fun(MetadataAndMessage, {MsgQueue, DeadLetterMsgMap}) ->
      MsgQueue1 = add_to_received_message_queue(MetadataAndMessage, MsgQueue, State),
      DeadLetterMsgMap1 = add_to_dead_letter_message_map(MetadataAndMessage, DeadLetterMsgMap, State),
      {MsgQueue1, DeadLetterMsgMap1}
    end, {State#state.message_queue, State#state.dead_letter_topic_messages}, MetadataAndMessages),
  State#state{message_queue = NewMessageQueue, dead_letter_topic_messages = DeadLetterMsgMap}.


%% @Todo Remove `compacted_out` messages
add_to_received_message_queue({_, Message}, MessageQueue, State) ->
  #'messageMeta'{redelivery_count = RedeliveryCount} = Message#'message'.metadata,
  if RedeliveryCount > State#state.dead_letter_topic_max_redeliver_count ->
    MessageQueue;
    true ->
      queue:in(Message, MessageQueue)
  end.

%% Dead Letter Policy not enable
add_to_dead_letter_message_map(_MetadataAndMessage, ?UNDEF, _State) ->
  ?UNDEF;
add_to_dead_letter_message_map({_, #'message'{id = MsgId, metadata = #'messageMeta'{
  redelivery_count = RedeliveryCount}} = Msg}, DeadLetterMsgMap,
    State) ->
  if RedeliveryCount > State#state.dead_letter_topic_max_redeliver_count ->
    Messages = [Msg | maps:get(MsgId, DeadLetterMsgMap, [])],
    maps:put(MsgId, Messages, DeadLetterMsgMap);
    true ->
      DeadLetterMsgMap
  end.

send_message_to_dead_letter_topic(#state{dead_letter_topic_messages = ?UNDEF} = State) ->
  State;
send_message_to_dead_letter_topic(#state{subscription_type = SubType} = State)
  when SubType /= ?SHARED_SUBSCRIPTION ->
  State;
send_message_to_dead_letter_topic(State) ->
  MessageCount = maps:size(State#state.dead_letter_topic_messages),
  if MessageCount > 0 ->
    send_message_to_dead_letter_topic1(State);
    true ->
      State
  end.

send_message_to_dead_letter_topic1(State) ->
  maps:fold(
    fun(_MsgId, Messages, State2) ->
      send_message_to_dead_letter_topic2(Messages, State2)
    end, State, State#state.dead_letter_topic_messages).

send_message_to_dead_letter_topic2(Messages, State) when is_list(Messages) ->
  lists:foldl(fun send_message_to_dead_letter_topic3/2, State, Messages).

send_message_to_dead_letter_topic3(Message,
    #state{parent_consumer = ParentConsumer} = State) ->
  if is_pid(ParentConsumer) ->
    case send_message_to_dead_letter(ParentConsumer, Message) of
      ok ->
        ack_and_clean_dead_letter_message(Message, State);
      _ ->
        State
    end;
    true ->
      {_, State2} = do_send_to_dead_letter_topic(Message, State),
      State2
  end.

do_send_to_dead_letter_topic(Message,
    #state{
      dead_letter_topic_name = Topic,
      dead_letter_topic_producer = ProducerPid} = State) ->
  %% Child/Non-Partitioned consumer
  if is_pid(ProducerPid) ->
    do_send_to_dead_letter_topic1(Message, State);
    true ->
      case pulserl_producer:create(Topic, []) of
        {ok, Pid} ->
          State2 = State#state{dead_letter_topic_producer = Pid},
          do_send_to_dead_letter_topic1(Message, State2);
        {error, Reason} ->
          error_logger:error_msg("Error creating dead letter topic: ~p "
          "from consumer: ~p. Reason: ~p", [Topic, self(), Reason]),
          State
      end
  end.

do_send_to_dead_letter_topic1(Message, State) ->
  Topic = State#state.dead_letter_topic_name,
  ProducerPid = State#state.dead_letter_topic_producer,
  #message{key = Key, value = Value, properties = Props} = Message,
  ProdMessage = pulserl_producer:new_message(Key, Value, Props),
  case pulserl_producer:sync_produce(ProducerPid, ProdMessage) of
    {error, Reason} = Result ->
      Topic = State#state.dead_letter_topic_name,
      error_logger:error_msg("Error sending to dead letter topic: ~p "
      "from consumer: ~p. Reason: ~p", [Topic, self(), Reason]),
      Result;
    _ ->
      {ok, ack_and_clean_dead_letter_message(Message, State)}
  end.


ack_and_clean_dead_letter_message(_,
    #state{connection = ?UNDEF} = State) ->
  State;
ack_and_clean_dead_letter_message(#message{id = MsgId}, State) ->
  State2 = untrack_message(MsgId, false, State),
  State3 = remove_from_batch_tracker(MsgId, State2),
  do_send_ack_now(MsgId, false, State3).


track_message(#messageId{} = MsgId, State) ->
  NewUnAckMgs = sets:add_element(MsgId, State#state.un_ack_message_ids),
  State#state{un_ack_message_ids = NewUnAckMgs}.

remove_from_batch_tracker(MsgId, State) ->
  TrackerKey = message_id_2_batch_ack_tracker_key(MsgId),
  NewBatchTrackers = maps:remove(TrackerKey, State#state.batch_ack_trackers),
  State#state{batch_ack_trackers = NewBatchTrackers}.

untrack_message(#messageId{} = MsgId, false, State) ->
  NewUnAcknowledgeMsgIds = sets:del_element(MsgId, State#state.un_ack_message_ids),
  State#state{un_ack_message_ids = NewUnAcknowledgeMsgIds};

untrack_message(#messageId{} = MsgId, _Cumulative, State) ->
  %% Remove all message ids up to the specified message id
  UnAcknowledgeMsgIds = sets:filter(
    fun(MsgId0) ->
      MsgId0 > MsgId
    end, State#state.un_ack_message_ids),
  State#state{un_ack_message_ids = UnAcknowledgeMsgIds}.

handle_message_error(MessageId, Error, State) ->
  error_logger:error_msg("Consumer message: ~p error: ~p",
    [MessageId, Error]),
  State.

payload_to_messages(Metadata, Data) ->
  payload_to_messages(Metadata, Data, []).

payload_to_messages(_Metadata, <<>>, Acc) ->
  lists:reverse(Acc);

payload_to_messages(Metadata, Data, Acc) ->
  {SingleMetadataSize, Data2} = commands:read_size(Data),
  <<SingleMetadataBytes:SingleMetadataSize/binary, RestOfData/binary>> = Data2,
  SingleMetadata = pulsar_api:decode_msg(SingleMetadataBytes, 'SingleMessageMetadata'),
  {PayloadData, RestOfData2} = read_payload_data(SingleMetadata, RestOfData),
  payload_to_messages(Metadata, RestOfData2, [{SingleMetadata, PayloadData} | Acc]).


read_payload_data(#'SingleMessageMetadata'{payload_size = PayloadSize}, RestOfData) ->
  <<Payload:PayloadSize/binary, Rest/binary>> = RestOfData,
  {Payload, Rest}.


maybe_inner_consumer_exited(ExitedPid, Reason, State) ->
  case dict:find(ExitedPid, State#state.child_to_partition) of
    {ok, Partition} ->
      error_logger:warning_msg("Consumer(~p) to '~s' exited abnormally due to reason."
      " '~p'. Restarting...", [ExitedPid, topic_utils:new_partition_str(
        State#state.topic, Partition), Reason]),
      State2 = State#state{
        partition_to_child = dict:erase(Partition, State#state.partition_to_child),
        child_to_partition = dict:erase(ExitedPid, State#state.child_to_partition)
      },
      case create_inner_consumer(Partition, State2) of
        {_NewPid, #state{} = NewState} ->
          error_logger:info_msg("Consumer to '~s' restarted.",
            [topic_utils:new_partition_str(State#state.topic, Partition)]),
          NewState;
        {error, NewReason} = Error ->
          error_logger:error_msg("Consumer to '~s' restart failed. Reason: ~p",
            [topic_utils:new_partition_str(State#state.topic, Partition), NewReason]),
          Error
      end;
    error ->
      %% We're told to exit by our parent
      {error, Reason}
  end.


try_reinitialize(#state{state = ?STATE_READY} = State) ->
  State;
try_reinitialize(State) ->
  case initialize(State) of
    {error, Reason} ->
      error_logger:error_msg("Re-initialization failed: ~p", [Reason]),
      State#state{re_init_timer = erlang:send_after(500, self(), try_reinitialize)};
    NewState ->
      NewState
  end.

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
    #state{} = NewState ->
      NewState#state{state = ?STATE_READY};
    _ -> Result
  end.

initialize_self(#state{topic = Topic} = State) ->
  case pulserl_client:get_broker_address(Topic) of
    LogicalAddress when is_list(LogicalAddress) ->
      case pulserl_client:get_broker_connection(LogicalAddress) of
        {ok, Pid} ->
          Id = generate_consumer_id(State, Pid),
          case subscribe_to_topic(State#state{
            connection = Pid, consumer_id = Id
          }) of
            #state{} = NewState ->
              send_flow_permits(NewState#state{
                init_count = State#state.init_count + 1,
                flow_permits = 0});
            {error, _} = Error ->
              Error
          end;
        {error, _} = Error ->
          Error
      end;
    {error, _} = Error ->
      Error
  end.


subscribe_to_topic(State) ->
  Subscribe = #'CommandSubscribe'{
    consumer_id = State#state.consumer_id,
    subscription = State#state.subscription,
    consumer_name = State#state.consumer_name,
    topic = topic_utils:to_string(State#state.topic),
    subType = to_pulsar_subType(State#state.subscription_type),
    initialPosition = to_pulsar_initial_pos(State#state.initial_position)
  },
  case pulserl_conn:send_simple_command(
    State#state.connection, Subscribe
  ) of
    {error, _} = Err ->
      Err;
    #'CommandSuccess'{} ->
      error_logger:info_msg("Consumer: ~p with subscription [~s] subscribed to topic: ~s",
        [self(), State#state.subscription, topic_utils:to_string(State#state.topic)]),
      State
  end.

send_flow_permits(#state{queue_size = QueueSize} = State) ->
  increase_flow_permits(State, QueueSize).

increment_flow_permits(State) ->
  increase_flow_permits(State, 1).

increase_flow_permits(State, 0) ->
  State;
increase_flow_permits(State, Increment) ->
  NewPermits = State#state.flow_permits + Increment,
  if NewPermits >= State#state.queue_refill_threshold ->
    case send_flow_permits(State, NewPermits) of
      {error, _} = Error ->
        error_logger:error_msg("Error: ~p on sending flow permits:"
        " [~p]) in consumer: [~p] as: ~s",
          [Error, NewPermits, self(), State#state.subscription]),
        State;
      _ ->
        %% Reset the permits
        State#state{flow_permits = 0}
    end;
    true ->
      State#state{flow_permits = NewPermits}
  end.

send_flow_permits(State, NumberOfMessages) ->
  Permit = #'CommandFlow'{
    consumer_id = State#state.consumer_id,
    messagePermits = NumberOfMessages
  },
  case pulserl_conn:send_simple_command(State#state.connection, Permit) of
    {error, _} = Error ->
      Error;
    _ -> State
  end.


initialize_children(#state{partition_count = Total} = State) ->
  {NewState, Err} = lists:foldl(
    fun(Index, {S, Error}) ->
      if Error == ?UNDEF ->
        case create_inner_consumer(Index, S) of
          {_, #state{} = S2} ->
            {S2, Error};
          Error0 ->
            {S, Error0}
        end;
        true ->
          {S, Error}
      end
    end, {State, ?UNDEF}, lists:seq(0, Total - 1)),
  case Err of
    {error, _} ->
      [pulserl_consumer:close(Pid, false) || {_, Pid} <- dict:fetch_keys(NewState#state.child_to_partition)],
      Err;
    ?UNDEF ->
      NewState
  end.


create_inner_consumer(Index, State) ->
  create_inner_consumer(3, Index, State).


create_inner_consumer(Retries, Index,
    #state{topic = Topic, options = Opts} = State) ->
  PartitionedTopic = topic_utils:new_partition(Topic, Index),
  case pulserl_consumer:start_link(PartitionedTopic, [{parent_consumer, self()} | Opts]) of
    {ok, Pid} ->
      {Pid, State#state{
        partition_to_child = dict:store(Index, Pid, State#state.partition_to_child),
        child_to_partition = dict:store(Pid, Index, State#state.child_to_partition)
      }};
    {error, _} = Error ->
      case Retries > 0 of
        true -> create_inner_consumer(Retries - 1, Index, State);
        _ -> Error
      end
  end.

close_children(State) ->
  foreach_child(
    fun(Pid) ->
      pulserl_consumer:close(Pid, false)
    end, State),
  State#state{
    child_to_partition = dict:new(),
    partition_to_child = dict:new()
  }.

foreach_child(Fun, State) when is_function(Fun) ->
  lists:foreach(Fun, dict:fetch_keys(State#state.child_to_partition)).


notify_instance_provider_of_state(
    #state{topic = Topic, parent_consumer = ParentPid} = State,
    Event) ->
  if ParentPid == ?UNDEF ->
    %% Check whether this consumer is:
    %% 1  -> a non-partitioned consumer or
    %%
    %% 2  -> a parent of a some partitioned consumer.
    erlang:send(pulserl_instance_registry, {Event, self(), Topic});
    true ->
      ok
  end,
  State.

to_pulsar_subType(Type) ->
  case Type of
    ?SHARED_SUBSCRIPTION ->
      'Shared';
    ?FAILOVER_SUBSCRIPTION ->
      'Failover';
    ?EXCLUSIVE_SUBSCRIPTION ->
      'Exclusive';
    ?KEY_SHARED_SUBSCRIPTION ->
      'Key_Shared'
  end.

to_pulsar_initial_pos(Pos) ->
  case Pos of
    ?POS_LATEST ->
      'Latest';
    ?POS_EARLIEST ->
      'Earliest';
    _ -> Pos
  end.



validate_options(Options) when is_list(Options) ->
  erlwater_assertions:is_proplist(Options),
  lists:foreach(
    fun({parent_consumer, _} = Opt) ->
      Opt;
      ({initial_position, V} = _Opt) ->
        case V of
          ?POS_LATEST ->
            ok;
          ?POS_EARLIEST ->
            ok;
          Pos when is_integer(Pos) ->
            ok;
          _ -> error({invalid_initial_position, V}, [Options])
        end;
      ({subscription_type, V} = _Opt) ->
        case V of
          ?SHARED_SUBSCRIPTION ->
            ok;
          ?FAILOVER_SUBSCRIPTION ->
            ok;
          ?EXCLUSIVE_SUBSCRIPTION ->
            ok;
          ?KEY_SHARED_SUBSCRIPTION ->
            ok;
          _ -> error({invalid_subscription_type, V}, [Options])
        end;
      ({queue_size, _} = Opt) ->
        erlwater_assertions:is_positive_int(Opt);
      ({consumer_name, _} = Opt) ->
        erlwater_assertions:is_string(Opt);
      ({subscription_name, _V} = Opt) ->
        erlwater_assertions:is_string(Opt);
      ({acknowledgments_send_tick, _} = Opt) ->
        erlwater_assertions:is_positive_int(Opt);
      ({max_pending_acknowledgments, _} = Opt) ->
        erlwater_assertions:is_non_negative_int(Opt);
      ({queue_refill_threshold, _} = Opt) ->
        erlwater_assertions:is_non_negative_int(Opt);
      ({acknowledgment_timeout, _} = Opt) ->
        erlwater_assertions:is_non_negative_int(Opt);
      ({neg_acknowledgment_delay, _} = Opt) ->
        erlwater_assertions:is_non_negative_int(Opt);
      ({dead_letter_topic_name, _} = Opt) ->
        erlwater_assertions:is_string(Opt);
      ({dead_letter_topic_max_redeliver_count, _} = Opt) ->
        erlwater_assertions:is_non_negative_int(Opt);
      (Opt) ->
        error(unknown_consumer_options, [Opt])
    end,
    Options),
  Options.


choose_partition_consumer(Partition, State) ->
  dict:find(Partition, State#state.partition_to_child).


get_all_partition_consumers(undefined,
    #state{partition_count = PartitionCount, next_consumer_partition = NextPartition} = State) ->
  PrioritizePartition = NextPartition rem State#state.partition_count,
  OtherPartitions = lists:foldl(
    fun(I, Acc) when I /= PrioritizePartition ->
      [I | Acc];
      (_, Acc) ->
        Acc
    end, [], lists:seq(0, PartitionCount - 1)),
  Pids = lists:foldr(
    fun(Partition, Acc) ->
      case dict:find(Partition, State#state.partition_to_child) of
        {ok, Pid} ->
          [Pid | Acc];
        _ ->
          Acc
      end
    end, [], [PrioritizePartition | OtherPartitions]),
  {Pids, State#state{next_consumer_partition = NextPartition + 1}}.


generate_consumer_id(#state{consumer_id = ?UNDEF}, Pid) ->
  pulserl_conn:register_handler(Pid, self(), consumer);
generate_consumer_id(#state{consumer_id = Id}, _Pid) ->
  Id.

dead_letter_topic_name(Opts, Topic, SubscriptionName) ->
  case proplists:get_value(dead_letter_topic_name, Opts) of
    ?UNDEF -> binary_to_list(topic_utils:to_string(Topic)) ++
      "-" ++ SubscriptionName ++ "-DLQ";
    Val -> Val
  end.