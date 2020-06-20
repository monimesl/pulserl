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
-export([ack/2, ack/3]).
-export([nack/2, redeliver_unack_messages/1]).

%% gen_server callbacks
-export([init/1,
  handle_call/3,
  handle_cast/2,
  handle_info/2,
  terminate/2,
  code_change/3]).

-define(STATE_READY, ready).

%% Initial Positions
-define(POS_LATEST, latest).
-define(POS_EARLIEST, earliest).

%% Subscription types
-define(SHARED_SUBSCRIPTION, shared).
-define(FAILOVER_SUBSCRIPTION, failover).
-define(EXCLUSIVE_SUBSCRIPTION, exclusive).
-define(KEY_SHARED_SUBSCRIPTION, key_shared).

%% Local Messages
-define(REINITIALIZE, reinitialize).
-define(RECEIVE_MESSAGE, receive_message).
-define(SEND_ACKNOWLEDGMENTS, send_acknowledgments).
-define(ACKNOWLEDGMENT_TIMEOUT, acknowledgment_timeout).
-define(REDELIVER_NACK_MESSAGES, redeliver_nack_messages).

%% Error Replies
-define(ERROR_CONSUMER_CLOSED, {error, consumer_closed}).
-define(ERROR_CONSUMER_NOT_READY, {error, consumer_not_ready}).
-define(ERROR_CONSUMER_ID_NOT_KNOWN_HERE, {error, id_not_known_here}).
-define(ERROR_CONSUMER_NOT_FROM_PARTITIONED_TOPIC, {error, id_not_from_partitioned_topic}).
-define(ERROR_CONSUMER_CUMULATIVE_ACK_INVALID, {error, <<"Cannot use cumulative ack with shared subscription type">>}).

receive_message(Pid) ->
  case gen_server:call(Pid, ?RECEIVE_MESSAGE) of
    {redirect, Children} ->
      receive_from_any(Children);
    Other ->
      Other
  end.

ack(Pid, #'message'{id = MessageId}) ->
  ack(Pid, MessageId, false);

ack(Pid, #'messageId'{} = MessageId) ->
  ack(Pid, MessageId, false).

ack(Pid, #'message'{id = MessageId}, Cumulative) ->
  ack(Pid, MessageId, Cumulative);

ack(Pid, #'messageId'{} = MessageId, Cumulative) ->
  call_associated_consumer(Pid, {acknowledge, MessageId, Cumulative}).

nack(Pid, #'message'{id = MessageId}) ->
  nack(Pid, MessageId);

nack(Pid, #messageId{} = MessageId) ->
  call_associated_consumer(Pid, {negative_acknowledge, MessageId}).

redeliver_unack_messages(Pid) ->
  call_associated_consumer(Pid, redeliver_unack_messages).

seek(Pid, Time) when is_pid(Pid) andalso is_integer(Time) ->
  gen_server:call(Pid, {seek, Time});

seek(Pid, #messageId{ledger_id = LedgerId, entry_id = EntryId}) when is_pid(Pid) ->
  seek(Pid, {LedgerId, EntryId});

seek(Pid, {LedgerId, EntryId} = MsgId) when is_pid(Pid) andalso is_integer(LedgerId) andalso is_integer(EntryId) ->
  gen_server:call(Pid, {seek, MsgId}).

call_associated_consumer(Pid, Request) ->
  case gen_server:call(Pid, Request, timer:seconds(10)) of
    {redirect, ChildConsumerPid} ->
      call_associated_consumer(ChildConsumerPid, Request);
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

receive_from_any([]) ->
  false;
receive_from_any([Child | Rest]) ->
  case receive_message(Child) of
    false ->
      receive_from_any(Rest);
    Other ->
      Other
  end.

send_message_to_dead_letter(Pid, Message) ->
  gen_server:call(Pid, {send_to_dead_letter_topic, Message}).

-record(state, {
  state,
  connection :: pid(),
  consumer_id :: integer(),
  %% start of consumer stuff
  consumer_name :: string(),
  consumer_properties :: list(),
  consumer_initial_position :: atom(),
  consumer_priority_level :: atom(),
  consumer_subscription_type :: atom(),
  consumer_subscription_name :: string(),
  %% end of consumer stuff
  partition_count = 0 :: non_neg_integer(),
  partition_to_child = #{},
  child_to_partition = #{},
  options :: list(),
  topic :: #topic{},
  %%
  parent_consumer :: pid(),
  re_init_attempts = 0, %% Number of times the re-initialization has been attempted
  re_init_timer,
  flow_permits = 0,
  queue_size = 1000,
  queue_refill_threshold = 1000,
  incoming_messages = queue:new() :: queue:queue(),
  %% Dead Letter Policy
  dead_letter_topic_producer :: pid(),
  dead_letter_topic_messages = ?UNDEF :: #{},
  dead_letter_topic_max_redeliver_count :: integer(),
  dead_letter_topic_name :: string(),
  retry_letter_topic_name :: string(),

  children_poll_sequence = 0,
  %%% For acknowledgements
  batch_ack_trackers = #{} :: #{},
  un_ack_message_ids = #{} :: #{}, %% messageId => ack_timeout_time

  acknowledgment_timeout_timer :: reference(),
  acknowledgment_timeout :: non_neg_integer(), %% [1000, ...), 0 to disable

  nack_message_redelivery_delay,  %% [100, ...)
  nack_messages_to_redeliver = [],
  nack_message_redelivery_timer :: reference(),
  nack_message_redelivery_timer_tick :: non_neg_integer(),

  acknowledgments_send_timer = ?UNDEF :: reference(),
  acknowledgments_send_timer_tick :: non_neg_integer(), %% [0, ...) 0 to disable delayed ack (ack immediately)
  max_pending_acknowledgments = 1000 :: non_neg_integer(), %% [0, ...) 0 to disable delayed ack (ack immediately)
  pending_acknowledgments = sets:new()
}).


%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([#topic{} = Topic, Opts]) ->
  process_flag(trap_exit, true),
  ConsumerOpts = proplists:get_value(consumer, Opts, []),
  ParentConsumer = proplists:get_value(parent_consumer, Opts),
  QueueSize = erlang:max(proplists:get_value(queue_size, Opts, 1000), 1),
  SubscriptionName = proplists:get_value(subscription_name, ConsumerOpts, "pulserl"),
  SubscriptionType = proplists:get_value(subscription_type, ConsumerOpts, ?SHARED_SUBSCRIPTION),
  NegAckDelay = erlang:max(proplists:get_value(nack_message_redelivery_delay, Opts, 100), 60000),
  DeadLetterMaxRedeliveryCount = erlang:max(proplists:get_value(dead_letter_topic_max_redeliver_count, Opts, 0), 0),
  State = #state{
    topic = Topic,
    options = Opts,
    queue_size = QueueSize,
    parent_consumer = ParentConsumer,
    consumer_subscription_name = SubscriptionName,
    consumer_subscription_type = SubscriptionType,
    consumer_name = proplists:get_value(name, ConsumerOpts),
    consumer_properties = proplists:get_value(properties, ConsumerOpts, []),
    consumer_priority_level = proplists:get_value(priority_level, ConsumerOpts, 0),
    consumer_initial_position = proplists:get_value(initial_position, ConsumerOpts, ?POS_LATEST),
    acknowledgments_send_timer_tick = proplists:get_value(acknowledgments_send_tick, Opts, 100),
    max_pending_acknowledgments = proplists:get_value(max_pending_acknowledgments, Opts, 1000),
    queue_refill_threshold = erlang:max(proplists:get_value(queue_refill_threshold, Opts, QueueSize div 2), 1),
    %% Acknowledgment
    acknowledgment_timeout = proplists:get_value(acknowledgment_timeout, Opts, 0),
    %% Negative Ack
    nack_message_redelivery_delay = NegAckDelay,
    nack_message_redelivery_timer_tick = NegAckDelay div 4,
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
    State2 ->
      {ok, notify_instance_provider_of_state(State2, consumer_up)}
  end.

handle_call(_, _From, #state{state = ?UNDEF} = State) ->
  %% I'm not ready yet
  {reply, ?ERROR_CONSUMER_NOT_READY, State};

handle_call({acknowledge, _, true}, _From, #state{consumer_subscription_type = ?SHARED_SUBSCRIPTION} = State) ->
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


handle_call(?RECEIVE_MESSAGE, _From,
    #state{partition_count = PartitionCount} = State) when PartitionCount > 0 ->
  %% This consumer is the partition parent.
  %% Redirect the client to all the consumers.
  {Replay, NextState} =
    case partition_consumers_to_poll(?UNDEF, State) of
      {[], NewState} ->
        {?ERROR_CONSUMER_NOT_READY, NewState};
      {Pids, NewState} ->
        {{redirect, Pids}, NewState}
    end,
  {reply, Replay, NextState};
handle_call(?RECEIVE_MESSAGE, _From, #state{} = State) ->
  {Reply, NewState} = handle_receive_message(State),
  {reply, Reply, NewState};


handle_call({seek, Target}, _From, #state{parent_consumer = Parent} = State) ->
  Res =
    if not is_pid(Parent) ->
      foreach_child(
        fun(Pid) ->
          seek(Pid, Target)
        end, State);
      true ->
        Command = commands:new_seek(State#state.consumer_id, Target),
        pulserl_conn:send_simple_command(State#state.connection, Command)
    end,
  {reply, Res, State};

handle_call(redeliver_unack_messages, _From, #state{parent_consumer = Parent} = State) ->
  {Reply, NewState} =
    if not is_pid(Parent) ->
      foreach_child(
        fun(Pid) ->
          redeliver_unack_messages(Pid)
        end, State),
      {ok, State};
      true ->
        Command = commands:new_redeliver_unack_messages(State#state.consumer_id, []),
        case pulserl_conn:send_simple_command(State#state.connection, Command) of
          {error, _} = Error ->
            {Error, State};
          _ ->
            MessageQueueLen = queue:len(State#state.incoming_messages),
            State2 = State#state{
              batch_ack_trackers = #{},
              un_ack_message_ids = #{},
              incoming_messages = queue:new(),
              pending_acknowledgments = sets:new()},
            {ok, increase_flow_permits(State2, MessageQueueLen)}
        end
    end,
  {reply, Reply, NewState};

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
            [self(), State#state.consumer_subscription_name]),
          reinitialize(State)
      end,
    {noreply, NewState};
    true ->
      error_logger:info_msg("Consumer(~p) with subscription [~p] is permanelty closing",
        [self(), State#state.consumer_subscription_name]),
      State2 = cancel_all_timers(State),
      {close, normal, close_children(State2)}
  end;

handle_cast(Request, State) ->
  error_logger:warning_msg("Unexpected Cast: ~p", [Request]),
  {noreply, State}.


handle_info({new_message, MsgId, RedeliveryCount, HeadersAndPayload},
    #state{topic = Topic} = State) ->
  MessageId = pulserl_utils:new_message_id(Topic, MsgId),
  case pulserl_io:decode_metadata(HeadersAndPayload) of
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
  {noreply, cancel_all_timers(State#state{state = ?UNDEF})};

%% Starts the schedulers again
handle_info(connection_up, State) ->
  {noreply, reinitialize(State)};

%% Last re-initialization failed. Try again!!
handle_info(?REINITIALIZE, State) ->
  {noreply, reinitialize(State)};

handle_info(?REDELIVER_NACK_MESSAGES, State) ->
  State2 = send_messages_to_dead_letter_topic(State),
  {noreply, redeliver_nack_messages(State2)};

handle_info(?ACKNOWLEDGMENT_TIMEOUT, State) ->
  State2 = send_messages_to_dead_letter_topic(State),
  {noreply, redeliver_ack_timeout_messages(State2)};

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
    nack_message_redelivery_timer = do_cancel_timer(State#state.nack_message_redelivery_timer)
  }.

do_cancel_timer(?UNDEF) ->
  ?UNDEF;
do_cancel_timer(TimeRef) ->
  erlang:cancel_timer(TimeRef).

start_nack_redelivery_timer(#state{
  nack_message_redelivery_timer_tick = Time} = State) ->
  State#state{nack_message_redelivery_timer = self_send_after(?REDELIVER_NACK_MESSAGES, Time)}.

start_acknowledgments_send_timer(#state{max_pending_acknowledgments = 0} = State) ->
  State;
start_acknowledgments_send_timer(#state{acknowledgments_send_timer_tick = 0} = State) ->
  State;
start_acknowledgments_send_timer(#state{acknowledgments_send_timer_tick = Time} = State) ->
  State#state{acknowledgments_send_timer = self_send_after(?SEND_ACKNOWLEDGMENTS, Time)}.

start_acknowledgment_timeout_timer(#state{acknowledgment_timeout = 0} = State) ->
  State;
start_acknowledgment_timeout_timer(#state{acknowledgment_timeout = Time} = State) ->
  State#state{acknowledgment_timeout_timer = self_send_after(?ACKNOWLEDGMENT_TIMEOUT, Time div 4)}.

self_send_after(Message, Time) ->
  erlang:send_after(Time, self(), Message).

redirect_to_the_child_partition(#messageId{partition = Partition}, State) ->
  case choose_partition_consumer(Partition, State) of
    {ok, Pid} ->
      {{redirect, Pid}, State};
    _ ->
      {?ERROR_CONSUMER_NOT_READY, State}
  end.

handle_negative_acknowledgement(#messageId{} = MsgId,
    #state{
      nack_message_redelivery_delay = NegAckDelay,
      nack_message_redelivery_timer = NegAckTimer,
      nack_messages_to_redeliver = NegAckMessages} = State) ->
  NegAckMsg = {erlwater_time:milliseconds() + NegAckDelay, MsgId},
  NewState0 = State#state{nack_messages_to_redeliver = [NegAckMsg | NegAckMessages]},
  NewState1 = untrack_message(MsgId, false, NewState0),
  if NegAckTimer == ?UNDEF ->
    start_nack_redelivery_timer(NewState1);
    true ->
      NewState1
  end.

redeliver_ack_timeout_messages(State) ->
  NowMillis = erlwater_time:milliseconds(),
  UnAckMessageIds = State#state.un_ack_message_ids,
  {MessagesToRedeliver2, RestOfUnAckMessageIds} = maps:fold(
    fun(MsgId, TimeoutTime, {MessagesToRedeliver, UnAckMessageIds2}) ->
      if TimeoutTime =< NowMillis ->
        {[MsgId | MessagesToRedeliver], maps:remove(MsgId, UnAckMessageIds2)};
        true ->
          {MessagesToRedeliver, UnAckMessageIds2}
      end
    end, {[], UnAckMessageIds}, UnAckMessageIds),
  State2 = State#state{un_ack_message_ids = RestOfUnAckMessageIds},
  State3 =
    if MessagesToRedeliver2 /= [] ->
      error_logger:info_msg("~p messages timed-out from consumer=[~s, ~p]",
        [length(MessagesToRedeliver2), topic_utils:to_string(State#state.topic), self()]),
      redeliver_messages(MessagesToRedeliver2, State2);
      true ->
        State2
    end,
  start_acknowledgment_timeout_timer(State3).

redeliver_nack_messages(State) ->
  NowMillis = erlwater_time:milliseconds(),
  {MessagesToRedeliver, RestOfNegAcknowledgments} = lists:partition(
    fun({WakeUpTime, _MsgId}) ->
      WakeUpTime =< NowMillis
    end, State#state.nack_messages_to_redeliver),
  NewState = State#state{nack_messages_to_redeliver = RestOfNegAcknowledgments},
  NewState2 = redeliver_messages([MsgId || {_, MsgId} <- MessagesToRedeliver], NewState),
  start_nack_redelivery_timer(NewState2).

redeliver_messages([], State) ->
  State;
redeliver_messages(MessageIds,
    #state{connection = Cnx, consumer_id = ConsumerId} = State) ->
  Command =
    case State#state.consumer_subscription_type of
      ?SHARED_SUBSCRIPTION ->
        commands:new_redeliver_unack_messages(ConsumerId, MessageIds);
      ?KEY_SHARED_SUBSCRIPTION ->
        commands:new_redeliver_unack_messages(ConsumerId, MessageIds);
      _ ->
        commands:new_redeliver_unack_messages(ConsumerId, [])
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
    #state{acknowledgment_timeout_timer = ?UNDEF} = State) ->
  %% Delayed ack is disabled. Send it now
  do_send_ack_now(MsgId, Cumulative, State);
%% Ack timer/max pending is set
send_acknowledgment(MsgId, _Cumulative,
    #state{
      pending_acknowledgments = PendingAcknowledgments,
      max_pending_acknowledgments = MaxPendingAcknowledgments} = State) ->
  PendingAcknowledgments2 = sets:add_element(MsgId, PendingAcknowledgments),
  NewState = State#state{pending_acknowledgments = PendingAcknowledgments2},
  case sets:size(PendingAcknowledgments2) >= MaxPendingAcknowledgments of
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
  case sets:is_empty(PendingAcknowledgments) of
    true ->
      State;
    _ ->
      MessageIds = sets:to_list(PendingAcknowledgments),
      AckSendCommand = commands:new_ack(ConsumerId, MessageIds),
      NewState = send_actual_command(AckSendCommand, MessageIds, State),
      NewState#state{pending_acknowledgments = sets:new()}
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

handle_receive_message(#state{incoming_messages = MessageQueue} = State) ->
  case queue:out(MessageQueue) of
    {{value, Message}, MessageQueue2} ->
      State2 = increase_flow_permits(State#state{incoming_messages = MessageQueue2}, 1),
      {Message, track_message(Message#message.id, State2)};
    {empty, _} ->
      {false, State}
  end.

handle_messages(MetadataAndMessages, State) ->
  {NewMessageQueue, DeadLetterMsgMap} = lists:foldl(
    fun(MetadataAndMessage, {MsgQueue, DeadLetterMsgMap}) ->
      MsgQueue1 = add_to_received_message_queue(MetadataAndMessage, MsgQueue, State),
      DeadLetterMsgMap1 = add_to_dead_letter_message_map(MetadataAndMessage, DeadLetterMsgMap, State),
      {MsgQueue1, DeadLetterMsgMap1}
    end, {State#state.incoming_messages, State#state.dead_letter_topic_messages}, MetadataAndMessages),
  State#state{incoming_messages = NewMessageQueue, dead_letter_topic_messages = DeadLetterMsgMap}.


%% @Todo Remove `compacted_out` messages
add_to_received_message_queue({_, Message}, MessageQueue, State) ->
  #'messageMeta'{redelivery_count = RedeliveryCount} = Message#'message'.metadata,
  MaxRedeliveryCount = State#state.dead_letter_topic_max_redeliver_count,
  if MaxRedeliveryCount > 0 andalso RedeliveryCount >= MaxRedeliveryCount ->
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
  if RedeliveryCount >= State#state.dead_letter_topic_max_redeliver_count ->
    Messages = [Msg | maps:get(MsgId, DeadLetterMsgMap, [])],
    maps:put(MsgId, Messages, DeadLetterMsgMap);
    true ->
      DeadLetterMsgMap
  end.

send_messages_to_dead_letter_topic(#state{dead_letter_topic_messages = ?UNDEF} = State) ->
  State;
send_messages_to_dead_letter_topic(#state{consumer_subscription_type = SubType} = State)
  when SubType /= ?SHARED_SUBSCRIPTION ->
  State;
send_messages_to_dead_letter_topic(State) ->
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
      error_logger:info_msg("Creating a producer to the dead letter topic=~s from "
      "consumer=[~s, ~p]", [Topic, topic_utils:to_string(State#state.topic), self()]),
      case pulserl_producer:create(Topic, []) of
        {ok, Pid} ->
          State2 = State#state{dead_letter_topic_producer = Pid},
          do_send_to_dead_letter_topic1(Message, State2);
        {error, Reason} ->
          error_logger:error_msg("Error creating dead letter topic=~s from consumer=[~s, ~p]. "
          "Reason: ~p", [Topic, topic_utils:to_string(State#state.topic), self(), Reason]),
          State
      end
  end.

do_send_to_dead_letter_topic1(Message, State) ->
  DeadLetterTopic = State#state.dead_letter_topic_name,
  ProducerPid = State#state.dead_letter_topic_producer,
  #message{id = MsgId, key = Key, value = Value, metadata = #messageMeta{properties = Props}} = Message,
  error_logger:warning_msg("Giving up processsing of message {legderId=~p, entryId=~p, redliveryCount=~p, topic=~s} "
  "by sending it to the dead letter topic=~s from consumer {topic=~s, subscription=~s, pid=~p}",
    [MsgId#messageId.ledger_id, MsgId#messageId.entry_id,
      (Message#message.metadata)#messageMeta.redelivery_count,
      MsgId#messageId.topic,
      DeadLetterTopic, topic_utils:to_string(State#state.topic),
      State#state.consumer_subscription_name, self()]),
  ProdMessage = pulserl_producer:new_message(Key, Value, Props),
  case pulserl_producer:sync_produce(ProducerPid, ProdMessage) of
    {error, Reason} = Result ->
      error_logger:error_msg("Error sending to dead letter topic=~s from consumer=[~s, ~p]. "
      "Reason: ~p", [DeadLetterTopic, topic_utils:to_string(State#state.topic), self(), Reason]),
      Result;
    _ ->
      {ok, ack_and_clean_dead_letter_message(Message, State)}
  end.


ack_and_clean_dead_letter_message(_,
    #state{state = ?UNDEF} = State) ->
  State;
ack_and_clean_dead_letter_message(_,
    #state{parent_consumer = ?UNDEF} = State) ->
  State;
ack_and_clean_dead_letter_message(#message{id = MsgId}, State) ->
  State2 = untrack_message(MsgId, false, State),
  State3 = remove_from_batch_tracker(MsgId, State2),
  do_send_ack_now(MsgId, false, State3).


track_message(#messageId{} = MsgId,
    #state{un_ack_message_ids = UnAckMgs, acknowledgment_timeout = AckTimeout} = State) ->
  TimeoutTime = erlwater_time:milliseconds() + AckTimeout,
  NewUnAckMgs = maps:put(MsgId, TimeoutTime, UnAckMgs),
  State#state{un_ack_message_ids = NewUnAckMgs}.

remove_from_batch_tracker(MsgId, State) ->
  TrackerKey = message_id_2_batch_ack_tracker_key(MsgId),
  NewBatchTrackers = maps:remove(TrackerKey, State#state.batch_ack_trackers),
  State#state{batch_ack_trackers = NewBatchTrackers}.

untrack_message(#messageId{} = MsgId, false, #state{un_ack_message_ids = UnAckMgs} = State) ->
  NewUnAckMsgIds = maps:remove(MsgId, UnAckMgs),
  State#state{un_ack_message_ids = NewUnAckMsgIds};

untrack_message(#messageId{} = MsgId, _Cumulative, State) ->
  %% Remove all message ids up to the specified message id
  UnAcknowledgeMsgIds = maps:filter(
    fun(MsgId0, _) ->
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
  {SingleMetadataSize, Rest} = pulserl_io:read_4bytes(Data),
  <<SingleMetadataBytes:SingleMetadataSize/binary, RestOfData/binary>> = Rest,
  SingleMetadata = pulsar_api:decode_msg(SingleMetadataBytes, 'SingleMessageMetadata'),
  {PayloadData, RestOfData2} = read_payload_data(SingleMetadata, RestOfData),
  payload_to_messages(Metadata, RestOfData2, [{SingleMetadata, PayloadData} | Acc]).


read_payload_data(#'SingleMessageMetadata'{payload_size = PayloadSize}, RestOfData) ->
  <<Payload:PayloadSize/binary, Rest/binary>> = RestOfData,
  {Payload, Rest}.


maybe_inner_consumer_exited(ExitedPid, Reason, State) ->
  case maps:get(ExitedPid, State#state.child_to_partition, false) of
    false ->
      %% We're told to exit by our parent
      {error, Reason};
    Partition ->
      error_logger:warning_msg("Consumer=[~s ~p] exited abnormally due to reason = '~p'. "
      "Restarting...", [topic_utils:partition_str(State#state.topic, Partition), self(), Reason]),
      State2 = State#state{
        partition_to_child = maps:remove(Partition, State#state.partition_to_child),
        child_to_partition = maps:remove(ExitedPid, State#state.child_to_partition)
      },
      case create_child_consumer(Partition, State2) of
        {_NewPid, #state{} = NewState} ->
          error_logger:info_msg("Consumer=[~s ~p] restarted.",
            [topic_utils:partition_str(State#state.topic, Partition), self()]),
          NewState;
        {error, NewReason} = Error ->
          error_logger:error_msg("Consumer=[~s ~p] restart failed. Reason: ~p",
            [topic_utils:partition_str(State#state.topic, Partition), self(), NewReason]),
          Error
      end
  end.


reinitialize(#state{state = ?STATE_READY} = State) ->
  State;
reinitialize(State) ->
  case initialize(State) of
    {error, Reason} ->
      error_logger:error_msg("Re-initialization failed: ~p", [Reason]),
      NextAttemptTime = erlang:min(100 bsl State#state.re_init_attempts, 10000),
      State#state{
        re_init_attempts = State#state.re_init_attempts + 1,
        re_init_timer = erlang:send_after(NextAttemptTime, self(), ?REINITIALIZE)};
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
            #state{} = State2 ->
              State3 = start_nack_redelivery_timer(State2),
              State4 = start_acknowledgments_send_timer(State3),
              State5 = start_acknowledgment_timeout_timer(State4),
              send_flow_permits(State5#state{flow_permits = 0});
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
  SubType = State#state.consumer_subscription_type,
  Subscribe = #'CommandSubscribe'{
    consumer_id = State#state.consumer_id,
    subType = to_pulsar_subType(SubType),
    consumer_name = State#state.consumer_name,
    topic = topic_utils:to_string(State#state.topic),
    subscription = State#state.consumer_subscription_name,
    metadata = commands:to_con_prod_metadata(State#state.consumer_properties),
    initialPosition = to_pulsar_initial_pos(State#state.consumer_initial_position),
    priority_level = (case SubType of
                        ?SHARED_SUBSCRIPTION -> State#state.consumer_priority_level;
                        ?FAILOVER_SUBSCRIPTION -> State#state.consumer_priority_level;
                        _ -> ?UNDEF
                      end)
  },
  case pulserl_conn:send_simple_command(
    State#state.connection, Subscribe
  ) of
    {error, _} = Err ->
      Err;
    #'CommandSuccess'{} ->
      error_logger:info_msg("Consumer=~p with subscription=~s subscribed to topic=~s",
        [self(), State#state.consumer_subscription_name, topic_utils:to_string(State#state.topic)]),
      State
  end.

send_flow_permits(#state{queue_size = QueueSize} = State) ->
  increase_flow_permits(State, QueueSize).

increase_flow_permits(State, 0) ->
  State;
increase_flow_permits(State, Increment) ->
  NewPermits = State#state.flow_permits + Increment,
  if NewPermits >= State#state.queue_refill_threshold ->
    case send_flow_permits(State, NewPermits) of
      {error, _} = Error ->
        error_logger:error_msg("Error: ~p on sending flow permits:"
        " [~p]) in consumer: [~p] as: ~s",
          [Error, NewPermits, self(), State#state.consumer_subscription_name]),
        State;
      _ ->
        %% Reset the permits
        State#state{flow_permits = 0}
    end;
    true ->
      State#state{flow_permits = NewPermits}
  end.

send_flow_permits(#state{connection = Cnx} = State, NumberOfMessages) ->
  Permit = #'CommandFlow'{
    consumer_id = State#state.consumer_id,
    messagePermits = NumberOfMessages
  },
  case pulserl_conn:send_simple_command(Cnx, Permit) of
    {error, _} = Error ->
      Error;
    _ -> State
  end.


initialize_children(#state{partition_count = Total} = State) ->
  {NewState, Err} = lists:foldl(
    fun(Index, {S, Error}) ->
      if Error == ?UNDEF ->
        case create_child_consumer(Index, S) of
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
      [pulserl_consumer:close(Pid, false) || {_, Pid} <- maps:keys(NewState#state.child_to_partition)],
      Err;
    ?UNDEF ->
      NewState
  end.


create_child_consumer(Index, State) ->
  create_child_consumer(3, Index, State).


create_child_consumer(Retries, Index,
    #state{topic = Topic, options = Opts} = State) ->
  PartitionedTopic = topic_utils:new_partition(Topic, Index),
  case pulserl_consumer:start_link(PartitionedTopic, [{parent_consumer, self()} | Opts]) of
    {ok, Pid} ->
      {Pid, State#state{
        partition_to_child = maps:put(Index, Pid, State#state.partition_to_child),
        child_to_partition = maps:put(Pid, Index, State#state.child_to_partition)
      }};
    {error, _} = Error ->
      case Retries > 0 of
        true -> create_child_consumer(Retries - 1, Index, State);
        _ -> Error
      end
  end.

close_children(State) ->
  foreach_child(
    fun(Pid) ->
      pulserl_consumer:close(Pid, false)
    end, State),
  State#state{
    child_to_partition = #{},
    partition_to_child = #{}
  }.

foreach_child(Fun, State) when is_function(Fun) ->
  lists:foreach(Fun, maps:keys(State#state.child_to_partition)).


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
      ({consumer, Options2} = Opt) ->
        validate_options(Options2),
        Opt;
      ({consumer_name, _} = Opt) ->
        erlwater_assertions:is_string(Opt);
      ({subscription_name, _V} = Opt) ->
        erlwater_assertions:is_string(Opt);
      ({priority_level, _V} = Opt) ->
        erlwater_assertions:is_non_negative_int(Opt);
      ({properties, _V} = Opt) ->
        erlwater_assertions:is_proplist(Opt);
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
      ({acknowledgments_send_tick, _} = Opt) ->
        erlwater_assertions:is_positive_int(Opt);
      ({max_pending_acknowledgments, _} = Opt) ->
        erlwater_assertions:is_non_negative_int(Opt);
      ({queue_refill_threshold, _} = Opt) ->
        erlwater_assertions:is_non_negative_int(Opt);
      ({acknowledgment_timeout, Val} = Opt) ->
        erlwater_assertions:assert(is_integer(Val) andalso
          (Val == 0 orelse Val >= 1000), Opt, "acknowledgment_timeout must be >= 10000");
      ({nack_message_redelivery_delay, _} = Opt) ->
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
  case maps:get(Partition, State#state.partition_to_child, false) of
    false -> false;
    Pid -> {ok, Pid}
  end.


partition_consumers_to_poll(undefined,
    #state{partition_count = PartitionCount,
      children_poll_sequence = Sequence} = State) ->
  HeadPartition = Sequence rem State#state.partition_count,
  TailPartitions = lists:foldl(
    fun(I, Acc) when I /= HeadPartition ->
      [I | Acc];
      (_, Acc) ->
        Acc
    end, [], lists:seq(0, PartitionCount - 1)),
  Pids = lists:foldr(
    fun(Partition, Acc) ->
      case maps:get(Partition, State#state.partition_to_child, false) of
        false ->
          Acc;
        Pid ->
          [Pid | Acc]
      end
    end, [], [HeadPartition | TailPartitions]),
  {Pids, State#state{children_poll_sequence = Sequence + 1}}.


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