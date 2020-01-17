%%%-------------------------------------------------------------------
%%% @author Alpha Umaru Shaw <shawalpha5@gmail.com>
%%% @doc
%%%
%%% @end
%%% Company: Skulup Ltd
%%% Copyright: (C) 2020
%%%-------------------------------------------------------------------
-module(pulserl_consumer).
-author("Alpha Umaru Shaw").

-include("pulserl.hrl").
-include("pulserl_topics.hrl").
-include("pulsar_api.hrl").

-behaviour(gen_server).

%% API
-export([start_link/2, stop/1, create/2, send_permits/2]).

%% gen_server callbacks
-export([init/1,
  handle_call/3,
  handle_cast/2,
  handle_info/2,
  terminate/2,
  code_change/3]).



create(TopicName, Options) when is_list(TopicName) ->
  create(topic_utils:parse(TopicName), Options);

create(#topic{} = Topic, Options) ->
  Options2 = validate_options(Options),
  supervisor:start_child(pulserl_consumer_sup, [Topic, Options2]).

validate_options(Options) when is_list(Options) ->
  erlwater_assertions:is_proplist(Options),
  lists:foreach(
    fun({consumer_name, _} = Opt) ->
      erlwater_assertions:is_string(Opt);
      ({subscription_name, _V} = Opt) ->
        erlwater_assertions:is_string(Opt);
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
      (Opt) ->
        error(unknown_consumer_options, [Opt])
    end,
    Options),
  Options.


start_link(#topic{} = Topic, Options) ->
  gen_server:start_link(?MODULE, [Topic, Options], []).

send_permits(Pid, Number) ->
  gen_server:call(Pid, {send_permits, Number}).

stop(Pid) ->
  gen_server:cast(Pid, stop).


-define(SERVER, ?MODULE).

-define(STATE_READY, ready).

-record(state, {
  state,
  connection :: pid(),
  subscription :: string(),
  subscription_type :: atom(),
  consumer_id :: integer(),
  consumer_name :: string(),
  initial_position :: any(),
  partition_count :: integer(),
  partition_to_child = dict:new(),
  child_to_partition = dict:new(),
  options :: list(),
  topic :: #topic{},
  %%
  re_init_timer
}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([#topic{} = Topic, Opts]) ->
  process_flag(trap_exit, true),
  State = #state{
    topic = Topic,
    options = Opts,
    consumer_name = proplists:get_value(consumer_name, Opts, "default"),
    subscription = proplists:get_value(subscription_name, Opts, "sync_latest_handling"),
    subscription_type = proplists:get_value(subscription_type, Opts, ?SHARED_SUBSCRIPTION),
    initial_position = proplists:get_value(initial_position, Opts, ?POS_LATEST)
  },
  case initialize(State) of
    {error, Reason} ->
      {stop, Reason};
    NewState ->
      erlang:send(instance_provider, {consumer_up, self(), Topic}),
      {ok, NewState}
  end.

handle_call({send_permits, Number}, _From, State) ->
  {reply, send_flow_permits(State, Number), State};

handle_call(_Request, _From, State) ->
  {reply, ok, State}.


handle_cast(stop, State) ->
  {stop, normal, stop_children(State)};

handle_cast(Request, State) ->
  error_logger:warning_msg("Unexpected Cast: ~p", [Request]),
  {noreply, State}.



handle_info({on_command, _, #'CommandCloseConsumer'{}}, State) ->
  {noreply, try_reinitialize(State#state{state = undefined})};

%% Last reinitialization failed. Still trying..
handle_info(try_reinitialize, State) ->
  {noreply, try_reinitialize(State)};


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

terminate(_Reason, _State) ->
  ok.


code_change(_OldVsn, State, _Extra) ->
  {ok, State}.


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


try_reinitialize(State) ->
  case initialize(State) of
    {error, Reason} ->
      error_logger:error_msg("Re-initialization failed: ~p", [Reason]),
      State#state{re_init_timer = erlang:send_after(500, self(), try_reinitialize)};
    NewState ->
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
          case create_inner_consumers(State2) of
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
          Id = pulserl_conn:register_handler(Pid, self(), consumer),
          establish_consumer(State#state{
            connection = Pid, consumer_id = Id
          });
        {error, _} = Error ->
          Error
      end;
    {error, _} = Error ->
      Error
  end.


establish_consumer(State) ->
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
      NewState = State#state{state = ?STATE_READY},
      Rep = send_flow_permits(NewState, 1),
      NewState
  end.

send_flow_permits(State, NumberOfMessages) ->
  Permit = #'CommandFlow'{
    consumer_id = State#state.consumer_id,
    messagePermits = NumberOfMessages
  },
  pulserl_conn:send_simple_command(State#state.connection, Permit).


create_inner_consumers(#state{partition_count = Total} = State) ->
  {NewState, Err} = lists:foldl(
    fun(Index, {S, Error}) ->
      if Error == undefined ->
        case create_inner_consumer(Index, S) of
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
      [pulserl_consumer:stop(Pid) || {_, Pid} <- dict:fetch_keys(NewState#state.child_to_partition)],
      Err
  end.


create_inner_consumer(Index, State) ->
  create_inner_consumer(3, Index, State).


create_inner_consumer(Retries, Index,
    #state{topic = Topic, options = Opts} = State) ->
  PartitionedTopic = topic_utils:new_partition(Topic, Index),
  case pulserl_consumer:start_link(PartitionedTopic, Opts) of
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

stop_children(State) ->
  lists:foreach(
    fun(Pid) ->
      pulserl_consumer:stop(Pid)
    end, dict:fetch_keys(State#state.child_to_partition)),
  State#state{
    child_to_partition = dict:new(),
    partition_to_child = dict:new()
  }.

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