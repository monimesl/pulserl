%%%-------------------------------------------------------------------
%%% @author Alpha Umaru Shaw <shawalpha5@gmail.com>
%%% @doc
%%%
%%% @end
%%% Copyright: (C) 2020, Skulup Ltd
%%%-------------------------------------------------------------------
-module(pulserl_instance_registry).

-include("pulserl.hrl").

-behaviour(gen_server).

%% API
-export([start_link/0]).
-export([get_consumer/2, get_producer/2]).


%% gen_server callbacks
-export([init/1,
  handle_call/3,
  handle_cast/2,
  handle_info/2,
  terminate/2,
  code_change/3]).

-define(SERVER, ?MODULE).

get_consumer(Topic, Options) ->
  Topic2 = topic_utils:parse(Topic),
  case ets:lookup(pulserl_consumers, topic_utils:to_string(Topic2)) of
    [] ->
      gen_server:call(?SERVER, {new_consumer, Topic2, Options}, 32000);
    Producers ->
      {_, Pid} = erlwater_collection:random_select(Producers),
      {ok, Pid}
  end.


get_producer(Topic, Options) ->
  Topic2 = topic_utils:parse(Topic),
  case ets:lookup(pulserl_producers, topic_utils:to_string(Topic2)) of
    [] ->
      gen_server:call(?SERVER, {new_producer, Topic2, Options}, 32000);
    Consumers ->
      {_, Pid} = erlwater_collection:random_select(Consumers),
      {ok, Pid}
  end.

start_link() ->
  gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).


-record(state, {}).

init([]) ->
  {ok, #state{}}.

handle_call({new_consumer, Topic, Options}, _From, State) ->
  case ets:lookup(pulserl_consumers, topic_utils:to_string(Topic)) of
    [] ->
      Reply =
        case pulserl:start_consumer(Topic, Options) of
          {ok, Pid} = Res ->
            %% Normally, this process will eventually update its topic-consumer
            %% index from the `consumer_up` message sent from the started consumer.
            %% However, if rely on that in this case, we may suffer from race condition.
            %% Hence, we update the index immediately here
            update_topic_consumer_index(Topic, Pid, true),
            %% If the broker has some unAck/unsent messages
            %% and the consumer is created for the first time via
            %% pulserl:consumer/1, sometimes the call returns without
            %% consuming any messages. This is actually due to the fact
            %% after the new consumer has been initialized, the message
            %% load is asynchronous. Sleeping here a bit, will increase
            %% the chance of having the message(s) arrived before we
            %% return the consumer pid to the client. This is just a hack :)
            timer:sleep(300),
            Res;
        Other ->
          Other
      end,
      {reply, Reply, State};
    Consumers ->
      {_, Pid} = erlwater_collection:random_select(Consumers),
      {reply, {ok, Pid}, State}
  end;

handle_call({new_producer, Topic, Options}, _From, State) ->
  case ets:lookup(pulserl_producers, topic_utils:to_string(Topic)) of
    [] ->
      Reply =
        case pulserl:start_producer(Topic, Options) of
          {ok, Pid} = Res ->
            %% Normally, this process will eventually update its topic-producer
            %% index from the `producer_up` message sent from the started consumer.
            %% However, if rely on that in this case, we may suffer from race condition.
            %% Hence, we update the index immediately here
            update_topic_producer_index(Topic, Pid, true),
            Res;
          Other ->
            Other
        end,
      {reply, Reply, State};
    Producers ->
      {_, Pid} = erlwater_collection:random_select(Producers),
      {reply, {ok, Pid}, State}
  end;

handle_call(_Request, _From, State) ->
  {reply, ok, State}.

handle_cast(_Request, State) ->
  {noreply, State}.

handle_info({producer_up, ProducerPid, Topic}, State) ->
  update_topic_producer_index(Topic, ProducerPid, true),
  {noreply, State};
handle_info({producer_down, ProducerPid, Topic}, State) ->
  update_topic_producer_index(Topic, ProducerPid, false),
  {noreply, State};

handle_info({consumer_up, ConsumerPid, Topic}, State) ->
  update_topic_consumer_index(Topic, ConsumerPid, true),
  {noreply, State};
handle_info({consumer_down, ConsumerPid, Topic}, State) ->
  update_topic_consumer_index(Topic, ConsumerPid, false),
  {noreply, State};

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

update_topic_consumer_index(Topic, ConsumerPid, Insert) ->
  Topic2 = topic_utils:to_string(Topic),
  if Insert ->
    ets:insert(pulserl_consumers, {Topic2, ConsumerPid});
    true ->
      ets:delete_object(pulserl_consumers, {Topic2, ConsumerPid})
  end.

update_topic_producer_index(Topic, ProducerPid, Insert) ->
  Topic2 = topic_utils:to_string(Topic),
  if Insert ->
    ets:insert(pulserl_producers, {Topic2, ProducerPid});
    true ->
      ets:delete_object(pulserl_producers, {Topic2, ProducerPid})
  end.