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
-export([singleton_consumer/2, singleton_producer/2]).


%% gen_server callbacks
-export([init/1,
  handle_call/3,
  handle_cast/2,
  handle_info/2,
  terminate/2,
  code_change/3]).

-define(SERVER, ?MODULE).

singleton_consumer(TopicName, Options) ->
  Topic = topic_utils:parse(TopicName),
  case ets:lookup(pulserl_consumers, topic_utils:to_string(Topic)) of
    [] ->
      gen_server:call(?SERVER, {new_consumer, Topic, Options}, 32000);
    Prods ->
      {_, Pid} = erlwater_collection:random_select(Prods),
      {ok, Pid}
  end.


singleton_producer(TopicName, Options) ->
  Topic = topic_utils:parse(TopicName),
  case ets:lookup(pulserl_producers, topic_utils:to_string(Topic)) of
    [] ->
      gen_server:call(?SERVER, {new_producer, Topic, Options}, 32000);
    Prods ->
      {_, Pid} = erlwater_collection:random_select(Prods),
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
      Reply = pulserl_consumer:create(Topic, Options),
      case Reply of
        {ok, _Pid} ->
          %% If the broker has some unAck/unsent messages
          %% and the consumer is created for the first time via
          %% pulserl:consumer/1, sometimes the call returns without
          %% consuming any messages. This is actually due to the fact
          %% after the new consumer has been initialized, the message
          %% load is asynchronous. Sleeping here a bit, will increases
          %% the chances of having the message(s) arrived before we return
          %% the consumer pid to the client. This is just a hack
          timer:sleep(300);
        _ ->
          ok
      end,
      {reply, Reply, State};
    Prods ->
      {_, Pid} = erlwater_collection:random_select(Prods),
      {reply, {ok, Pid}, State}
  end;

handle_call({new_producer, Topic, Options}, _From, State) ->
  case ets:lookup(pulserl_producers, topic_utils:to_string(Topic)) of
    [] ->
      {reply, pulserl_producer:create(Topic, Options), State};
    Prods ->
      {_, Pid} = erlwater_collection:random_select(Prods),
      {reply, {ok, Pid}, State}
  end;

handle_call(_Request, _From, State) ->
  {reply, ok, State}.

handle_cast(_Request, State) ->
  {noreply, State}.

handle_info({producer_up, ProducerPid, Topic}, State) ->
  ets:insert(pulserl_producers, {topic_utils:to_string(Topic), ProducerPid}),
  {noreply, State};
handle_info({producer_down, ProducerPid, Topic}, State) ->
  ets:delete_object(pulserl_producers, {topic_utils:to_string(Topic), ProducerPid}),
  {noreply, State};

handle_info({consumer_up, ProducerPid, Topic}, State) ->
  ets:insert(pulserl_consumers, {topic_utils:to_string(Topic), ProducerPid}),
  {noreply, State};
handle_info({consumer_down, ProducerPid, Topic}, State) ->
  ets:delete_object(pulserl_consumers, {topic_utils:to_string(Topic), ProducerPid}),
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
