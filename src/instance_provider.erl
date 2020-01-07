%%%-------------------------------------------------------------------
%%% @author Alpha Umaru Shaw <shawalpha5@gmail.com>
%%% @doc
%%%
%%% @end
%%% Company: Skulup Ltd
%%% Copyright: (C) 2020
%%%-------------------------------------------------------------------
-module(instance_provider).
-author("Alpha Umaru Shaw").

-include("pulserl_topics.hrl").

-behaviour(gen_server).

%% API
-export([start_link/0]).
-export([new_producer/2, singleton_producer/2]).


%% gen_server callbacks
-export([init/1,
  handle_call/3,
  handle_cast/2,
  handle_info/2,
  terminate/2,
  code_change/3]).

-define(SERVER, ?MODULE).

new_producer(#topic{} = Topic, Options) ->
  pulserl_producer:create(Topic, Options);
new_producer(TopicName, Options) ->
  new_producer(topic_utils:parse(TopicName), Options).


singleton_producer(TopicName, Options) ->
  Topic = topic_utils:parse(TopicName),
  case ets:lookup(producers, topic_utils:to_string(Topic)) of
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

handle_call({new_producer, Topic, Options}, _From, State) ->
  case ets:lookup(producers, topic_utils:to_string(Topic)) of
    [] ->
      {reply, ?MODULE:new_producer(Topic, Options), State};
    Prods ->
      {_, Pid} = erlwater_collection:random_select(Prods),
      {reply, {ok, Pid}, State}
  end;

handle_call(_Request, _From, State) ->
  {reply, ok, State}.

handle_cast(_Request, State) ->
  {noreply, State}.

handle_info({producer_up, ProducerPid, Topic}, State) ->
  case topic_utils:is_partitioned(Topic) of
    false ->
      ets:insert(producers, {topic_utils:to_string(Topic), ProducerPid});
    _ ->
      ok
  end,
  {noreply, State};
handle_info({producer_down, ProducerPid, Topic}, State) ->
  ets:delete_object(producers, {topic_utils:to_string(Topic), ProducerPid}),
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
