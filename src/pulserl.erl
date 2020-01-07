%%%-------------------------------------------------------------------
%% @doc pulserl public API
%% @end
%%%-------------------------------------------------------------------

-module(pulserl).

-include("pulserl.hrl").

-behaviour(application).

-export([start/2, stop/1]).

%% API
-export([await/1, await/2]).
-export([produce/2, produce/3, produce/4]).
-export([sync_produce/2, sync_produce/3]).
-export([new_producer/1, new_producer/2]).
-export([new_producer_message/2, new_producer_message/3, new_producer_message/4]).


await(Tag) ->
  await(Tag, 10000).

await(Tag, Timeout) ->
  receive
    {Tag, Reply} ->
      Reply
  after Timeout ->
    {error, timeout}
  end.


%% @doc
%%  Asynchronously produce
%% @end
produce(PidOrTopic, #prod_message{} = Msg) ->
  produce(PidOrTopic, Msg, undefined);

produce(PidOrTopic, Value) ->
  produce(PidOrTopic, new_producer_message(Value), undefined).

produce(PidOrTopic, #prod_message{} = Msg, Callback) ->
  if is_pid(PidOrTopic) ->
    pulserl_producer:produce(PidOrTopic, Msg, Callback);
    true ->
      case instance_provider:singleton_producer(PidOrTopic, []) of
        {ok, Pid} -> produce(Pid, Msg, Callback);
        Other -> Other
      end
  end;

produce(PidOrTopic, Value, Callback) ->
  produce(PidOrTopic, new_producer_message(Value), Callback).

produce(PidOrTopic, Key, Value, Callback) ->
  produce(PidOrTopic, new_producer_message(Key, Value), Callback).


%% @doc
%%  Synchronously produce
%% @end
sync_produce(Pid, #prod_message{} = Msg) ->
  sync_produce(Pid, Msg, ?PRODUCE_TIMEOUT);

sync_produce(PidOrTopic, Value) ->
  sync_produce(PidOrTopic, new_producer_message(Value), ?PRODUCE_TIMEOUT).

sync_produce(PidOrTopic, #prod_message{} = Msg, Timeout) when
  is_integer(Timeout) orelse Timeout == undefined ->
  if is_pid(PidOrTopic) ->
    pulserl_producer:sync_produce(PidOrTopic, Msg, Timeout);
    true ->
      case instance_provider:singleton_producer(PidOrTopic, []) of
        {ok, Pid} -> sync_produce(Pid, Msg, Timeout);
        Other -> Other
      end
  end;

sync_produce(PidOrTopic, Key, Value) ->
  sync_produce(PidOrTopic, Key, Value, ?PRODUCE_TIMEOUT).

sync_produce(PidOrTopic, Key, Value, Timeout) ->
  sync_produce(PidOrTopic, new_producer_message(Key, Value), Timeout).


%% @doc
%%  Create a producer
%% @end
new_producer(TopicName) ->
  new_producer(TopicName, []).

new_producer(TopicName, Options) ->
  Topic = topic_utils:parse(TopicName),
  pulserl_producer:create(Topic, Options).


new_producer_message(Value) ->
  new_producer_message(<<>>, Value).

new_producer_message(Key, Value) ->
  new_producer_message(Key, Value, []).

new_producer_message(Key, Value, Properties) ->
  new_producer_message(Key, Value, Properties, erlwater_time:milliseconds()).

new_producer_message(Key, Value, Properties, EventTime) ->
  Key2 = case Key of undefined -> <<>>; _ -> Key end,
  #prod_message{
    key = erlwater:to_binary(Key2),
    value = erlwater:to_binary(Value),
    event_time = erlwater:to_integer(EventTime),
    properties = Properties,
    replicate = true
  }.

%%%===================================================================
%%% application callbacks
%%%===================================================================

start(_StartType, _StartArgs) ->
  pulserl_sup:start_link().


stop(_State) ->
  ok.