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
-export([produce/2, produce/3]).
-export([sync_produce/2, sync_produce/3]).
-export([new_producer/1, new_producer/2]).


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
produce(PidOrTopic, Value) when is_binary(Value); is_list(Value) ->
  produce(PidOrTopic, Value, undefined);

produce(PidOrTopic, #prod_message{} = Msg) ->
  produce(PidOrTopic, Msg, undefined).

produce(PidOrTopic, Value, Callback) when is_binary(Value); is_list(Value) ->
  produce(PidOrTopic, undefined, Value, Callback);

produce(PidOrTopic, #prod_message{} = Msg, Callback) ->
  if is_pid(PidOrTopic) ->
    pulserl_producer:produce(PidOrTopic, Msg, Callback);
    true ->
      case instance_provider:singleton_producer(PidOrTopic, []) of
        {ok, Pid} -> produce(Pid, Msg, Callback);
        Other -> Other
      end
  end;

produce(PidOrTopic, Key, Value) when is_binary(Value); is_list(Value) ->
  produce(PidOrTopic, Key, Value, undefined).

produce(PidOrTopic, Key, Value, Callback) when is_binary(Value); is_list(Value) ->
  Key2 = case Key of undefined -> <<>>; _ -> iolist_to_binary(Key) end,
  produce(PidOrTopic, #prod_message{key = Key2, value = iolist_to_binary(Value)}, Callback).


%% @doc
%%  Synchronously produce
%% @end
sync_produce(PidOrTopic, Value) when is_binary(Value); is_list(Value) ->
  sync_produce(PidOrTopic, undefined, Value, ?PRODUCE_TIMEOUT);

sync_produce(Pid, #prod_message{} = Msg) ->
  sync_produce(Pid, Msg, ?PRODUCE_TIMEOUT).

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
  Key2 = case Key of undefined -> <<>>; _ -> iolist_to_binary(Key) end,
  sync_produce(PidOrTopic, #prod_message{key = Key2, value = iolist_to_binary(Value)}, Timeout).


%% @doc
%%  Create a producer
%% @end
new_producer(TopicName) ->
  new_producer(TopicName, []).

new_producer(TopicName, Options) ->
  Topic = topic_utils:parse(TopicName),
  pulserl_producer:create(Topic, Options).


%%%===================================================================
%%% application callbacks
%%%===================================================================

start(_StartType, _StartArgs) ->
  pulserl_sup:start_link().


stop(_State) ->
  ok.