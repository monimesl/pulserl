-module(pulserl_SUITE).

-export([suite/0, init_per_suite/1, end_per_suite/1, groups/0, all/0]).
-export([pulserl_produce_consume_test/1]).

-include_lib("common_test/include/ct.hrl").
-include("pulserl.hrl").

% Common Test API
-spec suite() -> term().
suite() ->
  [{timetrap, {seconds, 20}}].

-spec init_per_suite(term()) -> term().
init_per_suite(Config) ->
  {ok, _} = application:ensure_all_started(pulserl),
  Config.

-spec end_per_suite(term()) -> term().
end_per_suite(_Config) ->
  application:stop(pulserl),
  ok.

-spec groups() -> term().
groups() ->
  [].

-spec all() -> term().
all() ->
  [pulserl_produce_consume_test].

-spec pulserl_produce_consume_test(term()) -> ok.
pulserl_produce_consume_test(_Config) ->
  produce_after("test-topic", <<"test message">>, 1),
  do_consume("test-topic", <<"test message">>),
  ok.

produce_after(Topic, Message, Seconds) ->
  spawn(fun() ->
    timer:sleep(Seconds * 1000),
    pulserl:produce(Topic, Message)
  end).

do_consume(Topic, Message) ->
  case pulserl:consume(Topic) of
    #consMessage{value = Message} = ConsumerMsg ->
      _ = pulserl:ack(ConsumerMsg);
    {error, _} = Error ->
      error(Error);
    _ ->
      do_consume(Topic, Message)
  end.
