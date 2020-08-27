%%%-------------------------------------------------------------------
%%% @author Alpha Umaru Shaw <shawalpha5@gmail.com>,
%%% @doc
%%%
%%% @end
%%% Company: Skulup Ltd
%%% Copyright: (C) 2020
%%%-------------------------------------------------------------------
-module(consumer_SUITE).

-author("Alpha Umaru Shaw").
-author("Stanislav Sabudaye").

-export([suite/0, init_per_suite/1, end_per_suite/1, groups/0, all/0]).
-export([receive_message_test/1, seek_test/1]).
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
  [receive_message_test, seek_test].

-spec receive_message_test(term()) -> ok.
receive_message_test(_Config) ->
  Message = <<"message">>,
  Topic = topic_utils:parse("test-topic"),
  {ok, Pid} = pulserl_consumer:create(Topic, []),
  produce_after(Topic, Message, 1),
  do_receive_message(Pid, Message),
  ok.

produce_after(Topic, Message, Seconds) ->
  spawn(fun() ->
    timer:sleep(Seconds * 1000),
    pulserl:produce(Topic, Message)
  end).

do_receive_message(Pid, Message) ->
  case pulserl_consumer:receive_message(Pid) of
    #consMessage{value = Message} = ConsumerMsg ->
      _ = pulserl:ack(ConsumerMsg);
    {error, _} = Error ->
      error(Error);
    _ ->
      do_receive_message(Pid, Message)
  end.

-spec seek_test(term()) -> ok.
seek_test(_Config) ->
  Topic = topic_utils:parse("test-topic"),
  {ok, Pid} = pulserl_consumer:create(Topic, []),
  ok = pulserl_consumer:seek(Pid, 2000),
  ok.
