%%%-------------------------------------------------------------------
%%% @author Alpha Umaru Shaw <shawalpha5@gmail.com>
%%% @doc
%%%
%%% @end
%%% Company: Skulup Ltd
%%% Copyright: (C) 2020
%%%-------------------------------------------------------------------
-module(producer_SUITE).
-author("Alpha Umaru Shaw").

-export([suite/0, init_per_suite/1, end_per_suite/1, groups/0, all/0]).
-export([new_message_test/1, send_test/1]).

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
  [new_message_test, send_test].

-spec new_message_test(term()) -> ok.
new_message_test(_Config) ->
  #prodMessage{value = <<"test message">>} = pulserl_producer:new_message("test message"),
  ok.

-spec send_test(term()) -> ok.
send_test(_Config) ->
  Message = pulserl_producer:new_message("test message"),
  Topic = topic_utils:parse("test-topic"),
  {ok, Pid} = pulserl_producer:create(Topic, []),
  #messageId{} = pulserl_producer:sync_send(Pid, Message, ?UNDEF),
  ok.
