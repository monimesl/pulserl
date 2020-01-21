%%%-------------------------------------------------------------------
%%% @author Alpha Umaru Shaw <shawalpha5@gmail.com>
%%% @doc
%%%
%%% @end
%%% Copyright: (C) 2020, Skulup Ltd
%%%-------------------------------------------------------------------

-define(UNDEF, undefined).

-define(PERSISTENT_DOMAIN, <<"persistent">>).
-define(NON_PERSISTENT_DOMAIN, <<"non-persistent">>).
-define(PUBLIC_TENANT, <<"public">>).
-define(DEFAULT_NAMESPACE, <<"default">>).

-record(partitionMeta, {
  partitions :: integer()
}).

-record(topic, {
  domain = ?PERSISTENT_DOMAIN,
  tenant = ?PUBLIC_TENANT,
  namespace = ?DEFAULT_NAMESPACE,
  local,

  %%local
  parent :: #topic{}
}).


-record(batch, {
  index = -1 :: integer(),
  size :: non_neg_integer()
}).

-record(messageId, {
  ledger_id :: integer(),
  entry_id :: integer(),
  topic :: integer() | ?UNDEF,
  partition = -1 :: integer(),
  batch :: #batch{} | ?UNDEF
}).

-record(messageMeta, {
  event_time :: integer() | ?UNDEF,
  delivery_time :: integer() | ?UNDEF,
  redelivery_count :: integer() | ?UNDEF
}).

-record(message, {
  id :: #messageId{},
  key :: binary() | ?UNDEF,
  value :: binary(),
  topic :: binary(),
  properties = [] :: list(),
  metadata :: #messageMeta{}
}).

-record(prodMessage, {
  key :: string(),
  event_time = erlwater_time:milliseconds() :: integer(),
  properties = [] :: map(),
  value :: binary(),
  replicate = true :: binary()
}).

-record(consumerMessage, {
  consumer :: pid(),
  message :: #message{}
}).