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
  topic :: binary(),
  event_time :: integer() | ?UNDEF,
  redelivery_count = 0 :: integer(),
  properties = [] :: list()
}).

-record(message, {
  id :: #messageId{},
  metadata :: #messageMeta{},
  key :: binary() | ?UNDEF,
  value :: binary()
}).

-record(prodMessage, {
  key :: string(),
  event_time = erlwater_time:milliseconds() :: integer(),
  properties = [] :: map(),
  value :: binary(),
  deliverAtTime :: integer() | ?UNDEF
}).

-record(consumedMessage, {
  consumer :: pid(),
  message :: #message{}
}).

-record(clientConfig, {
  socket_options = [] :: list(),
  connect_timeout_ms = 15000 :: pos_integer(),
  max_connections_per_broker = 1 :: pos_integer(),
  tls_trust_certs_file :: string() | ?UNDEF
}).