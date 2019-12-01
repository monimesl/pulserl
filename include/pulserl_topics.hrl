%%%-------------------------------------------------------------------
%%% @author Alpha Umaru Shaw <shawalpha5@gmail.com>
%%% @doc
%%%
%%% @end
%%% Company: Skulup Ltd
%%% Copyright: (C) 2019
%%%-------------------------------------------------------------------
-author("Alpha Umaru Shaw").

-define(PERSISTENT_DOMAIN, "persistent").
-define(NON_PERSISTENT_DOMAIN, "non-persistent").
-define(PUBLIC_TENANT, "public").
-define(DEFAULT_NAMESPACE, "default").

-record(partition_meta, {
	partitions :: integer()
}).

-record(topic, {
	domain = ?PERSISTENT_DOMAIN,
	tenant = ?PUBLIC_TENANT,
	cluster = undefined,
	namespace = ?DEFAULT_NAMESPACE,
	local,

	%%local
	parent :: #topic{}
}).
