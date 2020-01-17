%%%-------------------------------------------------------------------
%%% @author Alpha Umaru Shaw <shawalpha5@gmail.com>
%%% @doc
%%%
%%% @end
%%% Company: Skulup Ltd
%%% Copyright: (C) 2019
%%%-------------------------------------------------------------------
-author("Alpha Umaru Shaw").

-define(UNDEF, undefined).

-define(PRODUCE_TIMEOUT, 10000).

-define(SHARED_SUBSCRIPTION, shared).
-define(FAILOVER_SUBSCRIPTION, failover).
-define(EXCLUSIVE_SUBSCRIPTION, exclusive).
-define(KEY_SHARED_SUBSCRIPTION, key_shared).

-define(POS_LATEST, latest).
-define(POS_EARLIEST, earliest).

-record(prod_message, {
	key :: string(),
	event_time = erlwater_time:milliseconds() :: integer(),
	properties = [] :: map(),
	value :: binary(),
	replicate = true :: binary()
}).