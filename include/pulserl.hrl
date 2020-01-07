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

-define(PRODUCE_TIMEOUT, 5000).

-record(prod_message, {
	key :: string(),
	event_time = erlwater_time:milliseconds() :: integer(),
	properties = [] :: map(),
	value :: binary(),
	replicate = true :: binary()
}).