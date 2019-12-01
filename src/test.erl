%%%-------------------------------------------------------------------
%%% @author Alpha Umaru Shaw <shawalpha5@gmail.com>
%%% @doc
%%%
%%% @end
%%% Company: Skulup Ltd
%%% Copyright: (C) 2019
%%%-------------------------------------------------------------------
-module(test).
-author("Alpha Umaru Shaw").

%% API
-export([produce/1]).


produce(N) when is_integer(N) ->
	produce("Message-", N).


produce(Data, N) ->
	spawn(
		fun() ->
			lists:foreach(
				fun(I) ->
					case pulserl:produce("pkaja", integer_to_binary(erlang:system_time()), Data ++ integer_to_list(I)) of
						{error, _} = Err ->
							error_logger:info_msg("~p", [Err]);
						_ -> ok
					end
				end
				, lists:seq(1, N))
		end).
