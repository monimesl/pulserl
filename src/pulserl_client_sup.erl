%%%-------------------------------------------------------------------
%%% @author Alpha Umaru Shaw <shawalpha5@gmail.com>
%%% @doc
%%%
%%% @end
%%% Company: Skulup Ltd
%%% Copyright: (C) 2020
%%%-------------------------------------------------------------------
-module(pulserl_client_sup).
-author("Alpha Umaru Shaw").

-include("pulserl.hrl").

-behaviour(supervisor).

%% API
-export([start_link/0, start_client/2]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

start_link() ->
  supervisor:start_link({local, ?SERVER}, ?MODULE, []).

start_client(ServiceUrl, #clientConfig{} = Config) ->
  case supervisor:start_child(pulserl_client_sup, [ServiceUrl, Config]) of
    {ok, _} -> ok;
    {error, {already_started, _}} -> ok;
    Other -> Other
  end.

init([]) ->
  SupFlags = #{strategy => simple_one_for_one,
    intensity => 1000,
    period => 3600},
  ChildSpecs = [
    #{
      id => pulserl_client,
      start => {pulserl_client, start_link, []},
      restart => transient,
      shutdown => 10000,
      type => worker,
      modules => [pulserl_client]
    }
  ],
  {ok, {SupFlags, ChildSpecs}}.

