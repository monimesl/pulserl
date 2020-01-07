%%%-------------------------------------------------------------------
%% @doc pulserl top level supervisor.
%% @end
%%%-------------------------------------------------------------------

-module(pulserl_sup).

-behaviour(supervisor).

-export([start_link/0]).

-export([init/1]).

-define(SERVER, ?MODULE).

start_link() ->
  supervisor:start_link({local, ?SERVER}, ?MODULE, []).


init([]) ->
  ServiceUrl = pulserl_utils:get_env(service_url, def_service_url()),
  application:set_env(pulserl, service_url, ServiceUrl),
  SupFlags = #{strategy => one_for_all,
    intensity => 0,
    period => 1},
  ChildSpecs = [
    #{
      id => instance_provider,
      start => {instance_provider, start_link, []},
      restart => permanent,
      shutdown => 10000,
      type => worker,
      modules => [instance_provider]
    },
    #{
      id => pulserl_conn_sup,
      start => {pulserl_conn_sup, start_link, []},
      restart => permanent,
      type => supervisor,
      modules => [pulserl_conn_sup]
    },
    #{
      id => pulserl_producer_sup,
      start => {pulserl_producer_sup, start_link, []},
      restart => permanent,
      type => supervisor,
      modules => [pulserl_producer_sup]
    },
    #{
      id => pulserl_client,
      start => {pulserl_client, start_link, [ServiceUrl]},
      restart => permanent,
      shutdown => 10000,
      type => worker,
      modules => [pulserl_client]
    }
  ],
  {ok, {SupFlags, ChildSpecs}}.

%% internal functions

def_service_url() ->
  {ok, Hostname} = inet:gethostname(),
  "pulsar://" ++ Hostname ++ ":6650".
