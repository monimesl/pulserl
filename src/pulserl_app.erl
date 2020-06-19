%%%-------------------------------------------------------------------
%%% @author Alpha Umaru Shaw <shawalpha5@gmail.com>
%%% @doc
%%%
%%% @end
%%% Copyright: (C) 2020, Skulup Ltd
%%%-------------------------------------------------------------------
-module(pulserl_app).

-behaviour(application).

-include("pulserl.hrl").

%% Application callbacks
-export([start/2,
  stop/1, client_config/0]).

%%%===================================================================
%%% Application callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called whenever an application is started using
%% application:start/[1,2], and should start the processes of the
%% application. If the application is structured according to the OTP
%% design principles as a supervision tree, this means starting the
%% top supervisor of the tree.
%%
%% @end
%%--------------------------------------------------------------------
-spec(start(StartType :: normal | {takeover, node()} | {failover, node()},
    StartArgs :: term()) ->
  {ok, pid()} |
  {ok, pid(), State :: term()} |
  {error, Reason :: term()}).
start(_StartType, _StartArgs) ->
  ClientConfig = client_config(),
  pulserl_sup:start_link(ClientConfig).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called whenever an application has stopped. It
%% is intended to be the opposite of Module:start/2 and should do
%% any necessary cleaning up. The return value is ignored.
%%
%% @end
%%--------------------------------------------------------------------
-spec(stop(State :: term()) -> term()).
stop(_State) ->
  ok.

%%%===================================================================
%%% Internal functions
%%%===================================================================

client_config() ->
  MaxConnectionsPerBroker = pulserl_utils:get_int_env(max_connections_per_broker, 1),
  if MaxConnectionsPerBroker < 1 orelse MaxConnectionsPerBroker > 64 ->
    error("The `max_connections_per_broker` value must respect the interval (0, 64]");
    true ->
      ok
  end,
  ServiceUrl =
    case pulserl_utils:get_env(service_url, ?UNDEF) of
      ?UNDEF ->
        def_service_url();
      Val -> Val
    end,
  {TlsEnable, CaCertFile} =
    case string:str(ServiceUrl, "pulsar+ssl://") > 0 of
      true ->
        case pulserl_utils:get_env(cacertfile, ?UNDEF) of
          ?UNDEF -> error("No CA certificate file is provided");
          CaCertFile0 ->
            {true, CaCertFile0}
        end;
      _ -> {false, ?UNDEF}
    end,
  #clientConfig{
    tls_enable = TlsEnable,
    cacertfile = CaCertFile,
    service_url = ServiceUrl,
    max_connections_per_broker = MaxConnectionsPerBroker,
    enable_tcp_no_delay = pulserl_utils:get_env(enable_tcp_no_delay, true),
    enable_tcp_keep_alive = pulserl_utils:get_env(enable_tcp_keep_alive, true),
    connect_timeout_ms = pulserl_utils:get_int_env(connect_timeout_ms, 15000)
  }.


def_service_url() ->
  {ok, Hostname} = inet:gethostname(),
  "pulsar://" ++ Hostname ++ ":6650".