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
-export([start/2, stop/1]).
-export([def_consumer_options/0, def_producer_options/0]).

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
  case pulserl_sup:start_link() of
    {ok, _} = Success ->
      ServiceUrl =
        case pulserl_utils:get_env(service_url, ?UNDEF) of
          ?UNDEF ->
            def_service_url();
          Val -> Val
        end,
      case pulserl_utils:get_env(autostart, true) of
        false ->
          Success;
        _ ->
          ClientConfig = client_config(ServiceUrl),
          case pulserl:start_client(ServiceUrl, ClientConfig) of
            ok ->
              Success;
            Other ->
              Other
          end
      end;
    Other ->
      Other
  end.

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

def_producer_options() ->
  pulserl_utils:get_env(producer_opts, []).

def_consumer_options() ->
  pulserl_utils:get_env(consumer_opts, []).

%%%===================================================================
%%% Internal functions
%%%===================================================================

client_config(ServiceUrl) ->
  MaxConnectionsPerBroker = pulserl_utils:get_int_env(max_connections_per_broker, 1),
  if MaxConnectionsPerBroker < 1 orelse MaxConnectionsPerBroker > 16 ->
    error("The `max_connections_per_broker` value must respect the interval (0, 16]");
    true ->
      ok
  end,
  CaCertFile =
    case pulserl_utils:tls_enable(ServiceUrl) of
      true ->
        case pulserl_utils:get_env(tls_trust_certs_file, ?UNDEF) of
          ?UNDEF -> error("No TLS trust certificates file is provided");
          CaCertFile0 ->
            CaCertFile0
        end;
      _ -> ?UNDEF
    end,
  #clientConfig{
    tls_trust_certs_file = CaCertFile,
    max_connections_per_broker = MaxConnectionsPerBroker,
    socket_options = pulserl_utils:get_env(socket_options, []),
    connect_timeout_ms = pulserl_utils:get_int_env(connect_timeout_ms, 15000)
  }.

def_service_url() ->
  {ok, Hostname} = inet:gethostname(),
  "pulsar://" ++ Hostname ++ ":6650".