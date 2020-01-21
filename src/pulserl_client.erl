%%%-------------------------------------------------------------------
%%% @author Alpha Umaru Shaw <shawalpha5@gmail.com>
%%% @doc
%%%
%%% @end
%%% Copyright: (C) 2020, Skulup Ltd
%%%-------------------------------------------------------------------
-module(pulserl_client).

-behaviour(gen_server).

-include("pulserl.hrl").
-include("pulsar_api.hrl").

%% API
-export([start_link/1, stop/0]).
-export([get_broker_address/1, get_broker_connection/1, get_partitioned_topic_meta/1]).

%% gen_server callbacks
-export([init/1,
  handle_call/3,
  handle_cast/2,
  handle_info/2,
  terminate/2,
  code_change/3]).

-define(SERVER, ?MODULE).
-define(SERVICE_LOOKUP, service_lookup).
-define(CONNECTION_UP_EVENT, connection_up).
-define(CONNECTION_DOWN_EVENT, connection_down).


-record(cached_conn, {
  pid,
  active
}).

-record(state, {
  use_tls = false,
  operation_timeout,
  connect_timeout_ms,
  enable_tcp_no_delay,
  enable_tcp_keep_alive,
  service_urls,
  service_lookup_connection,
  max_connections_per_broker,
  cnx_lookup_current_pos = 0,
  connection_2_physical_address = #{},
  physical_address_2_connections = #{},
  physical_address_2_logical_address = #{}
}).

%%%===================================================================
%%% API
%%%===================================================================


get_broker_connection(LogicalAddress) when is_binary(LogicalAddress) ->
  get_broker_connection(binary_to_list(LogicalAddress));
get_broker_connection(LogicalAddress) when is_list(LogicalAddress) ->
  gen_server:call(?SERVER, {broker_connection, LogicalAddress}).


get_broker_address(TopicName) when is_binary(TopicName) ->
  get_broker_address(binary_to_list(TopicName));
get_broker_address(TopicName) when is_list(TopicName) ->
  Topic = topic_utils:parse(TopicName),
  get_broker_address(Topic);
get_broker_address(#topic{} = Topic) ->
  gen_server:call(?SERVER, {get_topic_broker, Topic}).


get_partitioned_topic_meta(TopicName) when is_binary(TopicName) ->
  get_partitioned_topic_meta(binary_to_list(TopicName));
get_partitioned_topic_meta(TopicName) when is_list(TopicName) ->
  Topic = topic_utils:parse(TopicName),
  get_partitioned_topic_meta(Topic);
get_partitioned_topic_meta(#topic{} = Topic) ->
  gen_server:call(?SERVER, {get_partitioned_topic_meta, Topic}).


start_link(Address) ->
  gen_server:start_link({local, ?SERVER}, ?MODULE, [Address], []).


stop() ->
  gen_server:cast(?SERVER, stop).


%%%===================================================================
%%% gen_server callbacks
%%%===================================================================


init(_args) ->
  MaxConnectionsPerBroker = pulserl_utils:get_int_env(max_connections_per_broker, 1),
  if MaxConnectionsPerBroker < 1 orelse MaxConnectionsPerBroker > 64 ->
    error("The `max_connections_per_broker` value must respect the interval (0, 64]");
    true ->
      ok
  end,
  ConnectTimeoutMs = pulserl_utils:get_int_env(connect_timeout_ms, 15000),
  OperationTimeout = pulserl_utils:get_int_env(operation_timeout, 30000),
  EnableTcpKeepAlive = pulserl_utils:get_env(enable_tcp_keep_alive, true),
  EnableTcpNoDelay = pulserl_utils:get_env(enable_tcp_no_delay, true),
  ServiceUrl = pulserl_utils:get_env(service_url, ""),
  case parse_and_resolve_service_url(ServiceUrl) of
    {error, Reason} ->
      {stop, Reason};
    Addresses ->
      {ServiceUrls, PhysicalAddrs2Cnx, PhysicalAddrs2LogicalAddrs} = lists:foldl(
        fun({Hostname, IpAddress, Port}, {Urls, PhysicalAddress2Conns0, PhysicalAddrs2LogicalAddrs0}) ->
          LogicalAddress = pulserl_utils:to_logical_address(Hostname, Port),
          {
            sets:add_element(LogicalAddress, Urls),
            maps:put({IpAddress, Port}, [], PhysicalAddress2Conns0),
            maps:put({IpAddress, Port}, LogicalAddress, PhysicalAddrs2LogicalAddrs0)
          }
        end, {sets:new(), #{}, #{}}, Addresses),
      ets:new(partition_pending_messages, [named_table, set, public,
        {read_concurrency, true}, {write_concurrency, true}]),
      State = #state{
        service_urls = sets:to_list(ServiceUrls),
        max_connections_per_broker = MaxConnectionsPerBroker,
        physical_address_2_connections = PhysicalAddrs2Cnx,
        physical_address_2_logical_address = PhysicalAddrs2LogicalAddrs,
        enable_tcp_keep_alive = EnableTcpKeepAlive,
        enable_tcp_no_delay = EnableTcpNoDelay,
        connect_timeout_ms = ConnectTimeoutMs,
        operation_timeout = OperationTimeout
      },
      LogicalAddresses = maps:values(PhysicalAddrs2LogicalAddrs),
      case get_connection_to_one_of_these_logical_addresses(LogicalAddresses, State) of
        {Pid, LogicalAddr, NewState} ->
          erlang:register(?SERVICE_LOOKUP, Pid),
          error_logger:info_msg("Service lookup connection created to: ~s", [LogicalAddr]),
          {ok, NewState#state{service_lookup_connection = Pid}};
        {error, Reason} ->
          {stop, Reason}
      end
  end.


handle_call({broker_connection, LogicalAddress}, _From, State) ->
  case get_connection_to_one_of_these_logical_addresses([LogicalAddress], State) of
    {error, Reason} ->
      {reply, {error, Reason}, State};
    {Pid, _, NewState} ->
      {reply, {ok, Pid}, NewState}
  end;
handle_call({get_topic_broker, #topic{} = Topic}, _From, State) ->
  {Response, NewState} = find_broker_address(Topic, State),
  {reply, Response, NewState};
handle_call({get_partitioned_topic_meta, #topic{} = Topic}, _From, State) ->
  {Response, NewState} = get_partitioned_topic_meta(Topic, State),
  {reply, Response, NewState};

handle_call(_Request, _From, State) ->
  {reply, ok, State}.


handle_cast(stop, State) ->
  error_logger:warning_msg("Stopping: ~p", [self()]),
  {stop, normal, State};


handle_cast(_Request, State) ->
  {noreply, State}.


handle_info({?CONNECTION_UP_EVENT, SockAddr, _Socket, CnxPid}, State) ->
  %% Connection went up. Either from a fresh start or a reconnection
  {noreply, add_broker_connection(CnxPid, SockAddr, State)};
handle_info({?CONNECTION_DOWN_EVENT, _SockAddr, _Socket, CnxPid}, State) ->
  %% Connection (may not be the process) went down.
  %% It. It may under a reconnection
  {noreply, remove_broker_connection(CnxPid, State)};

handle_info({'DOWN', _ConnMonitorRef, process, Pid, _}, State) ->
  %% The connection process is dead. Update the state
  {noreply, remove_broker_connection(Pid, State)};

handle_info(Info, State) ->
  error_logger:warning_msg("Unexpected message: ~p", [Info]),
  {noreply, State}.


terminate(_Reason, _State) ->
  ok.


code_change(_OldVsn, State, _Extra) ->
  {ok, State}.


%%%===================================================================
%%% Internal functions
%%%===================================================================

get_partitioned_topic_meta(Topic,
    #state{service_lookup_connection = SrvCnxPid} = State) ->
  TopicName = topic_utils:to_string(Topic),
  Command = commands:new_partitioned_topic_meta(TopicName),
  case pulserl_conn:send_simple_command(SrvCnxPid, Command) of
    #'CommandPartitionedTopicMetadataResponse'{
      response = 'Failed', error = Error, message = Msg} ->
      {{error, {Error, Msg}}, State};
    #'CommandPartitionedTopicMetadataResponse'{
      partitions = Partitions} ->
      {#partitionMeta{partitions = Partitions}, State};
    {error, Reason} ->
      {{error, Reason}, State}
  end.


find_broker_address(Topic,
    #state{service_lookup_connection = SrvCnxPid} = State) ->
  TopicName = topic_utils:to_string(Topic),
  Command = commands:new_lookup_topic(TopicName, false),
  discover_address(TopicName, Command, SrvCnxPid, State).


discover_address(Topic, Command, CnxPid, State) ->
  case pulserl_conn:send_simple_command(CnxPid, Command) of
    #'CommandLookupTopicResponse'{response = 'Connect',
      brokerServiceUrl = BrokerServiceUrl,
      brokerServiceUrlTls = BrokerServiceUrlTls,
      proxy_through_service_url = ProxyThroughServiceUrl} ->
      case ProxyThroughServiceUrl of
        true ->
          {erlwater_collection:random_select(State#state.service_urls), State};
        _ ->
          {chose_broker_url(BrokerServiceUrl, BrokerServiceUrlTls, State), State}
      end;
    #'CommandLookupTopicResponse'{response = 'Redirect',
      authoritative = Authoritative,
      brokerServiceUrl = BrokerServiceUrl,
      brokerServiceUrlTls = BrokerServiceUrlTls} ->
      NewLookupCommand = commands:new_lookup_topic(Topic, Authoritative),
      ServiceUrl = chose_broker_url(BrokerServiceUrl, BrokerServiceUrlTls, State),
      case get_connection_to_one_of_these_logical_addresses([ServiceUrl], State) of
        {error, Reason} ->
          {{error, Reason}, State};
        {Pid, _, NewState} ->
          discover_address(Topic, NewLookupCommand, Pid, NewState)
      end;
    #'CommandLookupTopicResponse'{response = 'Failed', error = Error, message = Msg} ->
      {{error, {Error, Msg}}, State};
    {error, Reason} ->
      {{error, Reason}, State}
  end.



get_connection_to_one_of_these_logical_addresses([LogicalAddress | Rest], State) ->
  PhysicalAddresses = pulserl_utils:logical_to_physical_addresses(LogicalAddress),
  case get_connection(PhysicalAddresses, LogicalAddress, State) of
    {error, _} = Error ->
      if Rest /= [] ->
        get_connection_to_one_of_these_logical_addresses(Rest, State);
        true ->
          Error
      end;
    Result ->
      Result
  end.


get_connection([], LogicalAddress, State) ->
  create_connection(LogicalAddress, State);

get_connection([PhysicalAddr | Rest], LogicalAddr,
    #state{physical_address_2_connections = Address2Connections,
      max_connections_per_broker = MaxConnectionsPerBroker} = State) ->
  Connections = maps:get(PhysicalAddr, Address2Connections, []),
  case {MaxConnectionsPerBroker, Connections} of
    {1, [C]} ->
      %% 1 connection with 1 as `max` so we just returned it.
      {C#cached_conn.pid, LogicalAddr, State};
    {Max, Connections} when length(Connections) < Max ->
      %% Less connections than the `max`; we create more
      case create_connection(LogicalAddr, State) of
        {error, _} = Error ->
          if Rest /= [] ->
            get_connection(Rest, LogicalAddr, State);
            true ->
              Error
          end;
        Result -> Result
      end;
    {Max, Connections} when Max == length(Connections) ->
      %% Connections == `max`; we return them sequentially
      Pos = State#state.cnx_lookup_current_pos,
      CurrConnection = lists:nth((Pos rem Max) + 1, Connections),
      {CurrConnection#cached_conn.pid, LogicalAddr,
        State#state{cnx_lookup_current_pos = Pos + 1}}
  end.


chose_broker_url(_PlainUrl, TlsUrl, #state{use_tls = true}) ->
  TlsUrl;
chose_broker_url(PlainUrl, _TlsUrl, _State) ->
  PlainUrl.

parse_and_resolve_service_url(ServiceUrl) ->
  ServiceUrl2 = erlwater:to_binary(ServiceUrl),
  URIs = binary:split(ServiceUrl2, <<",">>, [global]),
  Ls = lists:map(
    fun(Uri) ->
      case pulserl_utils:resolve_uri(Uri) of
        {Hostname, [Address | _], Port, _} ->
          {Hostname, Address, Port};
        Other ->
          Other
      end
    end,
    URIs),
  case lists:any(
    fun({error, _}) -> true;
      (_) -> false
    end, Ls) of
    true -> {error, "Invalid service url: " ++ ServiceUrl2};
    _ -> Ls
  end.


create_connection(LogicalAddress, #state{} = State) when is_binary(LogicalAddress) ->
  create_connection(binary_to_list(LogicalAddress), State);
create_connection(LogicalAddress, #state{} = State) ->
  ConnOpts = [
    {nodelay, State#state.enable_tcp_no_delay},
    {keepalive, State#state.enable_tcp_keep_alive}
  ],
  case pulserl_conn:create([{logical_address, LogicalAddress} | ConnOpts]) of
    {ok, CnxPid} ->
      %% To avoid race condition on new connection update,
      %% we retrieve the message here and do the update right away
      receive
        {?CONNECTION_UP_EVENT, SockAddr, _Socket, CnxPid} ->
          erlang:monitor(process, CnxPid),
          State1 = update_physical_address_2_logical_address_map(SockAddr, LogicalAddress, State),
          State2 = add_broker_connection(CnxPid, SockAddr, State1),
          {CnxPid, LogicalAddress, State2}
      end;
    {error, _} = Error -> Error
  end.


remove_broker_connection(CnxPid, State) ->
  case maps:take(CnxPid, State#state.connection_2_physical_address) of
    {PhysicalAddress, NewMap} ->
      NewState = State#state{connection_2_physical_address = NewMap},
      update_physical_address_2_connections_map(PhysicalAddress,
        #cached_conn{pid = CnxPid, active = false}, NewState);
    _ ->
      State
  end.

add_broker_connection(CnxPid, PhysicalAddress, State) ->
  NewMap = maps:put(CnxPid, PhysicalAddress, State#state.connection_2_physical_address),
  NewState = State#state{connection_2_physical_address = NewMap},
  update_physical_address_2_connections_map(PhysicalAddress,
    #cached_conn{pid = CnxPid, active = true}, NewState).

update_physical_address_2_logical_address_map(PhysicalAddress, LogicalAddress, State) ->
  Map = State#state.physical_address_2_logical_address,
  State#state{physical_address_2_logical_address = maps:put(PhysicalAddress, LogicalAddress, Map)}.

update_physical_address_2_connections_map(PhysicalAddress, #cached_conn{} = Conn, State) ->
  PhysicalAddress2Connections = maps:update_with(PhysicalAddress,
    fun(Connections) ->
      PidPos = #cached_conn.pid,
      CnxPid = element(PidPos, Conn),
      case lists:keyfind(CnxPid, PidPos, Connections) of
        false -> [Conn | Connections];
        _ -> lists:keyreplace(CnxPid, PidPos, Connections, Conn)
      end
    end,
    [Conn],
    State#state.physical_address_2_connections),
  error_logger:info_msg("Pulsar connections updated. Before: ~p. Now: ~p",
    [logical_address2connections(State#state.physical_address_2_connections, State#state.physical_address_2_logical_address),
      logical_address2connections(PhysicalAddress2Connections, State#state.physical_address_2_logical_address)
    ]),
  State#state{physical_address_2_connections = PhysicalAddress2Connections}.


logical_address2connections(Physical2Connections, Physical2LogicalAddresses) ->
  maps:fold(
    fun(PhysicalAddress, Connections, Acc) ->
      LogicalAddress = maps:get(PhysicalAddress, Physical2LogicalAddresses),
      maps:put(LogicalAddress, [{Pid, Active} || #cached_conn{pid = Pid, active = Active} <- Connections], Acc)
    end, #{}, Physical2Connections).

