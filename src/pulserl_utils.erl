%%%-------------------------------------------------------------------
%%% @author Alpha Umaru Shaw <shawalpha5@gmail.com>
%%% @doc
%%%
%%% @end
%%% Copyright: (C) 2020, Skulup Ltd
%%%-------------------------------------------------------------------
-module(pulserl_utils).

-include("pulserl.hrl").
-include("pulsar_api.hrl").
-include_lib("kernel/include/inet.hrl").

-export([new_message_id/2, new_message_id/4, new_message/5, new_message/6]).

%% API
-export([tls_enable/1, hash/2, get_int_env/2, get_env/2, resolve_uri/2, to_logical_address/3, sock_address_to_string/2, logical_to_physical_addresses/2]).

tls_enable(ServiceUrl) when is_binary(ServiceUrl) ->
  tls_enable(binary_to_list(ServiceUrl));

tls_enable(ServiceUrl) ->
  string:str(ServiceUrl, "pulsar+ssl://") > 0.

new_message_id(Topic, #'MessageIdData'{} = MessageIdData) ->
  new_message_id(Topic, MessageIdData, -1, 0).

new_message_id(Topic, #'MessageIdData'{
  ledgerId = LedgerId, entryId = EntryId,
  partition = Partition}, BatchIndex, BatchSize) ->
  #messageId{
    ledger_id = erlwater_assertions:is_integer(LedgerId),
    entry_id = erlwater_assertions:is_integer(EntryId),
    topic = topic_utils:to_string(Topic),
    partition =
    if is_integer(Partition) andalso Partition > 0 ->
      Partition;
      true -> topic_utils:partition_index(Topic)
    end,
    batch =
    if (BatchIndex >= 0 andalso BatchSize > 0) ->
      #batch{index = BatchIndex, size = BatchSize};
      true ->
        ?UNDEF
    end
  }.

new_message(Topic, MessageId, #'MessageMetadata'{} = Meta, Value, RedeliveryCount) ->
  #consMessage{
    id = MessageId,
    key = Meta#'MessageMetadata'.partition_key,
    value = Value,
    metadata = #messageMeta{
      topic = topic_utils:to_string(Topic),
      properties = Meta#'MessageMetadata'.properties,
      event_time = Meta#'MessageMetadata'.event_time,
      redelivery_count = if is_integer(RedeliveryCount) -> RedeliveryCount; true -> 0 end}
  }.

new_message(Topic, MessageId, #'MessageMetadata'{} = Meta, #'SingleMessageMetadata'{} = SingleMeta, Value, RedeliveryCount) ->
  Message = new_message(Topic, MessageId, Meta, Value, RedeliveryCount),
  Metadata = Message#consMessage.metadata,
  Message2 =
    case SingleMeta#'SingleMessageMetadata'.partition_key of
      ?UNDEF ->
        Message;
      PartitionKey ->
        Message#consMessage{key = PartitionKey}
    end,
  Metadata2 =
    case SingleMeta#'SingleMessageMetadata'.event_time of
      ?UNDEF ->
        Metadata;
      EventTime ->
        Metadata#messageMeta{event_time = EventTime}
    end,
  Message2#consMessage{metadata = Metadata2}.

hash(Key, ExclusiveUpperBound) when is_list(Key) ->
  hash(iolist_to_binary(Key), ExclusiveUpperBound);

hash(Key, ExclusiveUpperBound)
  when is_binary(Key)
  andalso is_integer(ExclusiveUpperBound)
  andalso ExclusiveUpperBound >= 1 ->
  erlang:phash2(Key, ExclusiveUpperBound).

sock_address_to_string(Ip, Port) ->
  inet:ntoa(Ip) ++ ":" ++ integer_to_list(Port).


to_logical_address(Hostname, Port, TlsEnable) ->
  maybe_prepend_scheme(Hostname ++ ":" ++ integer_to_list(Port), TlsEnable).

logical_to_physical_addresses(Address, TlsEnable) when is_list(Address) ->
  case resolve_uri(list_to_binary(Address), TlsEnable) of
    {error, Reason} ->
      error({bad_address, Reason});
    {_, Addresses, Port, _} ->
      [{Host, Port} || Host <- Addresses]
  end.

resolve_uri(Uri, _TlsEnable) ->
  Uri1 = trim(binary_to_list(Uri)),
  case parse_uri(Uri1) of
    {error, _} ->
      {error, invalid_uri};
    {Host, Port} ->
      case resolve_address(Host) of
        {error, _} = Err ->
          Err;
        {Hostname, AddressType, Addresses} ->
          {Hostname, Addresses, Port, AddressType}
      end
  end.

parse_uri(Uri) ->
  case catch uri_string:parse(Uri) of
    #{host := Host, port := Port} ->
      {Host, Port};
    {error, Reason, Term} ->
      {error, {Reason, Term}};
    _ -> %% `uri_string:parse/1` undefined
      case http_uri:parse(Uri) of
        {ok, {_Scheme, _UserInfo, Host, Port, _Path, _Query}} ->
          {Host, Port};
        Error -> Error
      end
  end.

trim(Uri) ->
  case catch string:trim(Uri) of
    Uri1 when is_list(Uri1) ->
      Uri1;
    _ ->
      %% `string:trim/1` undefined
      string:strip(Uri)
  end.

resolve_address(Hostname) ->
  case inet:gethostbyname(Hostname) of
    {error, _} = Err ->
      Err;
    {ok, #hostent{h_name = Host, h_addrtype = AddressType, h_addr_list = Addresses}} ->
      {Host, AddressType, Addresses}
  end.

maybe_prepend_scheme(Url, TlsEnable) ->
  case string:str(Url, "//") of
    0 ->
      if TlsEnable ->
        "pulsar+ssl://" ++ Url;
        true ->
          "pulsar://" ++ Url
      end;
    _ -> Url
  end.

get_int_env(Param, Default) when is_integer(Default) ->
  erlwater_env:get_int_env(pulserl, Param, Default).


get_env(Param, Default) ->
  erlwater_env:get_env(pulserl, Param, Default).