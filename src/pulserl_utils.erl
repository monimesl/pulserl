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
-export([hash_key/2, get_int_env/2, get_env/2, resolve_uri/2, to_logical_address/3, sock_address_to_string/2, logical_to_physical_addresses/2]).

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
  #message{
    id = MessageId,
    key = Meta#'MessageMetadata'.partition_key,
    value = Value,
    topic = topic_utils:to_string(Topic),
    properties = Meta#'MessageMetadata'.properties,
    metadata = #messageMeta{
      event_time = Meta#'MessageMetadata'.event_time,
      redelivery_count = if is_integer(RedeliveryCount) -> RedeliveryCount; true -> 0 end}
  }.

new_message(Topic, MessageId, #'MessageMetadata'{} = Meta, #'SingleMessageMetadata'{} = SingleMeta, Value, RedeliveryCount) ->
  Message = new_message(Topic, MessageId, Meta, Value, RedeliveryCount),
  Metadata = Message#message.metadata,
  Message2 =
    case SingleMeta#'SingleMessageMetadata'.partition_key of
      ?UNDEF ->
        Message;
      PartitionKey ->
        Message#message{key = PartitionKey}
    end,
  Metadata2 =
    case SingleMeta#'SingleMessageMetadata'.event_time of
      ?UNDEF ->
        Metadata;
      EventTime ->
        Metadata#messageMeta{event_time = EventTime}
    end,
  Message2#message{metadata = Metadata2}.


hash_key(undefined, Divisor) ->
  hash_key(<<>>, Divisor);
hash_key(<<>>, Divisor) ->
  hash_key(crypto:strong_rand_bytes(8), Divisor);
hash_key(Key, Divisor) ->
  erlang:abs(crc32cer:nif(Key)) rem Divisor.


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

resolve_uri(Uri, TlsEnable) ->
  Uri1 = string:trim(binary_to_list(Uri)),
  Uri2 = maybe_prepend_scheme(Uri1, TlsEnable),
  case uri_string:parse(Uri2) of
    {error, _, _} ->
      {error, invalid_uri};
    UriMap ->
      Host = maps:get(host, UriMap),
      case resolve_address(Host) of
        {error, _} = Err ->
          Err;
        {Hostname, AddressType, Addresses} ->
          Port = maps:get(port, UriMap),
          {Hostname, Addresses, Port, AddressType}
      end
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
