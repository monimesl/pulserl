%%%-------------------------------------------------------------------
%%% @author Alpha Umaru Shaw <shawalpha5@gmail.com>
%%% @doc
%%%
%%% @end
%%% Company: Skulup Ltd
%%% Copyright: (C) 2019
%%%-------------------------------------------------------------------
-module(commands).
-author("Alpha Umaru Shaw").

-include("pulsar_api.hrl").


-define(CLIENT_VSN, "0.1.0").
-define(MAGIC_CRC32C, 16#0e01).

%% API
-export([cmd_connect/0, cmd_lookup_topic/2, cmd_partitioned_topic_meta/1]).

-export([simple_encode/1, payload_encode/3, decode_unwrap/1, wrap/1, get_request_id/1, set_request_id/2, encode_to_4bytes/1, encode_to_2bytes/1]).


cmd_connect() ->
  #'CommandConnect'{
    client_version = ?CLIENT_VSN,
    protocol_version = 6
  }.

cmd_partitioned_topic_meta(Topic) ->
  #'CommandPartitionedTopicMetadata'{
    topic = Topic
  }.

cmd_lookup_topic(Topic, Authoritative) ->
  #'CommandLookupTopic'{
    topic = Topic,
    authoritative = Authoritative
  }.


wrap(Command) when is_tuple(Command) ->
  {Type, FieldPos} = type_fieldpos(Command),
  base_wrap(Command, Type, FieldPos).


%% Format = [TOTAL_SIZE] [CMD_SIZE][CMD]
simple_encode(#'BaseCommand'{} = Command) ->
  Message = pulsar_api:encode_msg(Command),
  CmdSize = encode_to_4bytes(byte_size(Message)),
  TotalSize = encode_to_4bytes(byte_size(CmdSize) + byte_size(Message)),
  <<TotalSize/binary, CmdSize/binary, Message/binary>>.

-define(CMD_LEN, 4).
-define(CHECKSUM_LEN, 4).
-define(METADATA_LEN, 4).
-define(MAGIC_NUMBER_LEN, 2).

%% Format = [TOTAL_SIZE(4)] [CMD_SIZE(4)][CMD(~)] [MAGIC_NUMBER(2)][CHECKSUM(4)] [METADATA_SIZE(~)][METADATA(~)] [PAYLOAD(~)]
payload_encode(#'BaseCommand'{} = C, #'MessageMetadata'{} = Meta, Payload) when is_binary(Payload) ->
  SerializedCommand = pulsar_api:encode_msg(C),
  CmdSize = byte_size(SerializedCommand),
  Metadata = pulsar_api:encode_msg(Meta),
  MetadataSize = byte_size(Metadata),
  Data2Checksum = [encode_to_4bytes(MetadataSize), Metadata, Payload],
  TotalSize = ?CMD_LEN + CmdSize + ?MAGIC_NUMBER_LEN + ?CHECKSUM_LEN + ?METADATA_LEN + MetadataSize + byte_size(Payload),
  iolist_to_binary([
    encode_to_4bytes(TotalSize),
    encode_to_4bytes(CmdSize),
    SerializedCommand,
    encode_to_2bytes(?MAGIC_CRC32C),
    encode_to_4bytes(crc32c(Data2Checksum)), Data2Checksum]).


%% Sometimes a PING is embedded
%% together with other type response
%% in a `BaseCommand` message
decode_unwrap(BinData) ->
  BaseCommand = decode(BinData),
  ['BaseCommand', _Type | WrappedCommands] =
    lists:filter(
      fun(Val) -> Val /= undefined end,
      tuple_to_list(BaseCommand)
    ),
  WrappedCommands.


crc32c(Ls) ->
  crc32cer:nif(Ls).


get_request_id(Command) when is_tuple(Command) ->
  case Command of
    #'CommandProducerSuccess'{} -> Command#'CommandProducerSuccess'.request_id;
    #'CommandLookupTopicResponse'{} -> Command#'CommandLookupTopicResponse'.request_id;
    #'CommandPartitionedTopicMetadataResponse'{} ->
      Command#'CommandPartitionedTopicMetadataResponse'.request_id;
    _ -> false
  end.

set_request_id(Command, ReqId) when is_tuple(Command) ->
  case Command of
    #'CommandProducer'{} -> Command#'CommandProducer'{request_id = ReqId};
    #'CommandLookupTopic'{} -> Command#'CommandLookupTopic'{request_id = ReqId};
    #'CommandPartitionedTopicMetadata'{} ->
      Command#'CommandPartitionedTopicMetadata'{request_id = ReqId};
    _ ->
      false
  end.

decode(BinData) ->
  <<_:8/binary, Message/binary>> = BinData,
  pulsar_api:decode_msg(Message, 'BaseCommand').


base_wrap(Command, Type, FieldPos) ->
  Cmd = #'BaseCommand'{
    type = Type
  },
  setelement(FieldPos, Cmd, Command).


encode_to_2bytes(I) when is_integer(I) ->
  <<I:16/unsigned-integer>>.

encode_to_4bytes(I) when is_integer(I) ->
  <<I:32/unsigned-integer>>.


type_fieldpos(#'CommandConnect'{}) ->
  {'CONNECT', #'BaseCommand'.connect};
type_fieldpos(#'CommandLookupTopic'{}) ->
  {'LOOKUP', #'BaseCommand'.lookupTopic};
type_fieldpos(#'CommandPartitionedTopicMetadata'{}) ->
  {'PARTITIONED_METADATA', #'BaseCommand'.partitionMetadata};
type_fieldpos(#'CommandProducer'{}) ->
  {'PRODUCER', #'BaseCommand'.producer};
type_fieldpos(#'CommandSend'{}) ->
  {'SEND', #'BaseCommand'.send};
type_fieldpos(#'CommandPong'{}) ->
  {'PONG', #'BaseCommand'.pong};
type_fieldpos(Command) ->
  erlang:error({unknown_command, Command}).
