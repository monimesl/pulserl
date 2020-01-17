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
-define(MAGIC_NUMBER, 16#0e01).
-define(CMD_SIZE_LEN, 4).
-define(CHECKSUM_LEN, 4).
-define(METADATA_LEN, 4).
-define(MAGIC_NUMBER_LEN, 2).

-export([encode/1, decode/1, new_send/7]).

-export([entry_id/1, ledger_id/1]).
-export([cmd_connect/0, cmd_lookup_topic/2, cmd_partitioned_topic_meta/1]).

-export([encode/3, wrap_to_base_command/1, get_request_id/1, set_request_id/2, to_4bytes/1, encode_to_2bytes/1]).


%% Format = [TOTAL_SIZE] [CMD_SIZE][CMD]
encode(#'BaseCommand'{} = Command) ->
  Message = pulsar_api:encode_msg(Command),
  CmdSize = to_4bytes(byte_size(Message)),
  TotalSize = to_4bytes(byte_size(CmdSize) + byte_size(Message)),
  <<TotalSize/binary, CmdSize/binary, Message/binary>>;

encode(InnerCommand) ->
  BaseCmd = wrap_to_base_command(InnerCommand),
  encode(BaseCmd).


%% Format = [TOTAL_SIZE(4)] [CMD_SIZE(4)][CMD(~)] [MAGIC_NUMBER(2)][CHECKSUM(4)] [METADATA_SIZE(~)][METADATA(~)] [PAYLOAD(~)]
encode(InnerCommand, #'MessageMetadata'{} = Meta, Payload) when is_binary(Payload) ->
  BaseCommand = wrap_to_base_command(InnerCommand),
  SerializedCommand = pulsar_api:encode_msg(BaseCommand),
  CmdSize = byte_size(SerializedCommand),
  Metadata = pulsar_api:encode_msg(Meta),
  MetadataSize = byte_size(Metadata),
%%  error_logger:info_msg("Magic Number: ~p => ~p", [?MAGIC_NUMBER, encode_to_2bytes(?MAGIC_NUMBER)]),
%%  error_logger:info_msg("MetadataSize: ~p", [MetadataSize]),
%%  error_logger:info_msg("Payload: ~p", [Payload]),
  MetadataSize_Metadata_Payload = [to_4bytes(MetadataSize), Metadata, Payload],
  TotalSize = ?CMD_SIZE_LEN + CmdSize + ?MAGIC_NUMBER_LEN + ?CHECKSUM_LEN + ?METADATA_LEN + MetadataSize + byte_size(Payload),
  iolist_to_binary([
    to_4bytes(TotalSize),
    to_4bytes(CmdSize),
    SerializedCommand,
    encode_to_2bytes(?MAGIC_NUMBER),
    to_4bytes(crc32c(MetadataSize_Metadata_Payload)), MetadataSize_Metadata_Payload]).


decode(Data) when is_binary(Data) ->
  <<_TotalSize:32/unsigned-integer, CmdSize:32/unsigned-integer, Rest/binary>> = Data,
  {BaseCommand, HeadersAndPayload} =
    if CmdSize == byte_size(Rest) ->
      {pulsar_api:decode_msg(Rest, 'BaseCommand'), <<>>};
      true ->
        <<Command:CmdSize/binary, HeadersAndPayload0/binary>> = Rest,
        {pulsar_api:decode_msg(Command, 'BaseCommand'), HeadersAndPayload0}
    end,
  %% Sometimes a PING is embedded
  %% together with other type response
  %% in a `BaseCommand` message
  ['BaseCommand', _Type | WrappedCommands] =
    lists:filter(
      fun(Val) -> Val /= undefined end,
      tuple_to_list(BaseCommand)
    ),
  {hd(WrappedCommands), HeadersAndPayload}.


ledger_id(#'MessageIdData'{ledgerId = LedgerId}) ->
  LedgerId.

entry_id(#'MessageIdData'{entryId = EntryId}) ->
  EntryId.


new_send(ProducerId, ProducerName, SequenceId, PartitionKey, EventTime, NumMessages, Payload) ->
  SendCmd = #'CommandSend'{
    sequence_id = SequenceId, producer_id = ProducerId},
  Metadata = #'MessageMetadata'{
    event_time = EventTime,
    sequence_id = SequenceId,
    producer_name = ProducerName,
    partition_key = PartitionKey,
    publish_time = erlwater_time:milliseconds(),
    uncompressed_size = byte_size(Payload),
    num_messages_in_batch = NumMessages %% Must be `undefined` for non-batch messages
  },
  {SendCmd, Metadata}.

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


wrap_to_base_command(InnerCmd) when is_tuple(InnerCmd) ->
  {Type, FieldPos} = to_type_and_field_pos(InnerCmd),
  BaseCommand = #'BaseCommand'{
    type = Type
  },
  setelement(FieldPos, BaseCommand, InnerCmd).






crc32c(Ls) ->
  crc32cer:nif(Ls).


get_request_id(Command) when is_tuple(Command) ->
  case Command of
    #'CommandSuccess'{} -> Command#'CommandSuccess'.request_id;
    #'CommandProducerSuccess'{} -> Command#'CommandProducerSuccess'.request_id;
    #'CommandLookupTopicResponse'{} -> Command#'CommandLookupTopicResponse'.request_id;
    #'CommandPartitionedTopicMetadataResponse'{} ->
      Command#'CommandPartitionedTopicMetadataResponse'.request_id;
    _ -> false
  end.

set_request_id(Cmd, ReqId) when is_tuple(Cmd) ->
  case Cmd of
    #'CommandSeek'{} -> Cmd#'CommandSeek'{request_id = ReqId};
    #'CommandProducer'{} -> Cmd#'CommandProducer'{request_id = ReqId};
    #'CommandGetSchema'{} -> Cmd#'CommandGetSchema'{request_id = ReqId};
    #'CommandSubscribe'{} -> Cmd#'CommandSubscribe'{request_id = ReqId};
    #'CommandLookupTopic'{} -> Cmd#'CommandLookupTopic'{request_id = ReqId};
    #'CommandUnsubscribe'{} -> Cmd#'CommandUnsubscribe'{request_id = ReqId};
    #'CommandCloseProducer'{} -> Cmd#'CommandCloseProducer'{request_id = ReqId};
    #'CommandCloseConsumer'{} -> Cmd#'CommandCloseConsumer'{request_id = ReqId};
    #'CommandGetLastMessageId'{} -> Cmd#'CommandGetLastMessageId'{request_id = ReqId};
    #'CommandGetTopicsOfNamespace'{} -> Cmd#'CommandGetTopicsOfNamespace'{request_id = ReqId};
    #'CommandPartitionedTopicMetadata'{} ->
      Cmd#'CommandPartitionedTopicMetadata'{request_id = ReqId};
    _ ->
      false
  end.

%%decode(BinData) ->
%%  {Command, HeadersAndPayload} = decode_command(BinData),
%%  case HeadersAndPayload of
%%    <<>> ->
%%      Command;
%%    _ ->
%%      payload_decode(Command, HeadersAndPayload)
%%  end.



payload_decode(Command, HeadersAndPayload) ->
  Checksum = read_checksum(HeadersAndPayload),
  case verify_checksum(HeadersAndPayload) of
    true ->
      ok;
    _ ->
      {error, checksum_mismatch}
  end,
  case read_checksum(HeadersAndPayload) of
    undefined ->
      ok;
    Checksum ->
      ok
  end,
%%  <<Message:CmdSize/binary, HeadersAndPayload/binary>> = Rest,
%%  MessageCmd = pulsar_api:decode_msg(Message, 'BaseCommand'),
%%  io:format("Message Command: ~p~n", [MessageCmd]),
%%  <<MagicNum:16/unsigned-integer, Checksum:32/unsigned-integer, MetadataSize:32/unsigned-integer, MetadataPayload/binary>>
%%    = HeadersAndPayload,
%%  io:format("TotalSize: ~p~n", [TotalSize]),
%%  io:format("MagicNum: ~p~n", [MagicNum]),
%%  io:format("Checksum: ~p~n", [Checksum]),
%%  io:format("Metadata Size: ~p~n", [MetadataSize]),
%%  <<Metadata:47/binary, Payload/binary>> = MetadataPayload,
%%  MetadataCmd = pulsar_api:decode_msg(Metadata, 'MessageMetadata'),
%%  io:format("Metadata Command: ~p~n", [MetadataCmd]),
  io:format("HeadersAndPayload: ~p~n", [HeadersAndPayload]),

  error(payload_command_unexpected, [HeadersAndPayload]).

verify_checksum(_) ->
  error(not_implemented_yet).

read_checksum(HeadersAndPayload) ->
  case has_checksum(HeadersAndPayload) of
    true ->
      <<_MagicNumber:2/binary,
        Checksum:32/unsigned-integer,
        _/binary>> = HeadersAndPayload,
      Checksum;
    _ ->
      undefined
  end.

has_checksum(<<MagicNumber:16/unsigned-integer, _/binary>>) ->
  MagicNumber == ?MAGIC_NUMBER.


base_wrap(Command, Type, FieldPos) ->
  Cmd = #'BaseCommand'{
    type = Type
  },
  setelement(FieldPos, Cmd, Command).


encode_to_2bytes(I) when is_integer(I) ->
  <<I:16/unsigned-integer>>.

to_4bytes(I) when is_integer(I) ->
  <<I:32/unsigned-integer>>.


to_type_and_field_pos(#'CommandConnect'{}) ->
  {'CONNECT', #'BaseCommand'.connect};
to_type_and_field_pos(#'CommandLookupTopic'{}) ->
  {'LOOKUP', #'BaseCommand'.lookupTopic};
to_type_and_field_pos(#'CommandPartitionedTopicMetadata'{}) ->
  {'PARTITIONED_METADATA', #'BaseCommand'.partitionMetadata};
to_type_and_field_pos(#'CommandProducer'{}) ->
  {'PRODUCER', #'BaseCommand'.producer};
to_type_and_field_pos(#'CommandSubscribe'{}) ->
  {'SUBSCRIBE', #'BaseCommand'.subscribe};
to_type_and_field_pos(#'CommandSend'{}) ->
  {'SEND', #'BaseCommand'.send};
to_type_and_field_pos(#'CommandFlow'{}) ->
  {'FLOW', #'BaseCommand'.flow};
to_type_and_field_pos(#'CommandPong'{}) ->
  {'PONG', #'BaseCommand'.pong};
to_type_and_field_pos(Command) ->
  erlang:error({unknown_command, Command}).
