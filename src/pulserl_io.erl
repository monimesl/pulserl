%%%-------------------------------------------------------------------
%%% @author Alpha Umaru Shaw <shawalpha5@gmail.com>
%%% @doc
%%%
%%% @end
%%% Company: Skulup Ltd
%%% Copyright: (C) 2020
%%%-------------------------------------------------------------------
-module(pulserl_io).
-author("Alpha Umaru Shaw").

-include("pulsar_api.hrl").

%% API
-export([read_frame/1, read_int32/1, write_int16/1, write_int32/1]).
-export([encode_command/1, encode/3, decode_command/1, decode_metadata/1]).

-define(CMD_SIZE_LEN, 4).
-define(CHECKSUM_SIZE_LEN, 4).
-define(METADATA_SIZE_LEN, 4).
-define(MAGIC_NUMBER_SIZE_LEN, 2).
-define(MAGIC_NUMBER, 16#0e01).
-define(FRAME_LENGTH_INDICATOR_BYTE_SIZE, 4).

write_int16(I) when is_integer(I) ->
  <<I:16/unsigned-integer>>.

write_int32(I) when is_integer(I) ->
  <<I:32/unsigned-integer>>.

read_int32(<<Int:32/unsigned-integer, Rest/binary>>) ->
  {Int, Rest}.

%% @Read https://pulsar.apache.org/docs/en/develop-binary-protocol/#framing
read_frame(Buffer) ->
  if byte_size(Buffer) > ?FRAME_LENGTH_INDICATOR_BYTE_SIZE ->
    {Length, Rest} = read_int32(Buffer),
    if byte_size(Rest) >= Length ->
      <<Data:Length/binary, NewBuffer/binary>> = Rest,
      Frame = <<Length:32/unsigned-integer, Data/binary>>,
      {Frame, NewBuffer};
      true ->
        false
    end;
    true ->
      false
  end.


%% Format = [TOTAL_SIZE] [CMD_SIZE][CMD]
encode_command(#'BaseCommand'{} = Command) ->
  Message = pulsar_api:encode_msg(Command),
  CmdSize = write_int32(byte_size(Message)),
  TotalSize = write_int32(byte_size(CmdSize) + byte_size(Message)),
  <<TotalSize/binary, CmdSize/binary, Message/binary>>;

encode_command(Command) ->
  BaseCmd = wrap_to_base_command(Command),
  encode_command(BaseCmd).


%% Format = [TOTAL_SIZE(4)] [CMD_SIZE(4)][CMD(~)] [MAGIC_NUMBER(2)][CHECKSUM(4)] [METADATA_SIZE(~)][METADATA(~)] [PAYLOAD(~)]
encode(InnerCommand, #'MessageMetadata'{} = Metadata, Payload) when is_binary(Payload) ->
  BaseCommand = wrap_to_base_command(InnerCommand),
  EncodedCommand = pulsar_api:encode_msg(BaseCommand),
  EncodedMetadata = pulsar_api:encode_msg(Metadata),
  CommandSize = byte_size(EncodedCommand),
  MetadataSize = byte_size(EncodedMetadata),
  MetadataSize_Metadata_Payload = [write_int32(MetadataSize), EncodedMetadata, Payload],
  TotalSize = ?CMD_SIZE_LEN + CommandSize + ?MAGIC_NUMBER_SIZE_LEN + ?CHECKSUM_SIZE_LEN +
    ?METADATA_SIZE_LEN + MetadataSize + byte_size(Payload),
  iolist_to_binary([
    write_int32(TotalSize),
    write_int32(CommandSize),
    EncodedCommand,
    write_int16(?MAGIC_NUMBER),
    write_int32(crc32c(MetadataSize_Metadata_Payload)), MetadataSize_Metadata_Payload]).


decode_command(Data) when is_binary(Data) ->
  <<_TotalSize:32/unsigned-integer, CmdSize:32/unsigned-integer, Rest/binary>> = Data,
  {BaseCommand, HeadersAndPayload} =
    if CmdSize == byte_size(Rest) ->
      {pulsar_api:decode_msg(Rest, 'BaseCommand'), <<>>};
      true ->
        <<Command:CmdSize/binary, HeadersAndPayload0/binary>> = Rest,
        {pulsar_api:decode_msg(Command, 'BaseCommand'), HeadersAndPayload0}
    end,
  %% Sometimes a PING is embedded
  %% together with other type command
  %% in a `BaseCommand` message. I've seen it
  ['BaseCommand', _Type | WrappedCommands] =
    lists:filter(
      fun(Val) -> Val /= undefined end,
      tuple_to_list(BaseCommand)
    ),
  {WrappedCommands, HeadersAndPayload}.

decode_metadata(HeadersAndPayload) ->
  case verify_checksum(HeadersAndPayload) of
    {error, _} = Error ->
      Error;
    _ ->
      <<MetadataSize:32/unsigned-integer, MetadataPayload/binary>> =
        case has_checksum(HeadersAndPayload) of
          true ->
            <<_Checksum:4/binary, Rest/binary>> = HeadersAndPayload,
            Rest;
          _ ->
            HeadersAndPayload
        end,
      <<Metadata:MetadataSize/binary, Payload/binary>> = MetadataPayload,
      MetaData = pulsar_api:decode_msg(Metadata, 'MessageMetadata'),
      uncompress(MetaData, Payload)
  end.

uncompress(#'MessageMetadata'{compression = 'ZLIB'} = MetaData, Payload) ->
  Z = zlib:open(),
  ok = zlib:inflateInit(Z),
  [UnzippedPayload] = zlib:inflate(Z, Payload),
  zlib:close(Z),
  {MetaData, UnzippedPayload};

uncompress(#'MessageMetadata'{compression = 'NONE'} = MetaData, Payload) ->
  {MetaData, Payload};

uncompress(_MetaData, _Payload) ->
  {error, unsupported_compression}.
  
verify_checksum(HeadersAndPayload) ->
  case has_checksum(HeadersAndPayload) of
    true ->
      <<_MagicNumber:2/binary,
        Checksum:32/unsigned-integer,
        Rest/binary>> = HeadersAndPayload,
      CalculatedChecksum = crc32c(Rest),
      if CalculatedChecksum /= Checksum ->
        {error, corrupted_message};
        true ->
          ok
      end;
    _ ->
      ok
  end.

wrap_to_base_command(InnerCmd) when is_tuple(InnerCmd) ->
  {Type, FieldPos} = to_type_and_field_pos(InnerCmd),
  BaseCommand = #'BaseCommand'{
    type = Type
  },
  setelement(FieldPos, BaseCommand, InnerCmd).

has_checksum(<<MagicNumber:16/unsigned-integer, _/binary>>) ->
  MagicNumber == ?MAGIC_NUMBER.

crc32c(Ls) ->
  crc32cer:nif(Ls).

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
to_type_and_field_pos(#'CommandAck'{}) ->
  {'ACK', #'BaseCommand'.ack};
to_type_and_field_pos(#'CommandSeek'{}) ->
  {'SEEK', #'BaseCommand'.seek};
to_type_and_field_pos(#'CommandRedeliverUnacknowledgedMessages'{}) ->
  {'REDELIVER_UNACKNOWLEDGED_MESSAGES', #'BaseCommand'.redeliverUnacknowledgedMessages};
to_type_and_field_pos(Command) ->
  erlang:error({unknown_command, Command}).