%%%-------------------------------------------------------------------
%%% @author Alpha Umaru Shaw <shawalpha5@gmail.com>
%%% @doc
%%%
%%% @end
%%% Copyright: (C) 2020, Skulup Ltd
%%%-------------------------------------------------------------------
-module(commands).

-include("pulserl.hrl").
-include("pulsar_api.hrl").

-export([get_request_id/1, set_request_id/2]).
-export([has_messages_in_batch/1, to_con_prod_metadata/1]).
-export([new_send/8, new_ack/2, new_ack/3, new_seek/2, new_connect/0]).
-export([new_lookup_topic/2, new_partitioned_topic_meta/1, new_redeliver_unack_messages/2]).

has_messages_in_batch(#'MessageMetadata'{num_messages_in_batch = NumOfBatchMessages}) ->
  is_number(NumOfBatchMessages).

to_con_prod_metadata(?UNDEF) ->
  [];
to_con_prod_metadata(Map) when is_map(Map) ->
  to_con_prod_metadata(maps:to_list(Map));
to_con_prod_metadata(Ls) when is_list(Ls) ->
  lists:map(
    fun({Key, Value}) ->
      #'KeyValue'{key = erlwater:to_binary(Key), value = erlwater:to_binary(Value)};
      (I) ->
        error("Bad property. Expecting tuple", [I])
    end, Ls).

new_seek(ConsumerId, {LedgerId, EntryId}) ->
  #'CommandSeek'{
    consumer_id = ConsumerId,
    message_id = #'MessageIdData'{ledgerId = LedgerId, entryId = EntryId}
  };

new_seek(ConsumerId, Timestamp) when is_integer(Timestamp) ->
  #'CommandSeek'{
    consumer_id = ConsumerId,
    message_publish_time = Timestamp
  }.

new_send(ProducerId, ProducerName, SequenceId, PartitionKey, EventTime, NumMessages, DeliverAtTime, Payload) ->
  SendCmd = #'CommandSend'{
    sequence_id = SequenceId, producer_id = ProducerId},
  Metadata = #'MessageMetadata'{
    event_time = EventTime,
    sequence_id = SequenceId,
    producer_name = ProducerName,
    partition_key = PartitionKey,
    deliver_at_time = DeliverAtTime,
    uncompressed_size = byte_size(Payload),
    publish_time = erlwater_time:milliseconds(),
    num_messages_in_batch = NumMessages %% Must be `undefined` for non-batch messages
  },
  {SendCmd, Metadata}.


new_ack(ConsumerId,
    #messageId{ledger_id = LedgerId, entry_id = EntryId}, Cumulative) ->
  #'CommandAck'{
    consumer_id = ConsumerId,
    ack_type =
    if Cumulative ->
      'Cumulative';
      true ->
        'Individual'
    end,
    message_id = [#'MessageIdData'{ledgerId = LedgerId, entryId = EntryId}]
  }.

new_ack(ConsumerId, [#messageId{} | _] = MessageIds) ->
  #'CommandAck'{
    consumer_id = ConsumerId,
    ack_type = 'Individual',
    message_id = [#'MessageIdData'{
      ledgerId = LedgerId,
      entryId = EntryId
    } || #messageId{ledger_id = LedgerId, entry_id = EntryId} <- MessageIds]
  }.

new_redeliver_unack_messages(ConsumerId, MessageIds) when is_list(MessageIds) ->
  #'CommandRedeliverUnacknowledgedMessages'{
    consumer_id = ConsumerId,
    message_ids = [#'MessageIdData'{
      ledgerId = LedgerId,
      entryId = EntryId
    } || #messageId{ledger_id = LedgerId, entry_id = EntryId} <- MessageIds]
  }.

new_connect() ->
  #'CommandConnect'{
    protocol_version = 10,
    client_version = pulserl_utils:get_client_version(),
    proxy_to_broker_url = pulserl_utils:proxy_to_broker_url_env()
  }.

new_partitioned_topic_meta(Topic) ->
  #'CommandPartitionedTopicMetadata'{
    topic = Topic
  }.

new_lookup_topic(Topic, Authoritative) ->
  #'CommandLookupTopic'{
    topic = Topic,
    authoritative = Authoritative
  }.

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
