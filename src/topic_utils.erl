%%%-------------------------------------------------------------------
%%% @author Alpha Umaru Shaw <shawalpha5@gmail.com>
%%% @doc
%%%
%%% @end
%%% Copyright: (C) 2020, Skulup Ltd
%%%-------------------------------------------------------------------
-module(topic_utils).

-include("pulserl.hrl").

%% API
-export([parse/1, to_string/1, partition_index/1, is_partitioned/1,
  new_partition/2, partition_str/2, partition_of/2, is_persistent/1]).


parse(CompleteName) when is_list(CompleteName) ->
  parse(iolist_to_binary(CompleteName));

parse(Name) when is_binary(Name) ->
  CompleteName =
    case binary:match(Name, [<<"://">>]) of
      nomatch ->
        case binary:split(Name, <<"/">>, [global]) of
          [_Namespace, _Name] ->
            iolist_to_binary([?PERSISTENT_DOMAIN, "://", ?PUBLIC_TENANT, "/", Name]);
          [_Tenant, _Namespace | _Name] ->
            iolist_to_binary([?PERSISTENT_DOMAIN, "://", Name]);
          [_Name] ->
            iolist_to_binary([?PERSISTENT_DOMAIN, "://", ?PUBLIC_TENANT, "/", ?DEFAULT_NAMESPACE, "/", Name]);
          _ -> error({bad_topic_name, "Name must be in the format <topic>, or "
          "[<tenant>/]<namespace>/<topic>"}, [Name])
        end;
      _ -> Name
    end,
  case binary:split(CompleteName, <<"://">>) of
    [Domain, Rest] ->
      case binary:split(Rest, <<"/">>, [global]) of
        [Namespace, LocalName] ->
          #topic{domain = Domain, tenant = ?PUBLIC_TENANT,
            namespace = Namespace, local = LocalName};
        [Tenant, Namespace | LocalName] ->
          #topic{domain = Domain, tenant = Tenant,
            namespace = Namespace, local = iolist_to_binary(join(LocalName, <<"/">>))};
        _ ->
          error(bad_topic_name, [CompleteName])
      end;
    _ -> error(bad_topic_name, [CompleteName])
  end.

partition_of(#topic{} = Parent, PartitionTopic) when is_binary(PartitionTopic) ->
  partition_of(to_string(Parent), PartitionTopic);

partition_of(Parent, PartitionTopic) ->
  if Parent /= PartitionTopic ->
    case binary:split(PartitionTopic, Parent) of
      [<<>>, <<"-partition-", Rest/binary>>] when Rest /= <<>> ->
        true;
      _ ->
        false
    end;
    true ->
      false
  end.

is_persistent(TopicName) when is_list(TopicName) ->
  is_persistent(list_to_binary(TopicName));

is_persistent(TopicName) when is_binary(TopicName) ->
  is_persistent(parse(TopicName));

is_persistent(#topic{domain = ?PERSISTENT_DOMAIN}) ->
  true;

is_persistent(#topic{}) ->
  false.

is_partitioned(#topic{local = LocalName}) ->
  is_partitioned(LocalName);
is_partitioned(TopicName) when is_binary(TopicName) ->
  partition_index(TopicName) >= 0.

new_partition(#topic{} = Parent, Index)
  when is_integer(Index) andalso Index >= 0 ->
  Pt = parse(partition_str(Parent, Index)),
  Pt#topic{parent = Parent}.

partition_str(#topic{} = Topic, Index) ->
  iolist_to_binary([to_string(Topic), "-partition-", integer_to_list(Index)]).

partition_index(#topic{local = LocalName}) ->
  partition_index(LocalName);
partition_index(TopicName) when is_binary(TopicName) ->
  case binary:split(TopicName, [<<"-partition-">>]) of
    [_, Index] ->
      binary_to_integer(Index);
    _ ->
      -1
  end.

to_string(Topic) when is_binary(Topic) ->
  Topic;

to_string(Topic) when is_list(Topic) ->
  iolist_to_binary(Topic);

to_string(#topic{domain = Domain,
  tenant = Tenant, namespace = Namespace, local = LocalName}) ->
  iolist_to_binary([Domain, "://", Tenant, "/", Namespace, "/", LocalName]).

%%%===================================================================
%%% Internal functions
%%%===================================================================

join([], _Separator) ->
  [];
join([P], _Separator) ->
  [P];
join(Parts, Separator) ->
  lists:reverse(join2(Parts, Separator, [])).

join2([H | []], _Separator, Acc) ->
  [H | Acc];
join2([H | T], Separator, Acc) ->
  join2(T, Separator, [[H, Separator] | Acc]).