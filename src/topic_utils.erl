%%%------------------------------------------------------
%%%    Copyright 2022 Monime Ltd, licensed under the
%%%    Apache License, Version 2.0 (the "License");
%%%-------------------------------------------------------
-module(topic_utils).
-author("Alpha Umaru Shaw <shawalpha5@gmail.com>").

-include("pulserl.hrl").

%% API
-export([parse/1, to_string/1, partition_index/1, is_partitioned/1,
  new_partition/2, partition_str/2, partition_of/2, is_persistent/1]).


parse(#topic{} = Topic) ->
  Topic;
parse(Topic) when is_list(Topic) ->
  parse(iolist_to_binary(Topic));

parse(Topic) when is_binary(Topic) ->
  TopicStr =
    case binary:match(Topic, [<<"://">>]) of
      nomatch ->
        case binary:split(Topic, <<"/">>, [global]) of
          [_Namespace, _Name] ->
            iolist_to_binary([?PERSISTENT_DOMAIN, "://", ?PUBLIC_TENANT, "/", Topic]);
          [_Tenant, _Namespace | _Name] ->
            iolist_to_binary([?PERSISTENT_DOMAIN, "://", Topic]);
          [_Name] ->
            iolist_to_binary([?PERSISTENT_DOMAIN, "://", ?PUBLIC_TENANT, "/", ?DEFAULT_NAMESPACE, "/", Topic]);
          _ -> error({bad_topic_name, "Name must be in the format <topic>, or "
          "[<tenant>/]<namespace>/<topic>"}, [Topic])
        end;
      _ -> Topic
    end,
  case binary:split(TopicStr, <<"://">>) of
    [Domain, Rest] ->
      case binary:split(Rest, <<"/">>, [global]) of
        [Namespace, LocalName] ->
          #topic{domain = Domain, tenant = ?PUBLIC_TENANT,
            namespace = Namespace, local = LocalName};
        [Tenant, Namespace | LocalName] ->
          #topic{domain = Domain, tenant = Tenant,
            namespace = Namespace, local = iolist_to_binary(join(LocalName, <<"/">>))};
        _ ->
          error(bad_topic_name, [TopicStr])
      end;
    _ -> error(bad_topic_name, [TopicStr])
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

is_persistent(Topic) when is_list(Topic) ->
  is_persistent(list_to_binary(Topic));

is_persistent(Topic) when is_binary(Topic) ->
  is_persistent(parse(Topic));

is_persistent(#topic{domain = ?PERSISTENT_DOMAIN}) ->
  true;

is_persistent(#topic{}) ->
  false.

is_partitioned(#topic{local = Local}) ->
  is_partitioned(Local);
is_partitioned(Topic) when is_binary(Topic) ->
  partition_index(Topic) >= 0.

new_partition(#topic{} = Parent, Index)
  when is_integer(Index) andalso Index >= 0 ->
  Pt = parse(partition_str(Parent, Index)),
  Pt#topic{parent = Parent}.

partition_str(#topic{} = Topic, Index) ->
  iolist_to_binary([to_string(Topic), "-partition-", integer_to_list(Index)]).

partition_index(#topic{local = Local}) ->
  partition_index(Local);
partition_index(Topic) when is_binary(Topic) ->
  case binary:split(Topic, [<<"-partition-">>]) of
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
  tenant = Tenant, namespace = Namespace, local = Local}) ->
  iolist_to_binary([Domain, "://", Tenant, "/", Namespace, "/", Local]).

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