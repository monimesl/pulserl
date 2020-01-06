%%%-------------------------------------------------------------------
%%% @author Alpha Umaru Shaw <shawalpha5@gmail.com>
%%% @doc
%%%
%%% @end
%%% Company: Skulup Ltd
%%% Copyright: (C) 2019
%%%-------------------------------------------------------------------
-module(topic_utils).
-author("Alpha Umaru Shaw").

-include("pulserl_topics.hrl").

%% API
-export([parse/1, to_string/1, is_partitioned/1, new_partition/2, new_partition_str/2]).


parse(CompleteName) when is_list(CompleteName) ->
  parse(iolist_to_binary(CompleteName));

parse(Name) when is_binary(Name) ->
  CompleteName =
    case binary:match(Name, [<<"://">>]) of
      nomatch ->
        case binary:split(Name, <<"/">>, [global]) of
          [_Name] ->
            iolist_to_binary([?PERSISTENT_DOMAIN, "://", ?PUBLIC_TENANT, "/", ?DEFAULT_NAMESPACE, "/", Name]);
          [_Tenant, _Namespace, _Name] ->
            iolist_to_binary([?PERSISTENT_DOMAIN, "://", Name]);
          [_Tenant, _Cluster, _Namespace, _Name] ->
            iolist_to_binary([?PERSISTENT_DOMAIN, "://", Name]);
          _ -> error({bad_topic_name, "Name must be in the format <topic>, or "
          "<tenant>/[<cluster>/]<namespace>/<topic>"}, [Name])
        end;
      _ -> Name
    end,
  case binary:split(CompleteName, <<"://">>) of
    [Domain, Rest] ->
      case binary:split(Rest, <<"/">>, [global]) of
        [Tenant, Namespace, LocalName] ->
          #topic{domain = string(Domain), tenant = string(Tenant),
            namespace = string(Namespace), local = string(LocalName)};
        [Tenant, Cluster, Namespace, LocalName] ->
          #topic{domain = string(Domain), tenant = string(Tenant),
            cluster = string(Cluster), namespace = string(Namespace), local = string(LocalName)};
        _ ->
          error(bad_topic_name, [CompleteName])
      end;
    _ -> error(bad_topic_name, [CompleteName])
  end.


is_partitioned(#topic{local = LocalName}) ->
  is_partitioned(LocalName);

is_partitioned(TopicName) when is_binary(TopicName) ->
  is_partitioned(iolist_to_binary(TopicName));
is_partitioned(TopicName) when is_list(TopicName) ->
  string:str(TopicName, "-partition-") > 0.


new_partition(#topic{} = Parent, Index)
  when is_integer(Index) andalso Index >= 0 ->
  Pt = parse(new_partition_str(Parent, Index)),
  Pt#topic{parent = Parent}.

new_partition_str(#topic{} = Topic, Index) ->
  to_string(Topic) ++ "-partition-" ++ integer_to_list(Index).


to_string(#topic{domain = Domain, cluster = Cluster,
  tenant = Tenant, namespace = Namespace, local = LocalName}) ->
  Result =
    case Cluster of
      undefined -> [Domain, "://", Tenant, "/", Namespace, "/", LocalName];
      Cl when is_list(Cl) -> [Domain, "://", Tenant, "/", Cl, "/", Namespace, "/", LocalName]
    end,
  binary_to_list(iolist_to_binary(Result)).


%%%===================================================================
%%% Internal functions
%%%===================================================================

string(B) when is_binary(B) ->
  binary_to_list(B);
string(S) when is_list(S) ->
  S.
