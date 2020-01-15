%%%-------------------------------------------------------------------
%%% @author Alpha Umaru Shaw <shawalpha5@gmail.com>
%%% @doc
%%%
%%% @end
%%% Company: Skulup Ltd
%%% Copyright: (C) 2019
%%%-------------------------------------------------------------------
-module(pulserl_utils).
-author("Alpha Umaru Shaw").


-include_lib("kernel/include/inet.hrl").

%% API
-export([hash_key/2, get_int_env/2, get_env/2, resolve_uri/1, to_logical_address/2, sock_address_to_string/2, logical_to_physical_addresses/1]).

hash_key(undefined, Divisor) ->
  hash_key(<<>>, Divisor);
hash_key(<<>>, Divisor) ->
  hash_key(crypto:strong_rand_bytes(8), Divisor);
hash_key(Key, Divisor) ->
  erlang:abs(crc32cer:nif(Key)) rem Divisor.


sock_address_to_string(Ip, Port) ->
  inet:ntoa(Ip) ++ ":" ++ integer_to_list(Port).


to_logical_address(Hostname, Port) ->
  maybe_prepend_scheme(Hostname ++ ":" ++ integer_to_list(Port)).

logical_to_physical_addresses(Address) when is_list(Address) ->
  case resolve_uri(list_to_binary(Address)) of
    {error, Reason} ->
      error({bad_address, Reason});
    {_, Addresses, Port, _} ->
      [{Addrs, Port} || Addrs <- Addresses]
  end.

resolve_uri(Uri) ->
  Uri1 = string:trim(binary_to_list(Uri)),
  Uri2 = maybe_prepend_scheme(Uri1),
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


maybe_prepend_scheme(Url) ->
  case string:str(Url, "//") of
    0 -> "pulsar://" ++ Url;
    _ -> Url
  end.


get_int_env(Param, Default) when is_integer(Default) ->
  erlwater_env:get_int_env(pulserl, Param, Default).


get_env(Param, Default) ->
  erlwater_env:get_env(pulserl, Param, Default).
