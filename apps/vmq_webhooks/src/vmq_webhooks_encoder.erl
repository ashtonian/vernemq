%% Copyright 2018-2024 Octavo Labs/VerneMQ (https://vernemq.com/)
%% and Individual Contributors.
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
-module(vmq_webhooks_encoder).

-export([
    encode/2,
    decode/2,
    content_type/1,
    is_valid/2
]).

-type format() :: json | msgpack.
-export_type([format/0]).

-spec encode(format(), list()) -> binary().
encode(json, Term) ->
    vmq_json:encode(Term);
encode(msgpack, Term) ->
    msgpack:pack(proplist_to_maps(Term), [{map_format, map}]).

-spec decode(format(), binary()) -> list().
decode(json, Body) ->
    vmq_json:decode(Body, [{labels, binary}, {return_maps, false}]);
decode(msgpack, Body) ->
    {ok, Decoded} = msgpack:unpack(Body, [{map_format, jsx}, {unpack_str, as_binary}]),
    Decoded.

-spec content_type(format()) -> binary().
content_type(json) -> <<"application/json">>;
content_type(msgpack) -> <<"application/x-msgpack">>.

-spec is_valid(format(), binary()) -> boolean().
is_valid(json, Body) -> vmq_json:is_json(Body);
is_valid(msgpack, _Body) -> true.

-spec proplist_to_maps(term()) -> term().
proplist_to_maps(L) when is_list(L) ->
    case is_proplist(L) of
        true -> maps:from_list([{key_to_bin(K), proplist_to_maps(V)} || {K, V} <- L]);
        false -> [proplist_to_maps(E) || E <- L]
    end;
proplist_to_maps(M) when is_map(M) ->
    maps:map(fun(_, V) -> proplist_to_maps(V) end, M);
proplist_to_maps(V) ->
    V.

-spec key_to_bin(atom() | binary()) -> binary().
key_to_bin(K) when is_atom(K) -> atom_to_binary(K, utf8);
key_to_bin(K) when is_binary(K) -> K.

-spec is_proplist(term()) -> boolean().
is_proplist([{_, _} | T]) -> is_proplist(T);
is_proplist([]) -> true;
is_proplist(_) -> false.
