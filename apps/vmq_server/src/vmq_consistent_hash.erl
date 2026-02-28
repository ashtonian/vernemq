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

%%% @doc Consistent hash ring for distributing keys across cluster nodes.
%%%
%%% Used by vmq_reg_sync to select which node coordinates a given sync key.
%%% Replaces modulo-based hashing which remapped nearly all keys on membership
%%% changes. With 256 virtual nodes per physical node, only ~1/N of keys move
%%% when a node joins or leaves.
%%%
%%% Tradeoffs and limitations:
%%% - The ring is cached in persistent_term. Writes to persistent_term trigger
%%%   a global GC across all processes. This is acceptable because membership
%%%   changes are rare operational events (manual joins/leaves).
%%% - The ring is rebuilt lazily on the next sync_node/1 call after a membership
%%%   change. Multiple concurrent callers may rebuild simultaneously during a
%%%   transition; this is harmless since the result is idempotent.
%%% - The stale check compares vmq_cluster:nodes() (an ETS scan) against the
%%%   cached node list on every sync_node/1 call. For a 20-node cluster this
%%%   is a comparison of two 20-element sorted atom lists — negligible overhead.
%%% - 256 virtual nodes per physical node gives good distribution balance but is
%%%   not configurable. At 20 physical nodes this produces a 5120-entry ring
%%%   requiring ~13 binary search comparisons per lookup.
-module(vmq_consistent_hash).

-export([new/2, lookup/2, members/1, size/1]).

-type ring() :: {array, tuple()}.

%% @doc Build a consistent hash ring from a list of nodes.
%% Each node gets VNodesPerNode virtual nodes on the ring.
%% Returns a ring term suitable for lookup/2.
-spec new([node()], pos_integer()) -> ring().
new([], _VNodesPerNode) ->
    {array, {}};
new(Nodes, VNodesPerNode) ->
    Entries =
        lists:flatmap(
            fun(Node) ->
                [{erlang:phash2({Node, I}), Node} || I <- lists:seq(1, VNodesPerNode)]
            end,
            Nodes
        ),
    Sorted = lists:keysort(1, Entries),
    {array, list_to_tuple(Sorted)}.

%% @doc Find the node responsible for a given key on the ring.
%% Uses binary search to find the first virtual node with hash >= Hash.
%% Wraps around to the first entry if no such node exists.
-spec lookup(any(), ring()) -> node().
lookup(_Key, {array, Ring}) when tuple_size(Ring) =:= 0 ->
    error(empty_ring);
lookup(Key, {array, Ring}) ->
    Hash = erlang:phash2(Key),
    Size = tuple_size(Ring),
    case bsearch(Ring, Hash, 1, Size) of
        0 ->
            %% Wrap around: key hash is larger than all ring entries
            {_, Node} = element(1, Ring),
            Node;
        Idx ->
            {_, Node} = element(Idx, Ring),
            Node
    end.

%% @doc Return the unique set of physical nodes in the ring.
-spec members(ring()) -> [node()].
members({array, Ring}) ->
    lists:usort([Node || {_, Node} <- tuple_to_list(Ring)]).

%% @doc Return the number of virtual nodes in the ring.
-spec size(ring()) -> non_neg_integer().
size({array, Ring}) ->
    tuple_size(Ring).

%%% Internal functions

%% Binary search: find the index of the first entry with hash >= Hash.
%% Returns 0 if no such entry exists (all entries have hash < Hash).
bsearch(_Ring, _Hash, Low, High) when Low > High ->
    0;
bsearch(Ring, Hash, Low, High) ->
    Mid = (Low + High) div 2,
    {MidHash, _} = element(Mid, Ring),
    if
        MidHash >= Hash andalso (Mid =:= 1 orelse element(1, element(Mid - 1, Ring)) < Hash) ->
            Mid;
        MidHash < Hash ->
            bsearch(Ring, Hash, Mid + 1, High);
        true ->
            bsearch(Ring, Hash, Low, Mid - 1)
    end.
