-module(vmq_consistent_hash_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

all() ->
    [
        single_node_test,
        distribution_test,
        stability_test,
        removal_stability_test,
        lookup_deterministic_test,
        empty_ring_test
    ].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(_Case, Config) ->
    Config.

end_per_testcase(_Case, _Config) ->
    ok.

%% All keys map to the single node
single_node_test(_Config) ->
    Ring = vmq_consistent_hash:new([node_a], 256),
    ?assertEqual(1, length(vmq_consistent_hash:members(Ring))),
    lists:foreach(
        fun(I) ->
            ?assertEqual(node_a, vmq_consistent_hash:lookup({key, I}, Ring))
        end,
        lists:seq(1, 1000)
    ).

%% Keys distribute across all 5 nodes (none gets zero)
distribution_test(_Config) ->
    Nodes = [node_a, node_b, node_c, node_d, node_e],
    Ring = vmq_consistent_hash:new(Nodes, 256),
    ?assertEqual(5, length(vmq_consistent_hash:members(Ring))),
    ?assertEqual(5 * 256, vmq_consistent_hash:size(Ring)),
    Counts = count_distribution(Ring, 10000, Nodes),
    %% Every node must get at least some keys
    lists:foreach(
        fun(Node) ->
            Count = maps:get(Node, Counts, 0),
            ?assert(Count > 0)
        end,
        Nodes
    ).

%% Adding a 4th node to a 3-node ring: >60% of keys stay on the same node
stability_test(_Config) ->
    Nodes3 = [node_a, node_b, node_c],
    Nodes4 = [node_a, node_b, node_c, node_d],
    Ring3 = vmq_consistent_hash:new(Nodes3, 256),
    Ring4 = vmq_consistent_hash:new(Nodes4, 256),
    Keys = [{key, I} || I <- lists:seq(1, 10000)],
    Stable = lists:foldl(
        fun(Key, Acc) ->
            case vmq_consistent_hash:lookup(Key, Ring3) =:= vmq_consistent_hash:lookup(Key, Ring4) of
                true -> Acc + 1;
                false -> Acc
            end
        end,
        0,
        Keys
    ),
    StablePct = Stable / length(Keys),
    ct:pal("stability_test: ~.1f% of keys stable after adding node_d", [StablePct * 100]),
    ?assert(StablePct > 0.60).

%% Removing a node from a 4-node ring: >60% of keys stay on the same node
removal_stability_test(_Config) ->
    Nodes4 = [node_a, node_b, node_c, node_d],
    Nodes3 = [node_a, node_b, node_c],
    Ring4 = vmq_consistent_hash:new(Nodes4, 256),
    Ring3 = vmq_consistent_hash:new(Nodes3, 256),
    Keys = [{key, I} || I <- lists:seq(1, 10000)],
    Stable = lists:foldl(
        fun(Key, Acc) ->
            case vmq_consistent_hash:lookup(Key, Ring4) =:= vmq_consistent_hash:lookup(Key, Ring3) of
                true -> Acc + 1;
                false -> Acc
            end
        end,
        0,
        Keys
    ),
    StablePct = Stable / length(Keys),
    ct:pal("removal_stability_test: ~.1f% of keys stable after removing node_d", [StablePct * 100]),
    ?assert(StablePct > 0.60).

%% Same key always maps to same node for the same ring
lookup_deterministic_test(_Config) ->
    Nodes = [node_a, node_b, node_c],
    Ring = vmq_consistent_hash:new(Nodes, 256),
    lists:foreach(
        fun(I) ->
            Key = {key, I},
            Result1 = vmq_consistent_hash:lookup(Key, Ring),
            Result2 = vmq_consistent_hash:lookup(Key, Ring),
            ?assertEqual(Result1, Result2)
        end,
        lists:seq(1, 1000)
    ).

%% Empty nodes list produces empty ring, lookup crashes
empty_ring_test(_Config) ->
    Ring = vmq_consistent_hash:new([], 256),
    ?assertEqual([], vmq_consistent_hash:members(Ring)),
    ?assertEqual(0, vmq_consistent_hash:size(Ring)),
    ?assertError(empty_ring, vmq_consistent_hash:lookup(some_key, Ring)).

%%% Internal helpers

count_distribution(Ring, NumKeys, Nodes) ->
    Init = maps:from_list([{N, 0} || N <- Nodes]),
    lists:foldl(
        fun(I, Acc) ->
            Node = vmq_consistent_hash:lookup({key, I}, Ring),
            maps:update_with(Node, fun(V) -> V + 1 end, Acc)
        end,
        Init,
        lists:seq(1, NumKeys)
    ).
