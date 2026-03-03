-module(vmq_cluster_tier_SUITE).

%% Unit tests for the tiered cluster readiness pure functions:
%% compute_tier/2, compute_transition/2, migrate_old_format/1.

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-export([
    all/0,
    init_per_suite/1,
    end_per_suite/1
]).

-export([
    compute_tier_all_alive_test/1,
    compute_tier_empty_list_test/1,
    compute_tier_one_down_majority_quorum_test/1,
    compute_tier_two_down_majority_quorum_test/1,
    compute_tier_default_quorum_test/1,
    compute_tier_single_node_alive_test/1,
    compute_tier_single_node_down_test/1,
    compute_tier_exact_boundary_test/1,
    compute_tier_all_down_test/1,
    migrate_old_format_5tuple_test/1,
    migrate_old_format_3tuple_true_test/1,
    migrate_old_format_3tuple_false_test/1,
    migrate_old_format_unexpected_test/1,
    transition_healthy_to_degraded_test/1,
    transition_healthy_to_partitioned_test/1,
    transition_degraded_to_healthy_test/1,
    transition_degraded_to_partitioned_test/1,
    transition_partitioned_to_healthy_test/1,
    transition_partitioned_to_degraded_test/1,
    transition_same_state_test/1
]).

all() ->
    [
        compute_tier_all_alive_test,
        compute_tier_empty_list_test,
        compute_tier_one_down_majority_quorum_test,
        compute_tier_two_down_majority_quorum_test,
        compute_tier_default_quorum_test,
        compute_tier_single_node_alive_test,
        compute_tier_single_node_down_test,
        compute_tier_exact_boundary_test,
        compute_tier_all_down_test,
        migrate_old_format_5tuple_test,
        migrate_old_format_3tuple_true_test,
        migrate_old_format_3tuple_false_test,
        migrate_old_format_unexpected_test,
        transition_healthy_to_degraded_test,
        transition_healthy_to_partitioned_test,
        transition_degraded_to_healthy_test,
        transition_degraded_to_partitioned_test,
        transition_partitioned_to_healthy_test,
        transition_partitioned_to_degraded_test,
        transition_same_state_test
    ].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

%%% ===================================================================
%%% compute_tier/2 tests
%%% ===================================================================

compute_tier_all_alive_test(_Config) ->
    Nodes = [{a, true}, {b, true}, {c, true}],
    ?assertEqual(healthy, vmq_cluster:compute_tier(Nodes, 0.5)),
    ?assertEqual(healthy, vmq_cluster:compute_tier(Nodes, 1.0)).

compute_tier_empty_list_test(_Config) ->
    ?assertEqual(healthy, vmq_cluster:compute_tier([], 1.0)),
    ?assertEqual(healthy, vmq_cluster:compute_tier([], 0.5)).

compute_tier_one_down_majority_quorum_test(_Config) ->
    %% 3 nodes, 1 down, quorum=0.5: 2/3=0.66 >= 0.5 -> degraded
    Nodes = [{a, true}, {b, true}, {c, false}],
    ?assertEqual(degraded, vmq_cluster:compute_tier(Nodes, 0.5)).

compute_tier_two_down_majority_quorum_test(_Config) ->
    %% 3 nodes, 2 down, quorum=0.5: 1/3=0.33 < 0.5 -> partitioned
    Nodes = [{a, true}, {b, false}, {c, false}],
    ?assertEqual(partitioned, vmq_cluster:compute_tier(Nodes, 0.5)).

compute_tier_default_quorum_test(_Config) ->
    %% With default quorum=1.0, any node down -> partitioned
    Nodes = [{a, true}, {b, true}, {c, false}],
    ?assertEqual(partitioned, vmq_cluster:compute_tier(Nodes, 1.0)).

compute_tier_single_node_alive_test(_Config) ->
    ?assertEqual(healthy, vmq_cluster:compute_tier([{a, true}], 1.0)),
    ?assertEqual(healthy, vmq_cluster:compute_tier([{a, true}], 0.5)).

compute_tier_single_node_down_test(_Config) ->
    ?assertEqual(partitioned, vmq_cluster:compute_tier([{a, false}], 1.0)),
    ?assertEqual(partitioned, vmq_cluster:compute_tier([{a, false}], 0.5)).

%% Test exact boundary: 2/4 alive with quorum=0.5 is exactly at threshold.
%% 2*100 >= round(0.5*100)*4 -> 200 >= 200 -> true -> degraded (not partitioned).
compute_tier_exact_boundary_test(_Config) ->
    Nodes = [{a, true}, {b, true}, {c, false}, {d, false}],
    %% Exactly at 50% threshold -> degraded (>= means inclusive)
    ?assertEqual(degraded, vmq_cluster:compute_tier(Nodes, 0.5)),
    %% Just below: 1/4 alive with quorum=0.5 -> 100 >= 200 -> false -> partitioned
    NodesBelow = [{a, true}, {b, false}, {c, false}, {d, false}],
    ?assertEqual(partitioned, vmq_cluster:compute_tier(NodesBelow, 0.5)).

%% All nodes down in a multi-node cluster -> partitioned
compute_tier_all_down_test(_Config) ->
    Nodes = [{a, false}, {b, false}, {c, false}],
    ?assertEqual(partitioned, vmq_cluster:compute_tier(Nodes, 0.5)),
    ?assertEqual(partitioned, vmq_cluster:compute_tier(Nodes, 1.0)).

%%% ===================================================================
%%% migrate_old_format/1 tests
%%% ===================================================================

migrate_old_format_5tuple_test(_Config) ->
    Input = {degraded, 1, 2, 3, 4},
    ?assertEqual({degraded, 1, 2, 3, 4}, vmq_cluster:migrate_old_format(Input)).

migrate_old_format_3tuple_true_test(_Config) ->
    ?assertEqual({healthy, 5, 3, 0, 0}, vmq_cluster:migrate_old_format({true, 5, 3})).

migrate_old_format_3tuple_false_test(_Config) ->
    ?assertEqual({partitioned, 2, 1, 0, 0}, vmq_cluster:migrate_old_format({false, 2, 1})).

migrate_old_format_unexpected_test(_Config) ->
    %% Unexpected format should return safe default
    ?assertEqual({healthy, 0, 0, 0, 0}, vmq_cluster:migrate_old_format({garbage})),
    ?assertEqual({healthy, 0, 0, 0, 0}, vmq_cluster:migrate_old_format(undefined)).

%%% ===================================================================
%%% compute_transition/2 tests
%%% ===================================================================

transition_healthy_to_degraded_test(_Config) ->
    Old = {healthy, 0, 0, 0, 0},
    Result = vmq_cluster:compute_transition(degraded, Old),
    ?assertEqual({degraded, 0, 0, 1, 0}, Result).

transition_healthy_to_partitioned_test(_Config) ->
    Old = {healthy, 0, 0, 0, 0},
    Result = vmq_cluster:compute_transition(partitioned, Old),
    %% Netsplit detected +1, no degraded change
    ?assertEqual({partitioned, 1, 0, 0, 0}, Result).

transition_degraded_to_healthy_test(_Config) ->
    Old = {degraded, 0, 0, 1, 0},
    Result = vmq_cluster:compute_transition(healthy, Old),
    %% Degraded resolved +1
    ?assertEqual({healthy, 0, 0, 1, 1}, Result).

transition_degraded_to_partitioned_test(_Config) ->
    Old = {degraded, 0, 0, 1, 0},
    Result = vmq_cluster:compute_transition(partitioned, Old),
    %% Netsplit detected +1, degraded resolved +1
    ?assertEqual({partitioned, 1, 0, 1, 1}, Result).

transition_partitioned_to_healthy_test(_Config) ->
    Old = {partitioned, 1, 0, 0, 0},
    Result = vmq_cluster:compute_transition(healthy, Old),
    %% Netsplit resolved +1
    ?assertEqual({healthy, 1, 1, 0, 0}, Result).

transition_partitioned_to_degraded_test(_Config) ->
    Old = {partitioned, 1, 0, 0, 0},
    Result = vmq_cluster:compute_transition(degraded, Old),
    %% Netsplit resolved +1, degraded detected +1
    ?assertEqual({degraded, 1, 1, 1, 0}, Result).

transition_same_state_test(_Config) ->
    %% No counter changes when state stays the same
    Old = {healthy, 5, 3, 2, 1},
    ?assertEqual({healthy, 5, 3, 2, 1}, vmq_cluster:compute_transition(healthy, Old)),

    Old2 = {degraded, 5, 3, 2, 1},
    ?assertEqual({degraded, 5, 3, 2, 1}, vmq_cluster:compute_transition(degraded, Old2)),

    Old3 = {partitioned, 5, 3, 2, 1},
    ?assertEqual({partitioned, 5, 3, 2, 1}, vmq_cluster:compute_transition(partitioned, Old3)).
