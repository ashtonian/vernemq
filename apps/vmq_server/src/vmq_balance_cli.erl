%% Copyright 2024 Octavo Labs/VerneMQ (https://vernemq.com/)
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

-module(vmq_balance_cli).
-export([register_cli/0]).

register_cli() ->
    clique:register_usage(["vmq-admin", "balance"], balance_usage()),
    clique:register_usage(["vmq-admin", "balance", "show"], balance_show_usage()),
    clique:register_usage(["vmq-admin", "balance", "status"], balance_status_usage()),
    clique:register_usage(["vmq-admin", "balance", "rebalance"], balance_rebalance_usage()),

    balance_show_cmd(),
    balance_status_cmd(),
    balance_rebalance_cmd().

%%%===================================================================
%%% Commands
%%%===================================================================

balance_show_cmd() ->
    Cmd = ["vmq-admin", "balance", "show"],
    Callback = fun(_, _, _) ->
        NodeCounts = vmq_balance_srv:get_node_counts(),
        case map_size(NodeCounts) of
            0 ->
                [clique_status:text("No node data available.")];
            _ ->
                TotalConns = maps:fold(fun(_N, C, Acc) -> Acc + C end, 0, NodeCounts),
                NumNodes = map_size(NodeCounts),
                Avg = TotalConns / NumNodes,
                Table = maps:fold(
                    fun(Node, Count, Acc) ->
                        Ratio =
                            case Avg == 0.0 of
                                true -> 0.0;
                                false -> Count / Avg
                            end,
                        [
                            [
                                {'Node', Node},
                                {'Connections', Count},
                                {'Ratio', io_lib:format("~.2f", [Ratio])}
                            ]
                            | Acc
                        ]
                    end,
                    [],
                    NodeCounts
                ),
                Summary = io_lib:format(
                    "Total: ~p connections across ~p nodes (avg: ~.1f)",
                    [TotalConns, NumNodes, Avg]
                ),
                [clique_status:table(Table), clique_status:text(Summary)]
        end
    end,
    clique:register_command(Cmd, [], [], Callback).

balance_status_cmd() ->
    Cmd = ["vmq-admin", "balance", "status"],
    Callback = fun(_, _, _) ->
        {IsAccepting, LocalConns, ClusterAvg, IsEnabled, Rejections} =
            vmq_balance_srv:balance_stats(),
        BalanceEnabled = vmq_config:get_env(balance_enabled, false),
        BalanceRejectEnabled = vmq_config:get_env(balance_reject_enabled, false),
        BalanceThreshold = vmq_config:get_env(balance_threshold, "1.2"),
        BalanceHysteresis = vmq_config:get_env(balance_hysteresis, "0.1"),
        BalanceMinConns = vmq_config:get_env(balance_min_connections, 100),
        BalanceCheckInterval = vmq_config:get_env(balance_check_interval, 5000),
        RebalanceEnabled = vmq_config:get_env(rebalance_enabled, false),
        RebalanceThreshold = vmq_config:get_env(rebalance_threshold, "1.3"),
        RebalanceBatchSize = vmq_config:get_env(rebalance_batch_size, 50),
        RebalanceCooldown = vmq_config:get_env(rebalance_cooldown, 30),
        RebalanceOnNodeJoin = vmq_config:get_env(rebalance_on_node_join, false),
        RebalanceStableInterval = vmq_config:get_env(rebalance_stable_interval, 60),
        RebalanceAutoInterval = vmq_config:get_env(rebalance_auto_interval, 0),
        {RebalanceDisconnections, RebalanceRounds} =
            try
                vmq_balance_rebalancer:rebalance_stats()
            catch
                _:_ -> {0, 0}
            end,
        Table = [
            [{'Setting', "status"}, {'Value', status_str(IsAccepting)}],
            [{'Setting', "local_connections"}, {'Value', LocalConns}],
            [{'Setting', "cluster_avg"}, {'Value', ClusterAvg}],
            [{'Setting', "rejections"}, {'Value', Rejections}],
            [{'Setting', "balance_enabled"}, {'Value', BalanceEnabled}],
            [{'Setting', "balance_reject_enabled"}, {'Value', BalanceRejectEnabled}],
            [{'Setting', "balance_threshold"}, {'Value', BalanceThreshold}],
            [{'Setting', "balance_hysteresis"}, {'Value', BalanceHysteresis}],
            [{'Setting', "balance_min_connections"}, {'Value', BalanceMinConns}],
            [{'Setting', "balance_check_interval"}, {'Value', BalanceCheckInterval}],
            [{'Setting', "balance_is_enabled"}, {'Value', IsEnabled}],
            [{'Setting', "rebalance_enabled"}, {'Value', RebalanceEnabled}],
            [{'Setting', "rebalance_threshold"}, {'Value', RebalanceThreshold}],
            [{'Setting', "rebalance_batch_size"}, {'Value', RebalanceBatchSize}],
            [{'Setting', "rebalance_cooldown"}, {'Value', RebalanceCooldown}],
            [{'Setting', "rebalance_on_node_join"}, {'Value', RebalanceOnNodeJoin}],
            [{'Setting', "rebalance_stable_interval"}, {'Value', RebalanceStableInterval}],
            [{'Setting', "rebalance_auto_interval"}, {'Value', RebalanceAutoInterval}],
            [{'Setting', "rebalance_disconnections"}, {'Value', RebalanceDisconnections}],
            [{'Setting', "rebalance_rounds"}, {'Value', RebalanceRounds}]
        ],
        [clique_status:table(Table)]
    end,
    clique:register_command(Cmd, [], [], Callback).

balance_rebalance_cmd() ->
    Cmd = ["vmq-admin", "balance", "rebalance"],
    FlagSpecs = [
        {force, [
            {longname, "force"},
            {shortname, "f"}
        ]}
    ],
    Callback = fun(_, _, Flags) ->
        Force = lists:keymember(force, 1, Flags),
        case vmq_balance_rebalancer:trigger_rebalance(Force) of
            {ok, #{disconnected := Disconnected, rounds := Rounds}} ->
                Text = io_lib:format(
                    "Rebalance complete: ~p sessions disconnected in ~p rounds.",
                    [Disconnected, Rounds]
                ),
                [clique_status:text(Text)];
            {error, disabled} ->
                [
                    clique_status:alert([
                        clique_status:text(
                            "Rebalance failed: balance_enabled and rebalance_enabled must both be on."
                        )
                    ])
                ];
            {error, already_running} ->
                [
                    clique_status:alert([
                        clique_status:text("Rebalance already in progress.")
                    ])
                ];
            {error, cluster_unstable} ->
                [
                    clique_status:alert([
                        clique_status:text(
                            "Cluster is not stable long enough. Use --force to bypass."
                        )
                    ])
                ];
            {error, Reason} ->
                Text = io_lib:format("Rebalance failed: ~p", [Reason]),
                [clique_status:alert([clique_status:text(Text)])]
        end
    end,
    clique:register_command(Cmd, [], FlagSpecs, Callback).

%%%===================================================================
%%% Usage
%%%===================================================================

balance_usage() ->
    [
        "vmq-admin balance <sub-command>\n\n",
        "  Manage cluster connection auto-balancing.\n\n",
        "  Sub-commands:\n",
        "    show        Show per-node connection distribution\n",
        "    status      Show balance and rebalance configuration/status\n",
        "    rebalance   Trigger manual session rebalance\n\n",
        "  Use --help after a sub-command for more details.\n"
    ].

balance_show_usage() ->
    [
        "vmq-admin balance show\n\n",
        "  Displays a table of per-node connection counts, ratio to cluster\n",
        "  average, and a summary with total and average.\n"
    ].

balance_status_usage() ->
    [
        "vmq-admin balance status\n\n",
        "  Shows the current balance status (accepting/rejecting) and all\n",
        "  balance and rebalance configuration values for the local node.\n"
    ].

balance_rebalance_usage() ->
    [
        "vmq-admin balance rebalance [--force]\n\n",
        "  Triggers a manual rebalance of sessions on this node. If the node\n",
        "  has more connections than the cluster average * rebalance_threshold,\n",
        "  excess sessions are gracefully disconnected so they can reconnect\n",
        "  through the load balancer to underloaded nodes.\n\n",
        "Options\n\n",
        "  --force, -f\n",
        "      Bypass the cluster stability check and rebalance immediately.\n"
    ].

%%%===================================================================
%%% Internal
%%%===================================================================

status_str(1) -> "accepting";
status_str(0) -> "rejecting";
status_str(_) -> "unknown".
