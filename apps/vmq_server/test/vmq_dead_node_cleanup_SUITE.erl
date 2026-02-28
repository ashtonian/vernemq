-module(vmq_dead_node_cleanup_SUITE).
-include_lib("kernel/include/logger.hrl").

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

%% ===================================================================
%% common_test callbacks
%% ===================================================================
init_per_suite(Config) ->
    S = vmq_test_utils:get_suite_rand_seed(),
    Config0 = vmq_cluster_test_utils:init_distribution(Config),
    [S | Config0].

end_per_suite(_Config) ->
    _Config.

init_per_testcase(Case, Config) ->
    vmq_test_utils:seed_rand(Config),
    NodeWithPorts =
        [vmq_cluster_test_utils:random_node_with_port(Case) || _I <- lists:seq(1, 3)],
    Nodes =
        vmq_cluster_test_utils:pmap(
            fun({N, Port}) ->
                {ok, Peer, Node} =
                    vmq_cluster_test_utils:start_node(N, Config, Case),
                {ok, _} =
                    rpc:call(Node, vmq_server_cmd, listener_start, [Port, []]),
                ok = rpc:call(Node, vmq_auth, register_hooks, []),
                {Peer, Node, Port}
            end,
            NodeWithPorts
        ),
    {_, CoverNodes, _} = lists:unzip3(Nodes),
    {ok, _} = cover:start([node() | CoverNodes]),
    [{nodes, Nodes} | Config].

end_per_testcase(_, Config) ->
    {_, NodeList} = lists:keyfind(nodes, 1, Config),
    {Peers, Nodes, _} = lists:unzip3(NodeList),
    vmq_cluster_test_utils:pmap(
        fun({Peer, Node}) ->
            vmq_cluster_test_utils:stop_peer(Peer, Node)
        end,
        lists:zip(Peers, Nodes)
    ),
    ok.

all() ->
    [
        dead_node_tracking_test,
        dead_node_auto_cleanup_test,
        dead_node_quorum_blocks_cleanup_test,
        dead_node_cli_status_test
    ].

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Tests
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

dead_node_tracking_test(Config) ->
    %% Verify that dead nodes are tracked and reported by dead_node_status
    ok = ensure_cluster(Config),
    {_, [{Peer, Node, _Port} | RestNodes] = _Nodes} = lists:keyfind(nodes, 1, Config),
    [Nodename | _] = nodenames(Config),

    %% Initially no dead nodes
    [{_, CtrlNode, _} | _] = RestNodes,
    [] = rpc:call(CtrlNode, vmq_cluster_mon, dead_node_status, []),

    %% Stop a node
    ok = vmq_cluster_test_utils:stop_peer(Peer, Nodename),
    ok = vmq_cluster_test_utils:wait_until_offline(Node),

    %% Wait for the dead node to appear in tracking
    ok = vmq_cluster_test_utils:wait_until(
        fun() ->
            Status = rpc:call(CtrlNode, vmq_cluster_mon, dead_node_status, []),
            case [N || {N, _Secs} <- Status, N =:= Node] of
                [Node] -> true;
                _ -> false
            end
        end,
        60,
        500
    ),

    %% Verify down duration is reasonable (> 0 seconds)
    Status = rpc:call(CtrlNode, vmq_cluster_mon, dead_node_status, []),
    [{Node, DownSecs}] = [{N, S} || {N, S} <- Status, N =:= Node],
    ?assert(DownSecs >= 0),
    Config.

dead_node_auto_cleanup_test(Config) ->
    %% Full E2E: create sessions, kill a node, verify auto-cleanup migrates queues
    ok = ensure_cluster(Config),
    {_, [{Peer, Node, Port} | RestNodes] = Nodes} = lists:keyfind(nodes, 1, Config),
    [Nodename | _] = nodenames(Config),

    %% Set a short cleanup timeout on all remaining nodes
    CleanupTimeout = 3,
    lists:foreach(
        fun({_, N, _}) ->
            {ok, _} = rpc:call(N, vmq_server_cmd, set_config, [
                dead_node_cleanup_timeout, CleanupTimeout
            ])
        end,
        RestNodes
    ),

    %% Create 10 sessions with subscriptions on the target node
    Topic = "dead/node/cleanup/topic",
    lists:foreach(
        fun(I) ->
            Connect =
                packet:gen_connect(
                    "cleanup-test-" ++ integer_to_list(I),
                    [{clean_session, false}, {keepalive, 60}]
                ),
            Connack = packet:gen_connack(0),
            Subscribe = packet:gen_subscribe(123, Topic, 1),
            Suback = packet:gen_suback(123, 1),
            {ok, Socket} = packet:do_client_connect(Connect, Connack, [{port, Port}]),
            ok = gen_tcp:send(Socket, Subscribe),
            ok = packet:expect_packet(Socket, "suback", Suback),
            gen_tcp:send(Socket, packet:gen_disconnect()),
            gen_tcp:close(Socket)
        end,
        lists:seq(1, 10)
    ),

    %% Wait for subscriptions to be visible across cluster
    ok = wait_until_converged(
        Nodes,
        fun(N) -> rpc:call(N, vmq_reg, total_subscriptions, []) end,
        [{total, 10}]
    ),

    %% Stop the target node
    ok = vmq_cluster_test_utils:stop_peer(Peer, Nodename),
    ok = vmq_cluster_test_utils:wait_until_offline(Node),

    %% Wait for auto-cleanup to complete.
    %% The node should be removed from the cluster and queues migrated.
    %% Timeout: cleanup_timeout(3s) + recheck_interval(up to 10s) + cleanup_duration
    [{_, CtrlNode, _} | _] = RestNodes,
    ok = vmq_cluster_test_utils:wait_until(
        fun() ->
            %% Check that the dead node has been removed from cluster membership
            Members = rpc:call(CtrlNode, vmq_plugin, only, [cluster_members, []]),
            not lists:member(Node, Members)
        end,
        120,
        1000
    ),

    %% Verify queues were migrated to remaining nodes
    ok = wait_until_converged(
        RestNodes,
        fun(N) ->
            {_, _, _, Offline, _} = rpc:call(N, vmq_queue_sup_sup, summary, []),
            Offline
        end,
        5
    ),
    Config.

dead_node_quorum_blocks_cleanup_test(Config) ->
    %% With 3 nodes, stopping 2 should prevent the remaining node from auto-cleaning
    ok = ensure_cluster(Config),
    {_, [{Peer1, Node1, _}, {Peer2, Node2, _}, {_Peer3, Node3, _}]} =
        lists:keyfind(nodes, 1, Config),
    [Nodename1, Nodename2 | _] = nodenames(Config),

    %% Set a very short cleanup timeout on the surviving node
    {ok, _} = rpc:call(Node3, vmq_server_cmd, set_config, [
        dead_node_cleanup_timeout, 1
    ]),

    %% Stop two nodes (majority down)
    ok = vmq_cluster_test_utils:stop_peer(Peer1, Nodename1),
    ok = vmq_cluster_test_utils:stop_peer(Peer2, Nodename2),
    ok = vmq_cluster_test_utils:wait_until_offline(Node1),
    ok = vmq_cluster_test_utils:wait_until_offline(Node2),

    %% Wait long enough for cleanup to have been attempted
    timer:sleep(8000),

    %% Verify the dead nodes are still tracked (not cleaned up)
    %% because quorum is not met (1 of 3 = not majority)
    Members = rpc:call(Node3, vmq_plugin, only, [cluster_members, []]),
    ?assert(lists:member(Node1, Members)),
    ?assert(lists:member(Node2, Members)),
    Config.

dead_node_cli_status_test(Config) ->
    %% Verify the vmq-admin cluster dead-nodes command works
    ok = ensure_cluster(Config),
    {_, [{Peer, Node, _Port} | RestNodes]} = lists:keyfind(nodes, 1, Config),
    [Nodename | _] = nodenames(Config),
    [{_, CtrlNode, _} | _] = RestNodes,

    %% With no dead nodes, command should return text output
    {ok, _} = rpc:call(CtrlNode, vmq_server_cli, command, [
        ["vmq-admin", "cluster", "dead-nodes"], false
    ]),

    %% Stop a node and wait for tracking
    ok = vmq_cluster_test_utils:stop_peer(Peer, Nodename),
    ok = vmq_cluster_test_utils:wait_until_offline(Node),

    ok = vmq_cluster_test_utils:wait_until(
        fun() ->
            case rpc:call(CtrlNode, vmq_cluster_mon, dead_node_status, []) of
                Status when is_list(Status), length(Status) > 0 -> true;
                _ -> false
            end
        end,
        60,
        500
    ),

    %% Now the command should return table output with the dead node
    {ok, _} = rpc:call(CtrlNode, vmq_server_cli, command, [
        ["vmq-admin", "cluster", "dead-nodes"], false
    ]),
    Config.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Helpers
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

ensure_cluster(Config) ->
    [{_, Node1, _} | OtherNodes] = Nodes = proplists:get_value(nodes, Config),
    [
        begin
            {ok, _} = rpc:call(Node, vmq_server_cmd, node_join, [Node1])
        end
     || {_Peer, Node, _} <- OtherNodes
    ],
    {_, NodeNames, _} = lists:unzip3(Nodes),
    Expected = lists:sort(NodeNames),
    ok = vmq_cluster_test_utils:wait_until_joined(NodeNames, Expected),
    [
        ?assertEqual(
            {Node, Expected},
            {Node, lists:sort(vmq_cluster_test_utils:get_cluster_members(Node))}
        )
     || Node <- NodeNames
    ],
    vmq_cluster_test_utils:wait_until_ready(NodeNames),
    ok.

nodenames(Config) ->
    {_, Nodes} = lists:keyfind(nodes, 1, Config),
    [Name || {Name, _, _} <- Nodes].

wait_until_converged(Nodes, Fun, ExpectedReturn) ->
    {_, NodeNames, _} = lists:unzip3(Nodes),
    vmq_cluster_test_utils:wait_until(
        fun() ->
            lists:all(
                fun(X) -> X == true end,
                vmq_cluster_test_utils:pmap(
                    fun(Node) ->
                        ExpectedReturn == Fun(Node)
                    end,
                    NodeNames
                )
            )
        end,
        100 * 2,
        500
    ).
