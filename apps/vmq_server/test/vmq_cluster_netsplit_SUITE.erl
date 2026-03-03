-module(vmq_cluster_netsplit_SUITE).
-include_lib("kernel/include/logger.hrl").

-export([
         %% suite/0,
         init_per_suite/1,
         end_per_suite/1,
         init_per_testcase/2,
         end_per_testcase/2,
         all/0
        ]).

-export([publish_qos0_test/1,
         register_consistency_test/1,
         register_consistency_multiple_sessions_test/1,
         register_not_ready_test/1,
         remote_enqueue_cant_block_the_publisher/1,
         tiered_readiness_degraded_test/1,
         tiered_readiness_partitioned_test/1,
         tiered_readiness_cap_override_test/1,
         tiered_readiness_transition_counters_test/1,
         tiered_readiness_recovery_test/1,
         tiered_readiness_pubsub_during_degraded_test/1,
         tiered_health_http_tiered_test/1
        ]).

-include_lib("eunit/include/eunit.hrl").

%% ===================================================================
%% common_test callbacks
%% ===================================================================
init_per_suite(Config) ->
    S = vmq_test_utils:get_suite_rand_seed(),
    Config0 = vmq_cluster_test_utils:init_distribution(Config),
    ?LOG_INFO("node name ~p", [node()]),
    [S | Config0].

end_per_suite(_Config) ->
    _Config.

init_per_testcase(Case, Config) ->
    vmq_test_utils:seed_rand(Config),
    set_config(metadata_plugin, vmq_swc),

    NodeWithPorts =
        [vmq_cluster_test_utils:random_node_with_port(Case) || _I <- lists:seq(1, 3)],
    Nodes =
        vmq_cluster_test_utils:pmap(fun({N, Port}) ->
                                       {ok, Peer, Node} =
                                           vmq_cluster_test_utils:start_node(N, Config, Case),
                                       {ok, _} =
                                           rpc:call(Node,
                                                    vmq_server_cmd,
                                                    listener_start,
                                                    [Port, []]),
                                       %% allow all
                                       ok = rpc:call(Node, vmq_auth, register_hooks, []),
                                       {Peer, Node, Port}
                                    end,
                                    NodeWithPorts),
    {_, CoverNodes, _} = lists:unzip3(Nodes),
    {ok, _} = ct_cover:add_nodes(CoverNodes),
    [{nodes, Nodes} | Config].

end_per_testcase(_, Config) ->
    {_, NodeList} = lists:keyfind(nodes, 1, Config),
    {Peers, Nodes, _} = lists:unzip3(NodeList),
    vmq_cluster_test_utils:pmap(fun({Peer, Node}) ->
                                   ok = vmq_cluster_test_utils:stop_peer(Peer, Node)
                                end,
                                lists:zip(Peers, Nodes)),
    ok.

all() ->
    [publish_qos0_test,
     register_consistency_test,
     register_consistency_multiple_sessions_test,
     register_not_ready_test,
     remote_enqueue_cant_block_the_publisher,
     tiered_readiness_degraded_test,
     tiered_readiness_partitioned_test,
     tiered_readiness_cap_override_test,
     tiered_readiness_transition_counters_test,
     tiered_readiness_recovery_test,
     tiered_readiness_pubsub_during_degraded_test,
     tiered_health_http_tiered_test].


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Actual Tests
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
set_config(Key, Val) ->
    rpc:multicall(vmq_server_cmd, set_config, [Key, Val]),
    ok.

register_consistency_test(Config) ->
    ok = ensure_cluster(Config),
    set_config(allow_register_during_netsplit, false),
    {_, Nodes} = lists:keyfind(nodes, 1, Config),
    {Island1, Island2} = lists:split(length(Nodes) div 2, Nodes),

    %% Create Partitions
    {_, Island1Names, _} = lists:unzip3(Island1),
    {_, Island2Names, _} = lists:unzip3(Island2),
    vmq_cluster_test_utils:partition_cluster(Island1Names, Island2Names),

    ok = wait_until_converged(Nodes,
                         fun(N) ->
                                 {rpc:call(N, vmq_cluster, netsplit_statistics, []),
                                  rpc:call(N, vmq_cluster, is_ready, [])}
                         end, {{1, 0}, false}),

    {_, _, Island1Port} = random_node(Island1),
    {_, _, Island2Port} = random_node(Island2),
    ct:sleep(10000),
    Connect = packet:gen_connect("test-client", [{clean_session, true},
                                                 {keepalive, 10}]),
    %% Island 1 should return us the proper CONNACK(3)
    {ok, _} = packet:do_client_connect(Connect, packet:gen_connack(3),
                                       [{port, Island1Port}]),
    %% Island 2 should return us the proper CONACK(3)
    {ok, _} = packet:do_client_connect(Connect, packet:gen_connack(3),
                                       [{port, Island2Port}]),
    vmq_cluster_test_utils:heal_cluster(Island1Names, Island2Names),

    ok = wait_until_converged(Nodes,
                         fun(N) ->
                                 {rpc:call(N, vmq_cluster, netsplit_statistics, []),
                                  rpc:call(N, vmq_cluster, is_ready, [])}
                         end, {{1, 1}, true}),
    ok.

register_consistency_multiple_sessions_test(Config) ->
    ok = ensure_cluster(Config),
    {_, Nodes} = lists:keyfind(nodes, 1, Config),
    {Island1, Island2} = lists:split(length(Nodes) div 2, Nodes),

    %% we configure the nodes to trade consistency for availability
    set_config(allow_register_during_netsplit, true),

    %% Create Partitions
    {_, Island1Names, _} = lists:unzip3(Island1),
    {_, Island2Names, _} = lists:unzip3(Island2),
    vmq_cluster_test_utils:partition_cluster(Island1Names, Island2Names),

    {_, _, Island1Port} = random_node(Island1),
    {_, _, Island2Port} = random_node(Island2),

    Connect = packet:gen_connect("test-client-multiple", [{clean_session, true},
                                                 {keepalive, 10}]),
    Connack = packet:gen_connack(0),
    {ok, Socket1} = packet:do_client_connect(Connect, Connack,
                                             [{port, Island1Port}]),

    {ok, Socket2} = packet:do_client_connect(Connect, Connack,
                                               [{port, Island2Port}]),
    vmq_cluster_test_utils:heal_cluster(Island1Names, Island2Names),
    gen_tcp:close(Socket1),
    gen_tcp:close(Socket2),
    ok.

register_not_ready_test(Config) ->
    ok = ensure_cluster(Config),
    set_config(allow_register_during_netsplit, false),
    {_, Nodes} = lists:keyfind(nodes, 1, Config),
    {Island1, Island2} = lists:split(length(Nodes) div 2, Nodes),

    %% Connect a test-client
    Connect = packet:gen_connect("test-client-not-ready", [{clean_session, true},
                                                 {keepalive, 10}]),
    Connack = packet:gen_connack(0),
    {_, _, Port} = random_node(Nodes),
    {ok, _Socket} = packet:do_client_connect(Connect, Connack,
                                             [{port, Port}]),

    %% Create Partitions
    {_, Island1Names, _} = lists:unzip3(Island1),
    {_, Island2Names, _} = lists:unzip3(Island2),
    vmq_cluster_test_utils:partition_cluster(Island1Names, Island2Names),

    ok = wait_until_converged(Nodes,
                         fun(N) ->
                                 rpc:call(N, vmq_cluster, is_ready, [])
                         end, false),

    %% we are now on a partitioned network and SHOULD NOT allow new connections
    ConnNack = packet:gen_connack(3), %% server unavailable
    [begin
         {ok, S} = packet:do_client_connect(Connect, ConnNack, [{port, P}]),
         gen_tcp:close(S)
     end || {_, _, P} <- Nodes],

    %% fix cables
    vmq_cluster_test_utils:heal_cluster(Island1Names, Island2Names),

    ok = wait_until_converged(Nodes,
                         fun(N) ->
                                 rpc:call(N, vmq_cluster, is_ready, [])
                         end, true),

    %% connect MUST go through now.
    [begin
         {ok, S} = packet:do_client_connect(Connect, Connack, [{port, P}]),
         gen_tcp:close(S)
     end || {_, _, P} <- Nodes],
    ok.

publish_qos0_test(Config) ->
    ok = ensure_cluster(Config),
    {_, Nodes} = lists:keyfind(nodes, 1, Config),
    {Island1, Island2} = lists:split(length(Nodes) div 2, Nodes),
    Connect = packet:gen_connect("test-netsplit-client", [{clean_session, false},
                                                          {keepalive, 60}]),
    Connack = packet:gen_connack(0),
    Subscribe = packet:gen_subscribe(53, "netsplit/0/test", 0),
    Suback = packet:gen_suback(53, 0),
    {_, _, Island1Port} = random_node(Island1),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, [{port,
                                                                Island1Port}]),
    ok = gen_tcp:send(Socket, Subscribe),
    ok = packet:expect_packet(Socket, "suback", Suback),
    ok = wait_until_converged(Nodes,
                         fun(N) ->
                                 rpc:call(N, vmq_reg, total_subscriptions, [])
                         end, [{total, 1}]),

    %% Create Partitions
    {_, Island1Names, _} = lists:unzip3(Island1),
    {_, Island2Names, _} = lists:unzip3(Island2),
    vmq_cluster_test_utils:partition_cluster(Island1Names, Island2Names),

    ok = wait_until_converged(Nodes,
                         fun(N) ->
                                 rpc:call(N, vmq_cluster, is_ready, [])
                         end, false),

    {_, _, Island2Port} = random_node(Island2),
    set_config(allow_register_during_netsplit, true),
    set_config(allow_publish_during_netsplit, true),
    Publish = packet:gen_publish("netsplit/0/test", 0, <<"message">>,
                                 [{mid, 1}]),
    helper_pub_qos1("test-netsplit-sender", Publish, Island2Port),

    %% fix the network
    vmq_cluster_test_utils:heal_cluster(Island1Names, Island2Names),

    %% the publish is expected once the netsplit is fixed
    ok = packet:expect_packet(Socket, "publish", Publish).

remote_enqueue_cant_block_the_publisher(Config) ->
    ok = ensure_cluster(Config),

    set_config(allow_publish_during_netsplit, true),
    set_config(remote_enqueue_timeout, 500),

    %% Partition the nodenames into two sets
    {_, Nodes} = lists:keyfind(nodes, 1, Config),
    {Island1, Island2} = lists:split(length(Nodes) div 2, Nodes),

    %% Connect shared subscriber to the other side of the cluster
    %% (publishes to a shared subscriber are delivered using
    %% the enqueue_remote function in the vmq_cluster mod).
    ConnectSub = packet:gen_connect("netsplit-shared-sub", [{clean_session, true},
                                                            {keepalive, 120}]),
    Connack = packet:gen_connack(0),
    {_, _, PortSub} = random_node(Island1),
    {ok, SubSocket} = packet:do_client_connect(ConnectSub, Connack,
                                             [{port, PortSub}]),
    Subscribe = packet:gen_subscribe(53, "$share/group1/topic", 1),
    Suback = packet:gen_suback(53, 1),
    ok = gen_tcp:send(SubSocket, Subscribe),
    ok = packet:expect_packet(SubSocket, "suback", Suback),

    %% Connect publisher to one side of the cluster
    ConnectPub = packet:gen_connect("netsplit-publisher", [{clean_session, true},
                                                           {keepalive, 120}]),
    {_, _, PortPub} = random_node(Island2),
    {ok, PubSocket} = packet:do_client_connect(ConnectPub, Connack,
                                               [{port, PortPub}]),
    %% Let the cluster metadata converge.
    ok = wait_until_converged(Nodes,
                              fun(N) ->
                                      rpc:call(N, vmq_reg, total_subscriptions, [])
                              end, [{total, 1}]),

    %% Start the actual partition of the nodes.
    {_, Island1Names, _} = lists:unzip3(Island1),
    {_, Island2Names, _} = lists:unzip3(Island2),
    vmq_cluster_test_utils:partition_cluster(Island1Names, Island2Names),
    ok = wait_until_converged(Nodes,
                              fun(N) ->
                                      rpc:call(N, vmq_cluster, is_ready, [])
                              end, false),

    %% Publish
    Publish = packet:gen_publish("topic", 1, <<"message">>,
                                 [{mid, 1}]),
    Puback = packet:gen_puback(1),
    ok = gen_tcp:send(PubSocket, Publish),
    %% The ack should arrive fast as we'll at most blocked up to
    %% `remote_enqueue_timeout` trying to enqueue the msg one the
    %% remote node.
    ok = packet:expect_packet(gen_tcp, PubSocket, "puback", Puback, 5000),
    gen_tcp:close(PubSocket),
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Tiered Readiness Tests
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% With quorum=0.5, stopping 1 of 3 nodes should result in degraded
%% state. Clients should still be able to connect.
tiered_readiness_degraded_test(Config) ->
    ok = ensure_cluster(Config),
    {_, Nodes} = lists:keyfind(nodes, 1, Config),
    set_config(cluster_ready_quorum, 0.5),
    set_config(allow_register_during_netsplit, false),

    [{Peer3, Node3, _} | RestNodes] = lists:reverse(Nodes),
    vmq_cluster_test_utils:stop_peer(Peer3, Node3),

    %% Wait until surviving nodes detect tier as degraded
    ok = wait_until_converged(RestNodes,
                              fun(N) ->
                                      rpc:call(N, vmq_cluster, cluster_tier, [])
                              end, degraded),

    %% is_ready() should return true during degraded state
    {_, SurvivingNode, SurvivingPort} = hd(RestNodes),
    true = rpc:call(SurvivingNode, vmq_cluster, is_ready, []),

    %% Clients should be able to connect
    Connect = packet:gen_connect("tiered-degraded-client",
                                [{clean_session, true}, {keepalive, 10}]),
    Connack = packet:gen_connack(0),
    {ok, Socket} = packet:do_client_connect(Connect, Connack,
                                            [{port, SurvivingPort}]),
    gen_tcp:close(Socket),
    ok.

%% With quorum=0.5, stopping 2 of 3 nodes should result in partitioned
%% state. Clients should be rejected.
tiered_readiness_partitioned_test(Config) ->
    ok = ensure_cluster(Config),
    {_, Nodes} = lists:keyfind(nodes, 1, Config),
    set_config(cluster_ready_quorum, 0.5),
    set_config(allow_register_during_netsplit, false),

    [{Peer1, Node1, Port1} | Rest] = Nodes,
    [{Peer2, Node2, _}, {Peer3, Node3, _}] = Rest,

    %% Stop 2 nodes
    vmq_cluster_test_utils:stop_peer(Peer2, Node2),
    vmq_cluster_test_utils:stop_peer(Peer3, Node3),

    %% Wait until surviving node detects partitioned
    ok = wait_until_converged([{Peer1, Node1, Port1}],
                              fun(N) ->
                                      rpc:call(N, vmq_cluster, cluster_tier, [])
                              end, partitioned),

    %% is_ready() should return false
    false = rpc:call(Node1, vmq_cluster, is_ready, []),

    %% Clients should be rejected with CONNACK(3) = server unavailable
    Connect = packet:gen_connect("tiered-partitioned-client",
                                [{clean_session, true}, {keepalive, 10}]),
    ConnNack = packet:gen_connack(3),
    {ok, Socket} = packet:do_client_connect(Connect, ConnNack,
                                            [{port, Port1}]),
    gen_tcp:close(Socket),
    ok.

%% When partitioned but allow_register_during_netsplit=true, clients
%% should still be able to connect.
tiered_readiness_cap_override_test(Config) ->
    ok = ensure_cluster(Config),
    {_, Nodes} = lists:keyfind(nodes, 1, Config),
    set_config(cluster_ready_quorum, 0.5),
    set_config(allow_register_during_netsplit, true),

    [{Peer1, Node1, Port1} | Rest] = Nodes,
    [{Peer2, Node2, _}, {Peer3, Node3, _}] = Rest,

    %% Stop 2 nodes to trigger partitioned
    vmq_cluster_test_utils:stop_peer(Peer2, Node2),
    vmq_cluster_test_utils:stop_peer(Peer3, Node3),

    ok = wait_until_converged([{Peer1, Node1, Port1}],
                              fun(N) ->
                                      rpc:call(N, vmq_cluster, cluster_tier, [])
                              end, partitioned),

    %% Verify cluster is indeed partitioned, override allows connection
    false = rpc:call(Node1, vmq_cluster, is_ready, []),
    Connect = packet:gen_connect("tiered-cap-client",
                                [{clean_session, true}, {keepalive, 10}]),
    Connack = packet:gen_connack(0),
    {ok, Socket} = packet:do_client_connect(Connect, Connack,
                                            [{port, Port1}]),
    gen_tcp:close(Socket),
    ok.

%% Verify that transition counters are incremented when cluster
%% transitions between tiers.
tiered_readiness_transition_counters_test(Config) ->
    ok = ensure_cluster(Config),
    {_, Nodes} = lists:keyfind(nodes, 1, Config),
    set_config(cluster_ready_quorum, 0.5),

    [{Peer1, Node1, Port1} | Rest] = Nodes,
    [{Peer3, Node3, _} | _] = lists:reverse(Rest),

    %% Initially healthy: degraded counters should be {0, 0}
    {0, 0} = rpc:call(Node1, vmq_cluster, degraded_statistics, []),

    %% Netsplit counters should also be {0, 0} initially
    {0, 0} = rpc:call(Node1, vmq_cluster, netsplit_statistics, []),

    %% Stop 1 node -> degraded
    vmq_cluster_test_utils:stop_peer(Peer3, Node3),
    ok = wait_until_converged([{Peer1, Node1, Port1}],
                              fun(N) ->
                                      rpc:call(N, vmq_cluster, cluster_tier, [])
                              end, degraded),

    %% degraded_detected should be 1, netsplit counters unchanged
    {1, 0} = rpc:call(Node1, vmq_cluster, degraded_statistics, []),
    {0, 0} = rpc:call(Node1, vmq_cluster, netsplit_statistics, []),
    ok.

%% Test recovery from degraded/partitioned back to healthy using
%% partition/heal. Verifies that transition counters increment in
%% both directions (detected AND resolved).
tiered_readiness_recovery_test(Config) ->
    ok = ensure_cluster(Config),
    {_, Nodes} = lists:keyfind(nodes, 1, Config),
    set_config(cluster_ready_quorum, 0.5),
    set_config(allow_register_during_netsplit, false),

    %% Split: [Node3] alone (partitioned), [Node1, Node2] together (degraded)
    [{_, Node1, _} = N1, N2, {_, Node3, _} = N3] = Nodes,
    MajorityIsland = [N1, N2],
    MinorityIsland = [N3],
    {_, MajorityNames, _} = lists:unzip3(MajorityIsland),
    {_, MinorityNames, _} = lists:unzip3(MinorityIsland),
    vmq_cluster_test_utils:partition_cluster(MinorityNames, MajorityNames),

    %% Majority island (2/3 alive) should be degraded
    ok = wait_until_converged(MajorityIsland,
                              fun(N) -> rpc:call(N, vmq_cluster, cluster_tier, []) end,
                              degraded),
    %% Minority island (1/3 alive) should be partitioned
    ok = wait_until_converged(MinorityIsland,
                              fun(N) -> rpc:call(N, vmq_cluster, cluster_tier, []) end,
                              partitioned),

    %% Verify counters during partition
    {1, 0} = rpc:call(Node1, vmq_cluster, degraded_statistics, []),
    {0, 0} = rpc:call(Node1, vmq_cluster, netsplit_statistics, []),
    {0, 0} = rpc:call(Node3, vmq_cluster, degraded_statistics, []),
    {1, 0} = rpc:call(Node3, vmq_cluster, netsplit_statistics, []),

    %% Heal the partition
    vmq_cluster_test_utils:heal_cluster(MinorityNames, MajorityNames),

    %% All nodes should recover to healthy
    ok = wait_until_converged(Nodes,
                              fun(N) -> rpc:call(N, vmq_cluster, cluster_tier, []) end,
                              healthy),

    %% Node1 was degraded, now healthy: degraded_resolved incremented
    {1, 1} = rpc:call(Node1, vmq_cluster, degraded_statistics, []),
    {0, 0} = rpc:call(Node1, vmq_cluster, netsplit_statistics, []),
    %% Node3 was partitioned, now healthy: netsplit_resolved incremented
    {0, 0} = rpc:call(Node3, vmq_cluster, degraded_statistics, []),
    {1, 1} = rpc:call(Node3, vmq_cluster, netsplit_statistics, []),
    ok.

%% Verify pub/sub works during degraded state without CAP overrides.
%% This tests the core promise: degraded clusters remain operational
%% because is_ready() returns true.
tiered_readiness_pubsub_during_degraded_test(Config) ->
    ok = ensure_cluster(Config),
    {_, Nodes} = lists:keyfind(nodes, 1, Config),
    set_config(cluster_ready_quorum, 0.5),
    set_config(allow_register_during_netsplit, false),

    [{_, _Node1, Port1}, {_, _Node2, Port2}, {Peer3, Node3, _}] = Nodes,
    RestNodes = lists:sublist(Nodes, 2),

    %% Subscribe on Node2 while cluster is healthy
    SubConnect = packet:gen_connect("tiered-degraded-sub",
                                   [{clean_session, true}, {keepalive, 60}]),
    Connack = packet:gen_connack(0),
    {ok, SubSocket} = packet:do_client_connect(SubConnect, Connack,
                                               [{port, Port2}]),
    Subscribe = packet:gen_subscribe(53, "degraded/test", 0),
    Suback = packet:gen_suback(53, 0),
    ok = gen_tcp:send(SubSocket, Subscribe),
    ok = packet:expect_packet(SubSocket, "suback", Suback),

    %% Wait for subscription to propagate to all nodes
    ok = wait_until_converged(Nodes,
                              fun(N) -> rpc:call(N, vmq_reg, total_subscriptions, []) end,
                              [{total, 1}]),

    %% Stop Node3 to trigger degraded on the remaining 2 nodes
    vmq_cluster_test_utils:stop_peer(Peer3, Node3),
    ok = wait_until_converged(RestNodes,
                              fun(N) -> rpc:call(N, vmq_cluster, cluster_tier, []) end,
                              degraded),

    %% Connect publisher on Node1 — succeeds because is_ready()=true
    PubConnect = packet:gen_connect("tiered-degraded-pub",
                                   [{clean_session, true}, {keepalive, 60}]),
    {ok, PubSocket} = packet:do_client_connect(PubConnect, Connack,
                                               [{port, Port1}]),

    %% Publish QoS 0 on Node1, expect delivery on Node2
    Publish = packet:gen_publish("degraded/test", 0, <<"degraded-msg">>, []),
    ok = gen_tcp:send(PubSocket, Publish),
    ok = packet:expect_packet(SubSocket, "publish", Publish),

    gen_tcp:close(SubSocket),
    gen_tcp:close(PubSocket),
    ok.

%% Verify HTTP health endpoints return correct responses during
%% degraded and partitioned states.
tiered_health_http_tiered_test(Config) ->
    ok = ensure_cluster(Config),
    {_, Nodes} = lists:keyfind(nodes, 1, Config),
    set_config(cluster_ready_quorum, 0.5),
    set_config(allow_register_during_netsplit, false),

    [{_, Node1, Port1}, _, {_, Node3, Port3}] = Nodes,
    MajorityIsland = lists:sublist(Nodes, 2),
    MinorityIsland = [lists:nth(3, Nodes)],

    %% Start HTTP health listeners on Node1 (majority) and Node3 (minority)
    HttpPort1 = Port1 + 100,
    HttpPort3 = Port3 + 100,
    {ok, _} = rpc:call(Node1, vmq_server_cmd, listener_start,
                       [HttpPort1, [{http, true},
                                    {config_mod, vmq_health_http},
                                    {config_fun, routes}]]),
    {ok, _} = rpc:call(Node3, vmq_server_cmd, listener_start,
                       [HttpPort3, [{http, true},
                                    {config_mod, vmq_health_http},
                                    {config_fun, routes}]]),
    application:ensure_all_started(inets),

    %% Partition: [Node3] vs [Node1, Node2]
    {_, MajorityNames, _} = lists:unzip3(MajorityIsland),
    {_, MinorityNames, _} = lists:unzip3(MinorityIsland),
    vmq_cluster_test_utils:partition_cluster(MinorityNames, MajorityNames),

    ok = wait_until_converged(MajorityIsland,
                              fun(N) -> rpc:call(N, vmq_cluster, cluster_tier, []) end,
                              degraded),
    ok = wait_until_converged(MinorityIsland,
                              fun(N) -> rpc:call(N, vmq_cluster, cluster_tier, []) end,
                              partitioned),

    %% --- Degraded node (Node1) ---
    %% /health → 200, status=OK, cluster_state=degraded, warnings present
    {ok, {{_, 200, _}, _, DegHealthBody}} =
        httpc:request("http://localhost:" ++ integer_to_list(HttpPort1) ++ "/health"),
    DegHealthJson = vmq_json:decode(list_to_binary(DegHealthBody),
                                    [return_maps, {labels, binary}]),
    <<"OK">> = maps:get(<<"status">>, DegHealthJson),
    <<"degraded">> = maps:get(<<"cluster_state">>, DegHealthJson),
    true = is_list(maps:get(<<"warnings">>, DegHealthJson)),

    %% /health/cluster → 200, cluster_tier=degraded, alive_nodes=2, total=3
    {ok, {{_, 200, _}, _, DegClusterBody}} =
        httpc:request("http://localhost:" ++ integer_to_list(HttpPort1) ++ "/health/cluster"),
    DegClusterJson = vmq_json:decode(list_to_binary(DegClusterBody),
                                     [return_maps, {labels, binary}]),
    <<"degraded">> = maps:get(<<"cluster_tier">>, DegClusterJson),
    2 = maps:get(<<"alive_nodes">>, DegClusterJson),
    3 = maps:get(<<"total_nodes">>, DegClusterJson),

    %% --- Partitioned node (Node3) ---
    %% /health → 503, status=DOWN, cluster_state=partitioned
    {ok, {{_, 503, _}, _, PartHealthBody}} =
        httpc:request("http://localhost:" ++ integer_to_list(HttpPort3) ++ "/health"),
    PartHealthJson = vmq_json:decode(list_to_binary(PartHealthBody),
                                     [return_maps, {labels, binary}]),
    <<"DOWN">> = maps:get(<<"status">>, PartHealthJson),
    <<"partitioned">> = maps:get(<<"cluster_state">>, PartHealthJson),
    true = is_list(maps:get(<<"reasons">>, PartHealthJson)),

    %% /health/cluster → 503, cluster_tier=partitioned
    {ok, {{_, 503, _}, _, PartClusterBody}} =
        httpc:request("http://localhost:" ++ integer_to_list(HttpPort3) ++ "/health/cluster"),
    PartClusterJson = vmq_json:decode(list_to_binary(PartClusterBody),
                                      [return_maps, {labels, binary}]),
    <<"partitioned">> = maps:get(<<"cluster_tier">>, PartClusterJson),

    %% Heal and verify recovery
    vmq_cluster_test_utils:heal_cluster(MinorityNames, MajorityNames),
    ok = wait_until_converged(Nodes,
                              fun(N) -> rpc:call(N, vmq_cluster, cluster_tier, []) end,
                              healthy),
    ok.

helper_pub_qos1(ClientId, Publish, Port) ->
    Connect = packet:gen_connect(ClientId, [{keepalive, 60}]),
    Connack = packet:gen_connack(0),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, [{port, Port}]),
    ok = gen_tcp:send(Socket, Publish),
    gen_tcp:close(Socket).

ensure_cluster(Config) ->
    vmq_cluster_test_utils:ensure_cluster(Config).

wait_until_converged(Nodes, Fun, ExpectedReturn) ->
    {_, NodeNames, _} = lists:unzip3(Nodes),
    vmq_cluster_test_utils:wait_until(
      fun() ->
              lists:all(fun(X) -> X == true end,
                        vmq_cluster_test_utils:pmap(
                          fun(Node) ->
                                  ExpectedReturn == Fun(Node)
                          end, NodeNames))
      end, 100*2, 500).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Internal
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
random_node(Nodes) ->
    lists:nth(rand:uniform(length(Nodes)), Nodes).
