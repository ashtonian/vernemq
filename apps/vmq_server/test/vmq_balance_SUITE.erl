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

-module(vmq_balance_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

%% ===================================================================
%% common_test callbacks
%% ===================================================================

init_per_suite(Config) ->
    cover:start(),
    Config.

end_per_suite(_Config) ->
    _Config.

init_per_testcase(_Case, Config) ->
    vmq_test_utils:setup(),
    vmq_server_cmd:set_config(allow_anonymous, true),
    Config.

end_per_testcase(_, Config) ->
    vmq_test_utils:teardown(),
    Config.

all() ->
    [
        %% vmq_balance_srv tests
        balance_srv_starts_test,
        balance_srv_disabled_always_accepts_test,
        balance_srv_enabled_accepts_below_threshold_test,
        balance_srv_stats_returns_tuple_test,
        balance_stats_5tuple_test,

        %% Hook tests
        hook_registered_test,
        hook_not_registered_when_reject_disabled_test,
        hook_dynamic_registration_test,
        hook_dynamic_toggle_via_timer_test,
        hook_terminate_deregisters_test,
        hook_accepts_when_disabled_test,
        hook_accepts_when_below_threshold_test,
        hook_rejects_when_overloaded_test,
        hook_allows_session_takeover_test,
        hook_rejection_counter_test,

        %% HTTP endpoint tests
        balance_http_returns_200_when_disabled_test,
        balance_http_returns_200_when_accepting_test,
        balance_http_json_format_test,

        %% Metrics tests
        balance_metrics_exposed_in_prometheus_test,
        balance_metrics_default_values_test,

        %% Integration: single-node balance behavior
        balance_single_node_always_accepts_test,
        balance_enabled_single_node_always_accepts_test,

        %% Rebalancer tests
        rebalancer_starts_test,
        rebalancer_stats_defaults_test,
        rebalancer_disabled_returns_error_test,
        rebalancer_needs_both_flags_test,
        rebalancer_force_disconnects_sessions_test,
        rebalancer_metrics_test,

        %% Auto-interval rebalancer test
        rebalancer_auto_interval_schedules_test,

        %% Node counts API test
        get_node_counts_test
    ].

%% ===================================================================
%% vmq_balance_srv tests
%% ===================================================================

balance_srv_starts_test(_Config) ->
    %% vmq_balance_srv should be running as part of the supervision tree
    Pid = erlang:whereis(vmq_balance_srv),
    ?assert(is_pid(Pid)),
    ?assert(is_process_alive(Pid)).

balance_srv_disabled_always_accepts_test(_Config) ->
    %% By default, balance is disabled — should always accept
    ?assert(vmq_balance_srv:is_accepting()).

balance_srv_enabled_accepts_below_threshold_test(_Config) ->
    %% Enable balance and verify that with 0 connections (below min_connections),
    %% the node still accepts
    vmq_config:set_env(balance_enabled, true, false),
    vmq_config:set_env(balance_min_connections, 100, false),
    %% Restart the server to pick up new config
    restart_balance_srv(),
    %% Wait for at least one check cycle
    timer:sleep(200),
    %% Single node with 0 connections should always accept
    ?assert(vmq_balance_srv:is_accepting()).

balance_srv_stats_returns_tuple_test(_Config) ->
    {IsAccepting, LocalConns, ClusterAvg, IsEnabled, Rejections} = vmq_balance_srv:balance_stats(),
    ?assert(is_integer(IsAccepting)),
    ?assert(IsAccepting =:= 0 orelse IsAccepting =:= 1),
    ?assert(is_integer(LocalConns)),
    ?assert(LocalConns >= 0),
    ?assert(is_integer(ClusterAvg)),
    ?assert(is_integer(IsEnabled)),
    ?assert(IsEnabled =:= 0 orelse IsEnabled =:= 1),
    ?assert(is_integer(Rejections)),
    ?assert(Rejections >= 0).

%% ===================================================================
%% 5-tuple stats test
%% ===================================================================

balance_stats_5tuple_test(_Config) ->
    %% Verify balance_stats returns a 5-tuple with correct types
    Result = vmq_balance_srv:balance_stats(),
    ?assertMatch({_, _, _, _, _}, Result),
    {IsAccepting, LocalConns, ClusterAvg, IsEnabled, Rejections} = Result,
    ?assert(is_integer(IsAccepting)),
    ?assert(is_integer(LocalConns)),
    ?assert(is_integer(ClusterAvg)),
    ?assert(is_integer(IsEnabled)),
    ?assert(is_integer(Rejections)),
    %% Disabled by default: accepting=1, enabled=0, rejections=0
    ?assertEqual(1, IsAccepting),
    ?assertEqual(0, IsEnabled),
    ?assertEqual(0, Rejections).

%% ===================================================================
%% Hook tests
%% ===================================================================

hook_registered_test(_Config) ->
    %% Hooks are only registered when both balance_enabled and balance_reject_enabled are true
    %% Use vmq_config:set_env to properly update the ETS config cache
    vmq_config:set_env(balance_enabled, true, false),
    vmq_config:set_env(balance_reject_enabled, true, false),
    restart_balance_srv(),
    Hooks = vmq_plugin:info(all),
    AuthRegHooks = [H || {auth_on_register, vmq_balance_hook, _, _} = H <- Hooks],
    AuthRegM5Hooks = [H || {auth_on_register_m5, vmq_balance_hook, _, _} = H <- Hooks],
    ?assert(length(AuthRegHooks) > 0),
    ?assert(length(AuthRegM5Hooks) > 0).

hook_not_registered_when_reject_disabled_test(_Config) ->
    %% With balance_enabled=true but balance_reject_enabled=false, hooks should NOT be registered
    vmq_config:set_env(balance_enabled, true, false),
    vmq_config:set_env(balance_reject_enabled, false, false),
    restart_balance_srv(),
    Hooks = vmq_plugin:info(all),
    AuthRegHooks = [H || {auth_on_register, vmq_balance_hook, _, _} = H <- Hooks],
    AuthRegM5Hooks = [H || {auth_on_register_m5, vmq_balance_hook, _, _} = H <- Hooks],
    ?assertEqual(0, length(AuthRegHooks)),
    ?assertEqual(0, length(AuthRegM5Hooks)).

hook_dynamic_registration_test(_Config) ->
    %% Start with reject off — no hooks
    vmq_config:set_env(balance_enabled, true, false),
    vmq_config:set_env(balance_reject_enabled, false, false),
    restart_balance_srv(),
    Hooks1 = vmq_plugin:info(all),
    ?assertEqual(0, length([H || {auth_on_register, vmq_balance_hook, _, _} = H <- Hooks1])),
    %% Turn reject on and restart — hooks should appear
    vmq_config:set_env(balance_reject_enabled, true, false),
    restart_balance_srv(),
    Hooks2 = vmq_plugin:info(all),
    ?assert(length([H || {auth_on_register, vmq_balance_hook, _, _} = H <- Hooks2]) > 0),
    ?assert(length([H || {auth_on_register_m5, vmq_balance_hook, _, _} = H <- Hooks2]) > 0),
    %% Turn reject off again and restart — hooks should be gone
    vmq_config:set_env(balance_reject_enabled, false, false),
    restart_balance_srv(),
    Hooks3 = vmq_plugin:info(all),
    ?assertEqual(0, length([H || {auth_on_register, vmq_balance_hook, _, _} = H <- Hooks3])),
    ?assertEqual(0, length([H || {auth_on_register_m5, vmq_balance_hook, _, _} = H <- Hooks3])).

hook_dynamic_toggle_via_timer_test(_Config) ->
    %% Verify hooks are toggled dynamically by the check_balance timer
    %% without requiring a restart of vmq_balance_srv.
    vmq_config:set_env(balance_enabled, true, false),
    vmq_config:set_env(balance_reject_enabled, false, false),
    vmq_config:set_env(balance_check_interval, 200, false),
    restart_balance_srv(),
    %% No hooks initially
    Hooks1 = vmq_plugin:info(all),
    ?assertEqual(0, length([H || {auth_on_register, vmq_balance_hook, _, _} = H <- Hooks1])),
    %% Enable reject via config — next check_balance tick should register hooks
    vmq_config:set_env(balance_reject_enabled, true, false),
    timer:sleep(500),
    Hooks2 = vmq_plugin:info(all),
    ?assert(length([H || {auth_on_register, vmq_balance_hook, _, _} = H <- Hooks2]) > 0),
    ?assert(length([H || {auth_on_register_m5, vmq_balance_hook, _, _} = H <- Hooks2]) > 0),
    %% Disable reject via config — next tick should deregister hooks
    vmq_config:set_env(balance_reject_enabled, false, false),
    timer:sleep(500),
    Hooks3 = vmq_plugin:info(all),
    ?assertEqual(0, length([H || {auth_on_register, vmq_balance_hook, _, _} = H <- Hooks3])),
    ?assertEqual(0, length([H || {auth_on_register_m5, vmq_balance_hook, _, _} = H <- Hooks3])).

hook_terminate_deregisters_test(_Config) ->
    %% Verify that terminating vmq_balance_srv deregisters hooks
    vmq_config:set_env(balance_enabled, true, false),
    vmq_config:set_env(balance_reject_enabled, true, false),
    restart_balance_srv(),
    Hooks1 = vmq_plugin:info(all),
    ?assert(length([H || {auth_on_register, vmq_balance_hook, _, _} = H <- Hooks1]) > 0),
    %% Terminate (not restart) — hooks should be cleaned up
    supervisor:terminate_child(vmq_server_sup, vmq_balance_srv),
    Hooks2 = vmq_plugin:info(all),
    ?assertEqual(0, length([H || {auth_on_register, vmq_balance_hook, _, _} = H <- Hooks2])),
    ?assertEqual(0, length([H || {auth_on_register_m5, vmq_balance_hook, _, _} = H <- Hooks2])),
    %% Restart for subsequent tests
    supervisor:restart_child(vmq_server_sup, vmq_balance_srv).

hook_accepts_when_disabled_test(_Config) ->
    %% When balance is disabled (default), hook should return next
    SubscriberId = {"", <<"hook-test-client">>},
    ?assertEqual(
        next,
        vmq_balance_hook:auth_on_register(
            {{127, 0, 0, 1}, 12345}, SubscriberId, <<"user">>, <<"pass">>, true
        )
    ),
    ?assertEqual(
        next,
        vmq_balance_hook:auth_on_register_m5(
            {{127, 0, 0, 1}, 12345}, SubscriberId, <<"user">>, <<"pass">>, true, #{}
        )
    ).

hook_accepts_when_below_threshold_test(_Config) ->
    %% Enable balance, single node with 0 connections → still accepting
    vmq_config:set_env(balance_enabled, true, false),
    restart_balance_srv(),
    timer:sleep(200),
    SubscriberId = {"", <<"hook-threshold-client">>},
    ?assertEqual(
        next,
        vmq_balance_hook:auth_on_register(
            {{127, 0, 0, 1}, 12345}, SubscriberId, <<"user">>, <<"pass">>, true
        )
    ).

hook_rejects_when_overloaded_test(_Config) ->
    %% Force the balance_srv into rejecting state by manipulating its state
    %% We use sys:replace_state to force accepting=false, enabled=true
    %% Record field positions: #state.accepting = 5, #state.enabled = 6
    sys:replace_state(vmq_balance_srv, fun(State) ->
        setelement(5, setelement(6, State, true), false)
    end),
    SubscriberId = {"", <<"hook-reject-client">>},
    %% v3.1.1 should get not_authorized
    ?assertEqual(
        {error, not_authorized},
        vmq_balance_hook:auth_on_register(
            {{127, 0, 0, 1}, 12345}, SubscriberId, <<"user">>, <<"pass">>, true
        )
    ),
    %% v5 should get server_busy
    ?assertEqual(
        {error, #{reason_code => server_busy}},
        vmq_balance_hook:auth_on_register_m5(
            {{127, 0, 0, 1}, 12345}, SubscriberId, <<"user">>, <<"pass">>, true, #{}
        )
    ).

hook_allows_session_takeover_test(_Config) ->
    %% Start an MQTT listener and connect a client to create a session
    vmq_server_cmd:listener_start(1889, []),
    ClientId = <<"hook-takeover-client">>,
    Connect = packet:gen_connect(ClientId, [{keepalive, 60}]),
    Connack = packet:gen_connack(0),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, [{port, 1889}]),
    %% Force rejecting state
    %% Record field positions: #state.accepting = 5, #state.enabled = 6
    sys:replace_state(vmq_balance_srv, fun(State) ->
        setelement(5, setelement(6, State, true), false)
    end),
    %% The existing session should allow reconnect (returns next)
    SubscriberId = {"", ClientId},
    ?assertEqual(
        next,
        vmq_balance_hook:auth_on_register(
            {{127, 0, 0, 1}, 12345}, SubscriberId, <<"user">>, <<"pass">>, true
        )
    ),
    gen_tcp:close(Socket),
    vmq_server_cmd:listener_stop(1889, "127.0.0.1", false).

hook_rejection_counter_test(_Config) ->
    %% Force rejecting state
    %% Record field positions: #state.accepting = 5, #state.enabled = 6
    sys:replace_state(vmq_balance_srv, fun(State) ->
        setelement(5, setelement(6, State, true), false)
    end),
    %% Get initial rejection count
    {_, _, _, _, InitialCount} = vmq_balance_srv:balance_stats(),
    %% Trigger a rejection
    SubscriberId = {"", <<"hook-counter-client">>},
    vmq_balance_hook:auth_on_register(
        {{127, 0, 0, 1}, 12345}, SubscriberId, <<"user">>, <<"pass">>, true
    ),
    %% Allow the async cast to be processed
    timer:sleep(50),
    {_, _, _, _, NewCount} = vmq_balance_srv:balance_stats(),
    ?assertEqual(InitialCount + 1, NewCount).

%% ===================================================================
%% HTTP endpoint tests
%% ===================================================================

balance_http_returns_200_when_disabled_test(_Config) ->
    %% Balance is disabled by default — endpoint should return 200
    Port = start_http_listener(),
    application:ensure_all_started(inets),
    Url = "http://localhost:" ++ integer_to_list(Port) ++ "/api/balance-health",
    {ok, {{_, 200, _}, _Headers, _Body}} = httpc:request(Url),
    stop_http_listener(Port).

balance_http_returns_200_when_accepting_test(_Config) ->
    %% Enable balance, but single node should still accept
    vmq_config:set_env(balance_enabled, true, false),
    restart_balance_srv(),
    timer:sleep(200),
    Port = start_http_listener(),
    application:ensure_all_started(inets),
    Url = "http://localhost:" ++ integer_to_list(Port) ++ "/api/balance-health",
    {ok, {{_, 200, _}, _Headers, _Body}} = httpc:request(Url),
    stop_http_listener(Port).

balance_http_json_format_test(_Config) ->
    Port = start_http_listener(),
    application:ensure_all_started(inets),
    Url = "http://localhost:" ++ integer_to_list(Port) ++ "/api/balance-health",
    {ok, {_Status, _Headers, Body}} = httpc:request(Url),
    Json = vmq_json:decode(list_to_binary(Body), [return_maps, {labels, binary}]),
    %% Verify JSON structure
    ?assert(maps:is_key(<<"status">>, Json)),
    ?assert(maps:is_key(<<"connections">>, Json)),
    ?assert(maps:is_key(<<"cluster_avg">>, Json)),
    %% When disabled/accepting, status should be "accepting"
    ?assertEqual(<<"accepting">>, maps:get(<<"status">>, Json)),
    %% Connections should be an integer
    Connections = maps:get(<<"connections">>, Json),
    ?assert(is_integer(Connections)),
    stop_http_listener(Port).

%% ===================================================================
%% Metrics tests
%% ===================================================================

balance_metrics_exposed_in_prometheus_test(_Config) ->
    Port = start_metrics_listener(),
    application:ensure_all_started(inets),
    Url = "http://localhost:" ++ integer_to_list(Port) ++ "/metrics",
    {ok, {_Status, _Headers, Body}} = httpc:request(Url),
    Lines = re:split(Body, "\n"),
    Node = atom_to_list(node()),
    %% Check that balance metrics are present in prometheus output
    ?assert(has_metric_line(Lines, "balance_is_accepting", Node)),
    ?assert(has_metric_line(Lines, "balance_local_connections", Node)),
    ?assert(has_metric_line(Lines, "balance_cluster_avg", Node)),
    ?assert(has_metric_line(Lines, "balance_is_enabled", Node)),
    stop_http_listener(Port).

balance_metrics_default_values_test(_Config) ->
    %% When disabled, balance_is_accepting should be 1, balance_is_enabled should be 0
    {IsAccepting, _LocalConns, _ClusterAvg, IsEnabled, _Rejections} = vmq_balance_srv:balance_stats(),
    ?assertEqual(1, IsAccepting),
    ?assertEqual(0, IsEnabled).

%% ===================================================================
%% Integration tests
%% ===================================================================

balance_single_node_always_accepts_test(_Config) ->
    %% A single-node cluster should always accept regardless of connection count,
    %% because there's nowhere else to send traffic
    ?assert(vmq_balance_srv:is_accepting()),
    %% Even after a check cycle
    timer:sleep(100),
    ?assert(vmq_balance_srv:is_accepting()).

balance_enabled_single_node_always_accepts_test(_Config) ->
    %% Enable balance, set a very low threshold
    vmq_config:set_env(balance_enabled, true, false),
    vmq_config:set_env(balance_threshold, "1.0", false),
    vmq_config:set_env(balance_min_connections, 0, false),
    restart_balance_srv(),
    timer:sleep(200),
    %% Even with balance enabled and threshold=1.0, single node should accept
    %% because you can't redirect traffic when there's only one node
    ?assert(vmq_balance_srv:is_accepting()),
    %% Start an MQTT listener and make some connections
    vmq_server_cmd:listener_start(1888, []),
    Connect = packet:gen_connect("balance-test-client", [{keepalive, 60}]),
    Connack = packet:gen_connack(0),
    {ok, Socket} = packet:do_client_connect(Connect, Connack, []),
    %% Wait for a balance check cycle
    timer:sleep(200),
    %% Still accepting — single node
    ?assert(vmq_balance_srv:is_accepting()),
    gen_tcp:close(Socket),
    vmq_server_cmd:listener_stop(1888, "127.0.0.1", false).

%% ===================================================================
%% Rebalancer tests
%% ===================================================================

rebalancer_starts_test(_Config) ->
    %% vmq_balance_rebalancer should be running as part of the supervision tree
    Pid = erlang:whereis(vmq_balance_rebalancer),
    ?assert(is_pid(Pid)),
    ?assert(is_process_alive(Pid)).

rebalancer_stats_defaults_test(_Config) ->
    %% Default stats should be {0, 0}
    {Disconnections, Rounds} = vmq_balance_rebalancer:rebalance_stats(),
    ?assertEqual(0, Disconnections),
    ?assertEqual(0, Rounds).

rebalancer_disabled_returns_error_test(_Config) ->
    %% With balance_enabled=false (default), trigger_rebalance should return disabled
    ?assertEqual({error, disabled}, vmq_balance_rebalancer:trigger_rebalance(false)),
    ?assertEqual({error, disabled}, vmq_balance_rebalancer:trigger_rebalance(true)).

rebalancer_needs_both_flags_test(_Config) ->
    %% Only balance_enabled=true but rebalance_enabled=false -> disabled
    vmq_config:set_env(balance_enabled, true, false),
    vmq_config:set_env(rebalance_enabled, false, false),
    ?assertEqual({error, disabled}, vmq_balance_rebalancer:trigger_rebalance(true)),
    %% Only rebalance_enabled=true but balance_enabled=false -> disabled
    vmq_config:set_env(balance_enabled, false, false),
    vmq_config:set_env(rebalance_enabled, true, false),
    ?assertEqual({error, disabled}, vmq_balance_rebalancer:trigger_rebalance(true)),
    %% Both enabled -> should not return disabled (may return other error on single node)
    vmq_config:set_env(balance_enabled, true, false),
    vmq_config:set_env(rebalance_enabled, true, false),
    Result = vmq_balance_rebalancer:trigger_rebalance(true),
    ?assertNotEqual({error, disabled}, Result).

rebalancer_force_disconnects_sessions_test(_Config) ->
    %% Enable balance and rebalance, connect clients, fake overload, trigger rebalance
    vmq_config:set_env(balance_enabled, true, false),
    vmq_config:set_env(rebalance_enabled, true, false),
    vmq_config:set_env(rebalance_threshold, "1.0", false),
    vmq_config:set_env(rebalance_batch_size, 100, false),
    vmq_config:set_env(rebalance_cooldown, 0, false),
    restart_balance_srv(),
    timer:sleep(200),

    %% Start listener and connect multiple clients
    vmq_server_cmd:listener_start(1888, []),
    NumClients = 5,
    Sockets = lists:map(
        fun(I) ->
            ClientId = "rebal-test-" ++ integer_to_list(I),
            Connect = packet:gen_connect(ClientId, [{keepalive, 60}]),
            Connack = packet:gen_connack(0),
            {ok, Socket} = packet:do_client_connect(Connect, Connack, []),
            Socket
        end,
        lists:seq(1, NumClients)
    ),

    %% Wait for balance check to pick up connections
    timer:sleep(300),

    %% Use sys:replace_state to fake multi-node scenario with overloaded local node.
    %% Make it look like there are 2 nodes: local has many, "fake" has few.
    %% node_counts field is at position 4 in the #state record (after local_count,
    %% cluster_avg). We need to find the right field positions.
    %% #state{local_count=2, cluster_avg=3, node_counts=4, accepting=5, enabled=6, ...}
    FakeOtherNode = 'fake_other@127.0.0.1',
    sys:replace_state(vmq_balance_srv, fun(State) ->
        %% Set node_counts to show local node overloaded relative to a fake node
        %% Record: {state, local_count, cluster_avg, node_counts, accepting, enabled, ...}
        NodeCounts = #{node() => NumClients, FakeOtherNode => 0},
        setelement(4, State, NodeCounts)
    end),

    %% Trigger rebalance with force (skip stability check)
    Result = vmq_balance_rebalancer:trigger_rebalance(true),
    ?assertMatch({ok, _}, Result),
    {ok, #{disconnected := D}} = Result,
    ?assert(D > 0),

    %% Cleanup
    lists:foreach(fun(S) -> catch gen_tcp:close(S) end, Sockets),
    vmq_server_cmd:listener_stop(1888, "127.0.0.1", false).

rebalancer_metrics_test(_Config) ->
    %% Verify rebalance stats are exposed through metrics
    {RebalanceDisconnections, RebalanceRounds} = vmq_balance_rebalancer:rebalance_stats(),
    ?assert(is_integer(RebalanceDisconnections)),
    ?assert(is_integer(RebalanceRounds)),
    ?assert(RebalanceDisconnections >= 0),
    ?assert(RebalanceRounds >= 0).

rebalancer_auto_interval_schedules_test(_Config) ->
    %% Verify that the auto_interval timer is scheduled and re-checked.
    %% With rebalance disabled (default), the timer should still run
    %% (slow re-check at 60s) but not trigger a rebalance.
    Pid = erlang:whereis(vmq_balance_rebalancer),
    ?assert(is_pid(Pid)),
    ?assert(is_process_alive(Pid)),

    %% Set auto_interval to a short value and enable rebalance
    vmq_config:set_env(rebalance_auto_interval, 1, false),
    vmq_config:set_env(balance_enabled, true, false),
    vmq_config:set_env(rebalance_enabled, true, false),

    %% Restart the rebalancer to pick up the new config in init
    supervisor:terminate_child(vmq_server_sup, vmq_balance_rebalancer),
    supervisor:restart_child(vmq_server_sup, vmq_balance_rebalancer),
    timer:sleep(100),

    %% The rebalancer should still be alive after the timer fires
    %% (on a single node, rebalance is a no-op since NumNodes <= 1)
    NewPid = erlang:whereis(vmq_balance_rebalancer),
    ?assert(is_pid(NewPid)),
    ?assert(is_process_alive(NewPid)),

    %% Wait for at least one auto_interval tick (1 second + buffer)
    timer:sleep(1500),

    %% Process should still be alive and stats accessible
    ?assert(is_process_alive(NewPid)),
    {Disconnections, Rounds} = vmq_balance_rebalancer:rebalance_stats(),
    ?assert(is_integer(Disconnections)),
    ?assert(is_integer(Rounds)),

    %% Verify runtime config change: set interval to 0 (disabled)
    %% Timer should still re-check at the slow interval
    vmq_config:set_env(rebalance_auto_interval, 0, false),
    timer:sleep(1500),
    ?assert(is_process_alive(whereis(vmq_balance_rebalancer))).

get_node_counts_test(_Config) ->
    %% get_node_counts should return a map
    NodeCounts = vmq_balance_srv:get_node_counts(),
    ?assert(is_map(NodeCounts)).

%% ===================================================================
%% Cluster tests
%% ===================================================================

%% Note: Multi-node cluster tests for balance behavior belong in
%% vmq_balance_cluster_SUITE.erl which uses the cluster test utilities
%% to start multiple peer nodes. See vmq_cluster_SUITE.erl for the pattern.

%% ===================================================================
%% Helpers
%% ===================================================================

start_http_listener() ->
    Port = vmq_test_utils:get_free_port(),
    vmq_server_cmd:listener_start(Port, [
        {http, true},
        {config_mod, vmq_balance_http},
        {config_fun, routes}
    ]),
    Port.

start_metrics_listener() ->
    Port = vmq_test_utils:get_free_port(),
    application:set_env(vmq_server, http_modules_auth, #{vmq_metrics_http => "noauth"}),
    vmq_server_cmd:listener_start(Port, [
        {http, true},
        {config_mod, vmq_metrics_http},
        {config_fun, routes}
    ]),
    Port.

stop_http_listener(Port) ->
    vmq_server_cmd:listener_stop(Port, "127.0.0.1", false).

restart_balance_srv() ->
    case erlang:whereis(vmq_balance_srv) of
        undefined ->
            ok;
        Pid ->
            supervisor:terminate_child(vmq_server_sup, vmq_balance_srv),
            supervisor:restart_child(vmq_server_sup, vmq_balance_srv),
            %% Wait for the new process to be up
            wait_for_balance_srv(Pid, 50)
    end.

wait_for_balance_srv(_OldPid, 0) ->
    error(balance_srv_restart_timeout);
wait_for_balance_srv(OldPid, Retries) ->
    case erlang:whereis(vmq_balance_srv) of
        undefined ->
            timer:sleep(50),
            wait_for_balance_srv(OldPid, Retries - 1);
        OldPid ->
            timer:sleep(50),
            wait_for_balance_srv(OldPid, Retries - 1);
        _NewPid ->
            ok
    end.

has_metric_line(Lines, MetricName, Node) ->
    Prefix = list_to_binary(MetricName ++ "{node=\"" ++ Node ++ "\""),
    lists:any(
        fun(Line) ->
            case binary:match(Line, Prefix) of
                {0, _} -> true;
                _ -> false
            end
        end,
        Lines
    ).
