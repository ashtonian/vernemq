-module(vmq_cluster_com_SUITE).
-compile(export_all).
-compile(nowarn_export_all).

-include_lib("common_test/include/ct.hrl").

%% ===================================================================
%% common_test callbacks
%% ===================================================================
init_per_suite(Config) ->
    S = vmq_test_utils:get_suite_rand_seed(),
    Config0 = vmq_cluster_test_utils:init_distribution(Config),
    ct:log("node name ~p", [node()]),
    {ok, Peer, Node} = vmq_cluster_test_utils:start_node(test_com1, Config, default_case),
    ct:pal("This is the default NODE : ~p~n", [Node]),
    {ok, _} = ct_cover:add_nodes([Node]),
    vmq_cluster_test_utils:wait_until_ready([Node]),
    [{peer, Peer}, {node, Node}, S | Config0].

end_per_suite(Config) ->
    {_, Peer} = lists:keyfind(peer, 1, Config),
    {_, Node} = lists:keyfind(node, 1, Config),
    ok = vmq_cluster_test_utils:stop_peer(Peer, Node),
    Config.

init_per_testcase(reconnect_backoff_test, Config) ->
    vmq_test_utils:seed_rand(Config),
    Node = proplists:get_value(node, Config),
    %% Use short delays so the test runs quickly.
    ok = rpc:block_call(Node, vmq_config, set_env, [
        outgoing_clustering_reconnect_base_delay, 100, false
    ]),
    ok = rpc:block_call(Node, vmq_config, set_env, [
        outgoing_clustering_reconnect_max_delay, 800, false
    ]),
    ClusterNodePid = setup_mock_vmq_cluster_node(Config, lifo),
    [{cluster_node_pid, ClusterNodePid} | Config];
init_per_testcase(Case, Config) when
    Case =:= fifo_head_drop_test;
    Case =:= fifo_qos_priority_test
->
    vmq_test_utils:seed_rand(Config),
    ClusterNodePid = setup_mock_vmq_cluster_node(Config, fifo),
    [{cluster_node_pid, ClusterNodePid} | Config];
init_per_testcase(_Case, Config) ->
    vmq_test_utils:seed_rand(Config),
    ClusterNodePid = setup_mock_vmq_cluster_node(Config, lifo),
    [{cluster_node_pid, ClusterNodePid} | Config].

end_per_testcase(_Case, Config) ->
    terminate_mock_vmq_cluster_node(Config),
    ok.

all() ->
    [
        connect_success_test,
        connect_success_send_error,
        connect_success_send_error_timeout,
        reconnect_backoff_test,
        fifo_head_drop_test,
        lifo_tail_drop_test,
        fifo_qos_priority_test
    ].

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Actual Tests
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
connect_params(_RemoteNode) ->
    {gen_tcp, {127, 0, 0, 1}, 12345}.

connect_success_test(Config) ->
    ClusterNodePid = cluster_node_pid(Config),
    {ok, ListenSocket} = gen_tcp:listen(12345, [binary, {reuseaddr, true}, {active, false}]),
    {ok, Socket} = gen_tcp:accept(ListenSocket, 30000),
    recv_connect(Socket, Config),

    % send test message
    ok = send_message(ClusterNodePid, hello_world),
    % recv this message
    recv_message(Socket, hello_world).

connect_success_send_error(Config) ->
    % check that message isn't lost
    ClusterNodePid = cluster_node_pid(Config),
    {ok, ListenSocket} = gen_tcp:listen(12345, [binary, {reuseaddr, true}, {active, false}]),
    {ok, Socket1} = gen_tcp:accept(ListenSocket, 30000),
    recv_connect(Socket1, Config),
    % close this socket
    gen_tcp:close(Socket1),
    % send test message, will be buffered and delivered on next successful reconnect
    ok = send_message(ClusterNodePid, hello_world),

    {ok, Socket2} = gen_tcp:accept(ListenSocket, 30000),
    recv_connect(Socket2, Config),
    % recv this message
    recv_message(Socket2, hello_world).

connect_success_send_error_timeout(Config) ->
    ct:timetrap({minutes, 10}),
    % check that message isn't lost
    ClusterNodePid = cluster_node_pid(Config),
    {ok, ListenSocket} = gen_tcp:listen(12345, [binary, {reuseaddr, true}, {active, false}]),
    {ok, Socket1} = gen_tcp:accept(ListenSocket, 30000),
    recv_connect(Socket1, Config),

    N = send_until_tcp_buffer_full(ClusterNodePid),
    % once the tcp buffer is full, we get disconnected
    recv_until_tcp_buffer_empty(Socket1, N),
    % we should have a TCP_CLOSE now
    {error, closed} = gen_tcp:recv(Socket1, 0),

    % the cluster node should do the reconnect
    {ok, Socket2} = gen_tcp:accept(ListenSocket, 30000),
    recv_connect(Socket2, Config),

    % the last buffered message is repeated as the cluster node doesn't
    % actually know if we have received it or not.
    recv_message(Socket2, <<1:10000, N:32>>),
    {error, timeout} = gen_tcp:recv(Socket2, 0, 1000).

reconnect_backoff_test(Config) ->
    %% Configured with base_delay=100, max_delay=800 in init_per_testcase.
    %% Verify that successive reconnects take progressively longer.
    _ClusterNodePid = cluster_node_pid(Config),
    {ok, ListenSocket} = gen_tcp:listen(12345, [binary, {reuseaddr, true}, {active, false}]),

    %% Accept initial connection.
    {ok, Socket0} = gen_tcp:accept(ListenSocket, 5000),
    recv_connect(Socket0, Config),
    gen_tcp:close(Socket0),

    %% Measure time for 3 successive reconnects.
    Delays = measure_reconnect_delays(ListenSocket, Config, 3, []),
    ct:pal("Measured reconnect delays: ~p ms", [Delays]),

    %% Verify backoff: each delay should be greater than the previous.
    true = is_increasing(Delays),

    %% Verify the first delay is roughly in the base range (100ms +/- 20% jitter = 80-120ms).
    %% Allow generous bounds since timer precision and scheduling add noise.
    [First | _] = Delays,
    true = First >= 50,
    true = First =< 300,

    gen_tcp:close(ListenSocket),
    ok.

measure_reconnect_delays(_ListenSocket, _Config, 0, Acc) ->
    lists:reverse(Acc);
measure_reconnect_delays(ListenSocket, Config, N, Acc) ->
    T0 = erlang:monotonic_time(millisecond),
    {ok, Socket} = gen_tcp:accept(ListenSocket, 10000),
    T1 = erlang:monotonic_time(millisecond),
    recv_connect(Socket, Config),
    gen_tcp:close(Socket),
    measure_reconnect_delays(ListenSocket, Config, N - 1, [T1 - T0 | Acc]).

is_increasing([_]) -> true;
is_increasing([A, B | Rest]) when B > A -> is_increasing([B | Rest]);
is_increasing(_) -> false.

%% With fifo policy, oldest messages are evicted when the buffer is full.
%% After reconnect the newest messages should be present.
fifo_head_drop_test(Config) ->
    ClusterNodePid = cluster_node_pid(Config),
    {ok, ListenSocket} = gen_tcp:listen(12345, [binary, {reuseaddr, true}, {active, false}]),
    {ok, Socket1} = gen_tcp:accept(ListenSocket, 30000),
    recv_connect(Socket1, Config),

    % Close socket and listener to make node unreachable and prevent reconnect
    gen_tcp:close(Socket1),
    gen_tcp:close(ListenSocket),
    timer:sleep(200),

    % Send 100 small messages while unreachable. With fifo (head-drop),
    % all publishes succeed because oldest messages are evicted to make room.
    Results = [send_message(ClusterNodePid, {msg, I}) || I <- lists:seq(1, 100)],
    true = lists:all(fun(ok) -> true; (_) -> false end, Results),

    % Re-open listener and wait for reconnect
    {ok, ListenSocket2} = gen_tcp:listen(12345, [binary, {reuseaddr, true}, {active, false}]),
    {ok, Socket2} = gen_tcp:accept(ListenSocket2, 30000),
    recv_connect(Socket2, Config),

    % Receive the surviving messages
    Messages = recv_all_messages(Socket2),
    ct:pal("fifo_head_drop: received ~p messages", [length(Messages)]),

    % The newest message must be present (it was never evicted)
    true = lists:member({msg, 100}, Messages),
    % The oldest message must have been evicted (buffer can only hold ~47 messages)
    false = lists:member({msg, 1}, Messages),
    % Messages should be in FIFO order within their queue
    true = Messages =:= lists:sort(Messages),
    ok.

%% With lifo policy, newest buffered messages are evicted when the buffer is full.
%% The incoming message is always accepted; the most recently buffered message
%% of the lowest priority class is evicted. After reconnect the oldest messages
%% (plus the very last one sent) should be present.
lifo_tail_drop_test(Config) ->
    ClusterNodePid = cluster_node_pid(Config),
    {ok, ListenSocket} = gen_tcp:listen(12345, [binary, {reuseaddr, true}, {active, false}]),
    {ok, Socket1} = gen_tcp:accept(ListenSocket, 30000),
    recv_connect(Socket1, Config),

    % Close socket and listener to make node unreachable and prevent reconnect
    gen_tcp:close(Socket1),
    gen_tcp:close(ListenSocket),
    timer:sleep(200),

    % Send 100 small messages while unreachable. With lifo (QoS-aware tail-drop),
    % all publishes succeed because the newest existing buffered messages are
    % evicted to make room for the incoming message.
    Results = [send_message(ClusterNodePid, {msg, I}) || I <- lists:seq(1, 100)],
    true = lists:all(fun(ok) -> true; (_) -> false end, Results),

    % Re-open listener and wait for reconnect
    {ok, ListenSocket2} = gen_tcp:listen(12345, [binary, {reuseaddr, true}, {active, false}]),
    {ok, Socket2} = gen_tcp:accept(ListenSocket2, 30000),
    recv_connect(Socket2, Config),

    % Receive the surviving messages
    Messages = recv_all_messages(Socket2),
    ct:pal("lifo_tail_drop: received ~p messages: ~p", [length(Messages), Messages]),

    % The oldest messages must be present (they were buffered first and never evicted)
    true = lists:member({msg, 1}, Messages),
    true = lists:member({msg, 2}, Messages),
    % The very last message must be present (it evicted the previous newest)
    true = lists:member({msg, 100}, Messages),
    % Middle messages that were evicted to make room should be absent
    false = lists:member({msg, 50}, Messages),
    ok.

%% With fifo policy, QoS 0 messages are evicted before QoS 1/2.
%% This test sends a mix of QoS 0 and QoS 1 messages via the enqueue path
%% and verifies that QoS 1 messages survive while QoS 0 are evicted first.
fifo_qos_priority_test(Config) ->
    Node = proplists:get_value(node, Config),
    ClusterNodePid = cluster_node_pid(Config),
    {ok, ListenSocket} = gen_tcp:listen(12345, [binary, {reuseaddr, true}, {active, false}]),
    {ok, Socket1} = gen_tcp:accept(ListenSocket, 30000),
    recv_connect(Socket1, Config),

    % Close socket and listener to make node unreachable
    gen_tcp:close(Socket1),
    gen_tcp:close(ListenSocket),
    timer:sleep(200),

    % Send a mix of QoS 0 and QoS 1 messages via the enqueue path.
    % enqueue_many terms allow extract_qos to classify the QoS level.
    SubId = {<<"">>, <<"test_client">>},
    SendEnq = fun(QoS, Id) ->
        Term = {enqueue_many, SubId, [{deliver, QoS, Id}], #{}},
        rpc:call(
            Node, vmq_cluster_node, enqueue,
            [ClusterNodePid, Term, true, 5000]
        )
    end,

    % Send 40 QoS 0 messages, then 40 QoS 1 messages.
    % Buffer is 1000 bytes; each message is ~50-60 bytes framed+serialized.
    % Total ~4000-5000 bytes, well over the buffer limit.
    [SendEnq(0, {qos0, I}) || I <- lists:seq(1, 40)],
    [SendEnq(1, {qos1, I}) || I <- lists:seq(1, 40)],

    % Re-open listener and wait for reconnect
    {ok, ListenSocket2} = gen_tcp:listen(12345, [binary, {reuseaddr, true}, {active, false}]),
    {ok, Socket2} = gen_tcp:accept(ListenSocket2, 30000),
    recv_connect(Socket2, Config),

    % Receive the surviving messages (they are enq-framed, not msg-framed)
    EnqMessages = recv_all_enq_messages(Socket2),
    ct:pal("fifo_qos_priority: received ~p enq messages", [length(EnqMessages)]),

    % Extract the Terms from the deserialized enqueue messages
    Terms = [T || {_CallerPid, _Ref, T} <- EnqMessages],
    % Extract the deliver payloads
    Payloads = [Payload || {enqueue_many, _, [{deliver, _, Payload}], _} <- Terms],
    ct:pal("fifo_qos_priority: payloads = ~p", [Payloads]),

    % Count surviving QoS 0 vs QoS 1 messages
    QoS0Count = length([P || {qos0, _} = P <- Payloads]),
    QoS1Count = length([P || {qos1, _} = P <- Payloads]),
    ct:pal("fifo_qos_priority: qos0=~p, qos1=~p", [QoS0Count, QoS1Count]),

    % QoS 1 messages should dominate the surviving buffer.
    % With fifo, QoS 0 are evicted first, so more QoS 1 messages survive.
    true = QoS1Count > QoS0Count,
    ok.

send_until_tcp_buffer_full(ClusterNodePid) ->
    send_until_tcp_buffer_full(ClusterNodePid, 0).
send_until_tcp_buffer_full(ClusterNodePid, MsgsAcc) ->
    % the only way we detect that the buffer is full is that vmq_cluster_node will close
    % the connection and will reconnect
    case send_message(ClusterNodePid, <<1:10000, MsgsAcc:32>>) of
        ok ->
            send_until_tcp_buffer_full(ClusterNodePid, MsgsAcc + 1);
        {error, msg_dropped} ->
            MsgsAcc - 1
    end.

recv_until_tcp_buffer_empty(Socket, N) ->
    recv_until_tcp_buffer_empty(Socket, 0, N).

recv_until_tcp_buffer_empty(Socket, I, N) when I =< N ->
    recv_message(Socket, <<1:10000, I:32>>),
    recv_until_tcp_buffer_empty(Socket, I + 1, N);
recv_until_tcp_buffer_empty(_, _, _) ->
    ok.

setup_mock_vmq_cluster_node(Config, DropPolicy) ->
    Node = proplists:get_value(node, Config),
    % make the test_com1 node connect to myself
    ok = rpc:block_call(Node, vmq_config, set_env, [
        outgoing_connect_options, [{keepalive, true}, {send_timeout, 0}], false
    ]),
    ok = rpc:block_call(Node, vmq_config, set_env, [
        outgoing_connect_params_module, ?MODULE, false
    ]),
    ok = rpc:block_call(Node, vmq_config, set_env, [
        outgoing_connect_timeout, 1000, false
    ]),
    ok = rpc:block_call(Node, vmq_config, set_env, [
        outgoing_clustering_buffer_size, 1000, false
    ]),
    ok = rpc:block_call(Node, vmq_config, set_env, [
        outgoing_clustering_buffer_drop_policy, DropPolicy, false
    ]),
    %% Ensure pool ETS table exists (test bypasses supervisor init)
    rpc:block_call(Node, ?MODULE, ensure_pool_table, []),
    {ok, ClusterNodePid} = rpc:block_call(Node, vmq_cluster_node, start_link, [node(), 0]),
    ClusterNodePid.

ensure_pool_table() ->
    case ets:info(vmq_cluster_node_pool) of
        undefined ->
            ets:new(vmq_cluster_node_pool,
                    [public, set, named_table, {read_concurrency, true}]),
            ets:insert(vmq_cluster_node_pool, {pool_size, 1});
        _ ->
            ok
    end.

terminate_mock_vmq_cluster_node(Config) ->
    Node = proplists:get_value(node, Config),
    ClusterNodePid = cluster_node_pid(Config),
    rpc:block_call(Node, erlang, exit, [ClusterNodePid, kill]).

cluster_node_pid(Config) ->
    proplists:get_value(cluster_node_pid, Config).

recv_connect(Socket, Config) ->
    Node = proplists:get_value(node, Config),
    NodeName = term_to_binary(Node),
    L1 = byte_size(NodeName),
    HandshakeMsg = <<"vmq-connect", L1:32, NodeName/binary>>,
    {ok, HandshakeMsg} = gen_tcp:recv(Socket, byte_size(HandshakeMsg)),
    ok.

send_message(ClusterNodePid, Msg) ->
    rpc:call(node(ClusterNodePid), vmq_cluster_node, publish, [ClusterNodePid, Msg]).

recv_message(Socket, Term) ->
    TermBin = term_to_binary(Term),
    L = byte_size(TermBin),
    Msg = <<"msg", L:32, TermBin/binary>>,
    BatchMsg = <<"vmq-send", (byte_size(Msg)):32, Msg/binary>>,
    case gen_tcp:recv(Socket, byte_size(BatchMsg)) of
        {ok, BatchMsg} ->
            ok;
        E ->
            io:format(user, "got ~p instead of ~p~n", [E, {ok, BatchMsg}]),
            E
    end.

%% Receive a batch of "msg"-framed messages and return a list of deserialized terms.
recv_all_messages(Socket) ->
    {ok, <<"vmq-send", L:32>>} = gen_tcp:recv(Socket, 12, 5000),
    {ok, Payload} = gen_tcp:recv(Socket, L, 5000),
    parse_msg_frames(Payload, []).

parse_msg_frames(<<>>, Acc) ->
    lists:reverse(Acc);
parse_msg_frames(<<"msg", L:32, Rest/binary>>, Acc) ->
    <<TermBin:L/binary, Remaining/binary>> = Rest,
    Term = binary_to_term(TermBin),
    parse_msg_frames(Remaining, [Term | Acc]).

%% Receive a batch of "enq"-framed messages and return a list of
%% deserialized {CallerPid, Ref, Term} tuples.
recv_all_enq_messages(Socket) ->
    {ok, <<"vmq-send", L:32>>} = gen_tcp:recv(Socket, 12, 5000),
    {ok, Payload} = gen_tcp:recv(Socket, L, 5000),
    parse_enq_frames(Payload, []).

parse_enq_frames(<<>>, Acc) ->
    lists:reverse(Acc);
parse_enq_frames(<<"enq", L:32, Rest/binary>>, Acc) ->
    <<TermBin:L/binary, Remaining/binary>> = Rest,
    Term = binary_to_term(TermBin),
    parse_enq_frames(Remaining, [Term | Acc]).
