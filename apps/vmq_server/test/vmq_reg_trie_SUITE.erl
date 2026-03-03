-module(vmq_reg_trie_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("common_test/include/ct.hrl").
-include("../src/vmq_reg_trie.hrl").

%%--------------------------------------------------------------------
%% COMMON TEST CALLBACK FUNCTIONS
%%--------------------------------------------------------------------

suite() ->
    [{timetrap, {minutes, 5}}].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    ok = vmq_test_utils:setup(),
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok = vmq_test_utils:teardown(),
    ok.

groups() ->
    [].

all() ->
    [
        add_and_inc_test,
        rem_and_dec_test,
        worker_name_test,
        worker_routing_consistency_test,
        worker_routing_distribution_test,
        single_subscriber_add_remove_test,
        wildcard_hash_subscriber_test,
        wildcard_plus_subscriber_test,
        shared_subscription_test,
        fanout_multiple_subscribers_test,
        remote_subscriber_test,
        fold_read_path_test,
        fold_with_wildcards_test,
        concurrent_add_remove_test,
        persistent_term_readiness_test,
        supervisor_tree_structure_test,
        table_owner_holds_tables_test,
        del_trie_subs_consistency_test
    ].

%%--------------------------------------------------------------------
%% UNIT TESTS — pure helper functions
%%--------------------------------------------------------------------

add_and_inc_test(_Config) ->
    %% Adding to empty list
    [{a, 1}] = vmq_reg_trie_worker:add_and_inc(a, []),
    %% Adding new node to existing list
    [{b, 1}, {a, 1}] = vmq_reg_trie_worker:add_and_inc(b, [{a, 1}]),
    %% Incrementing existing node
    [{a, 2}] = vmq_reg_trie_worker:add_and_inc(a, [{a, 1}]),
    [{a, 1}, {b, 3}] = vmq_reg_trie_worker:add_and_inc(b, [{a, 1}, {b, 2}]),
    ok.

rem_and_dec_test(_Config) ->
    %% Decrementing from count > 1
    [{a, 1}] = vmq_reg_trie_worker:rem_and_dec(a, [{a, 2}]),
    %% Removing when count == 1
    [] = vmq_reg_trie_worker:rem_and_dec(a, [{a, 1}]),
    [{b, 1}] = vmq_reg_trie_worker:rem_and_dec(a, [{a, 1}, {b, 1}]),
    %% Removing non-existent node
    [{a, 1}] = vmq_reg_trie_worker:rem_and_dec(b, [{a, 1}]),
    ok.

worker_name_test(_Config) ->
    vmq_reg_trie_worker_0 = vmq_reg_trie_worker:worker_name(0),
    vmq_reg_trie_worker_7 = vmq_reg_trie_worker:worker_name(7),
    vmq_reg_trie_worker_99 = vmq_reg_trie_worker:worker_name(99),
    ok.

%%--------------------------------------------------------------------
%% WORKER ROUTING TESTS
%%--------------------------------------------------------------------

worker_routing_consistency_test(_Config) ->
    %% Same subscriber always routes to same worker
    NumWorkers = 8,
    SubId = {"mp", <<"client1">>},
    Worker1 = erlang:phash2(SubId, NumWorkers),
    Worker2 = erlang:phash2(SubId, NumWorkers),
    Worker3 = erlang:phash2(SubId, NumWorkers),
    true = (Worker1 =:= Worker2),
    true = (Worker2 =:= Worker3),
    ok.

worker_routing_distribution_test(_Config) ->
    %% Verify events distribute across workers (not all to one)
    NumWorkers = 8,
    SubIds = [{"mp", integer_to_binary(I)} || I <- lists:seq(1, 1000)],
    Workers = [erlang:phash2(SubId, NumWorkers) || SubId <- SubIds],
    WorkerSet = lists:usort(Workers),
    %% With 1000 subscribers and 8 workers, all workers should get at least one
    true = (length(WorkerSet) =:= NumWorkers),
    ok.

%%--------------------------------------------------------------------
%% SUBSCRIPTION LIFECYCLE TESTS
%%--------------------------------------------------------------------

single_subscriber_add_remove_test(_Config) ->
    MP = "test_mp",
    Topic = [<<"a">>, <<"b">>],
    SubId = {MP, <<"1">>},
    SubInfo = 0,

    %% Add subscriber
    Event = updated_event(MP, 1, [{Topic, SubInfo}]),
    sync_event(Event),

    %% Verify via fold
    Result = vmq_reg_trie:fold(
        SubId, Topic, fun(E, _, Acc) -> [E | Acc] end, []
    ),
    [{SubId, SubInfo}] = Result,

    %% Delete subscriber
    DelEvent = deleted_event(MP, 1, [{Topic, SubInfo}]),
    sync_event(DelEvent),

    %% Verify removed
    [] = vmq_reg_trie:fold(
        SubId, Topic, fun(E, _, Acc) -> [E | Acc] end, []
    ),
    ok.

wildcard_hash_subscriber_test(_Config) ->
    MP = "test_mp",
    WildcardTopic = [<<"a">>, <<"#">>],
    PublishTopic = [<<"a">>, <<"b">>, <<"c">>],
    SubId = {MP, <<"1">>},
    SubInfo = 0,

    %% Add wildcard subscriber
    Event = updated_event(MP, 1, [{WildcardTopic, SubInfo}]),
    sync_event(Event),

    %% Publish to matching topic should find the subscriber
    Result = vmq_reg_trie:fold(
        {MP, <<"publisher">>}, PublishTopic, fun(E, _, Acc) -> [E | Acc] end, []
    ),
    [{SubId, SubInfo}] = Result,

    %% Cleanup
    sync_event(deleted_event(MP, 1, [{WildcardTopic, SubInfo}])),
    ok.

wildcard_plus_subscriber_test(_Config) ->
    MP = "test_mp",
    WildcardTopic = [<<"a">>, <<"+">>, <<"c">>],
    MatchTopic = [<<"a">>, <<"b">>, <<"c">>],
    NoMatchTopic = [<<"a">>, <<"b">>, <<"d">>],
    SubInfo = 0,

    Event = updated_event(MP, 1, [{WildcardTopic, SubInfo}]),
    sync_event(Event),

    %% Should match a/b/c
    [_] = vmq_reg_trie:fold(
        {MP, <<"pub">>}, MatchTopic, fun(E, _, Acc) -> [E | Acc] end, []
    ),

    %% Should NOT match a/b/d
    [] = vmq_reg_trie:fold(
        {MP, <<"pub">>}, NoMatchTopic, fun(E, _, Acc) -> [E | Acc] end, []
    ),

    sync_event(deleted_event(MP, 1, [{WildcardTopic, SubInfo}])),
    ok.

shared_subscription_test(_Config) ->
    MP = "test_mp",
    Group = <<"mygroup">>,
    Topic = [<<"shared">>, <<"topic">>],
    SharedTopic = [<<"$share">>, Group | Topic],
    SubInfo = 0,

    %% Add two shared subscribers in same group
    sync_event(updated_event(MP, 1, [{SharedTopic, SubInfo}])),
    sync_event(updated_event(MP, 2, [{SharedTopic, SubInfo}])),

    %% fold should find shared subscribers
    Result = vmq_reg_trie:fold(
        {MP, <<"pub">>}, Topic, fun(E, _, Acc) -> [E | Acc] end, []
    ),
    2 = length(Result),

    %% Cleanup
    sync_event(deleted_event(MP, 1, [{SharedTopic, SubInfo}])),
    sync_event(deleted_event(MP, 2, [{SharedTopic, SubInfo}])),
    ok.

fanout_multiple_subscribers_test(_Config) ->
    MP = "test_mp",
    Topic = [<<"fanout">>, <<"topic">>],
    SubInfo = 0,
    NumSubs = 100,

    %% Add many subscribers to same topic
    lists:foreach(
        fun(I) ->
            sync_event(updated_event(MP, I, [{Topic, SubInfo}]))
        end,
        lists:seq(1, NumSubs)
    ),

    %% fold should find all subscribers
    Result = vmq_reg_trie:fold(
        {MP, <<"pub">>}, Topic, fun(E, _, Acc) -> [E | Acc] end, []
    ),
    NumSubs = length(Result),

    %% Remove all
    lists:foreach(
        fun(I) ->
            sync_event(deleted_event(MP, I, [{Topic, SubInfo}]))
        end,
        lists:seq(1, NumSubs)
    ),

    %% Verify all removed
    [] = vmq_reg_trie:fold(
        {MP, <<"pub">>}, Topic, fun(E, _, Acc) -> [E | Acc] end, []
    ),
    ok.

remote_subscriber_test(_Config) ->
    MP = "test_mp",
    Topic = [<<"remote">>, <<"#">>],
    SubInfo = 0,
    FakeRemoteNode = 'remote@fake',

    %% Directly exercise the worker's remote subscriber path by sending
    %% an init_entry that looks like a remote subscription
    WorkerId = erlang:phash2({MP, <<"remote_client">>}, 8),
    Entry = {MP, Topic, {{MP, <<"remote_client">>}, SubInfo, FakeRemoteNode, false}},
    vmq_reg_trie_worker:init_entry(WorkerId, Entry),

    %% Give worker time to process
    sync_worker(WorkerId),

    %% Verify remote subscriber is in the ETS table
    Key = {MP, Topic},
    [{_, Remotes}] = ets:lookup(vmq_trie_remote_subs, Key),
    true = lists:keymember(FakeRemoteNode, 1, Remotes),
    ok.

%%--------------------------------------------------------------------
%% FOLD READ PATH TESTS
%%--------------------------------------------------------------------

fold_read_path_test(_Config) ->
    MP = "test_mp",
    %% Add subscribers on different topics
    sync_event(updated_event(MP, 1, [{[<<"x">>, <<"y">>], 0}])),
    sync_event(updated_event(MP, 2, [{[<<"x">>, <<"z">>], 1}])),

    %% fold on x/y should only find client 1
    R1 = vmq_reg_trie:fold(
        {MP, <<"pub">>}, [<<"x">>, <<"y">>], fun(E, _, Acc) -> [E | Acc] end, []
    ),
    [{{MP, <<"1">>}, 0}] = R1,

    %% fold on x/z should only find client 2
    R2 = vmq_reg_trie:fold(
        {MP, <<"pub">>}, [<<"x">>, <<"z">>], fun(E, _, Acc) -> [E | Acc] end, []
    ),
    [{{MP, <<"2">>}, 1}] = R2,

    %% Cleanup
    sync_event(deleted_event(MP, 1, [{[<<"x">>, <<"y">>], 0}])),
    sync_event(deleted_event(MP, 2, [{[<<"x">>, <<"z">>], 1}])),
    ok.

fold_with_wildcards_test(_Config) ->
    MP = "test_mp",
    %% Add wildcard and exact subscriptions
    sync_event(updated_event(MP, 1, [{[<<"a">>, <<"#">>], 0}])),
    sync_event(updated_event(MP, 2, [{[<<"a">>, <<"b">>], 0}])),

    %% fold on a/b should find both: wildcard match + exact match
    Result = vmq_reg_trie:fold(
        {MP, <<"pub">>}, [<<"a">>, <<"b">>], fun(E, _, Acc) -> [E | Acc] end, []
    ),
    2 = length(Result),
    SubIds = [SubId || {SubId, _} <- Result],
    true = lists:member({MP, <<"1">>}, SubIds),
    true = lists:member({MP, <<"2">>}, SubIds),

    %% fold on a/b/c should only find wildcard subscriber
    R2 = vmq_reg_trie:fold(
        {MP, <<"pub">>},
        [<<"a">>, <<"b">>, <<"c">>],
        fun(E, _, Acc) -> [E | Acc] end,
        []
    ),
    [{{MP, <<"1">>}, 0}] = R2,

    sync_event(deleted_event(MP, 1, [{[<<"a">>, <<"#">>], 0}])),
    sync_event(deleted_event(MP, 2, [{[<<"a">>, <<"b">>], 0}])),
    ok.

%%--------------------------------------------------------------------
%% CONCURRENT TESTS
%%--------------------------------------------------------------------

concurrent_add_remove_test(_Config) ->
    MP = "test_mp",
    Topic = [<<"concurrent">>, <<"#">>],
    SubInfo = 0,
    NumProcs = 50,
    NumOpsPerProc = 20,

    Parent = self(),
    %% Spawn many processes that concurrently add and remove subscribers
    Pids = [
        spawn_link(fun() ->
            lists:foreach(
                fun(J) ->
                    ClientId = I * 1000 + J,
                    sync_event(updated_event(MP, ClientId, [{Topic, SubInfo}])),
                    sync_event(deleted_event(MP, ClientId, [{Topic, SubInfo}]))
                end,
                lists:seq(1, NumOpsPerProc)
            ),
            Parent ! {done, self()}
        end)
     || I <- lists:seq(1, NumProcs)
    ],

    %% Wait for all to finish
    lists:foreach(
        fun(Pid) ->
            receive
                {done, Pid} -> ok
            after 30000 ->
                ct:fail({timeout, Pid})
            end
        end,
        Pids
    ),

    %% After all add+remove pairs, trie_subs and trie_subs_fanout should be empty
    %% for the concurrent topic. Give workers time to drain.
    timer:sleep(100),
    0 = ets:select_count(vmq_trie_subs_fanout, [
        {{{{MP, Topic}, '_'}}, [], [true]}
    ]),
    ok.

%%--------------------------------------------------------------------
%% PERSISTENT TERM READINESS TESTS
%%--------------------------------------------------------------------

persistent_term_readiness_test(_Config) ->
    %% After setup, the trie should be ready
    1 = persistent_term:get(subscribe_trie_ready, 0),
    ok.

%%--------------------------------------------------------------------
%% SUPERVISOR STRUCTURE TESTS
%%--------------------------------------------------------------------

supervisor_tree_structure_test(_Config) ->
    %% Verify the supervisor tree is correct
    Children = supervisor:which_children(vmq_reg_trie_sup),

    %% Should have 3 children: table_owner, coordinator, worker_sup
    3 = length(Children),

    %% Verify each child exists and has correct type
    {vmq_reg_trie_table_owner, TableOwnerPid, worker, _} =
        lists:keyfind(vmq_reg_trie_table_owner, 1, Children),
    true = is_pid(TableOwnerPid),

    {vmq_reg_trie, CoordPid, worker, _} =
        lists:keyfind(vmq_reg_trie, 1, Children),
    true = is_pid(CoordPid),

    {vmq_reg_trie_worker_sup, WorkerSupPid, supervisor, _} =
        lists:keyfind(vmq_reg_trie_worker_sup, 1, Children),
    true = is_pid(WorkerSupPid),

    %% Verify worker_sup has the right number of workers
    Workers = supervisor:which_children(vmq_reg_trie_worker_sup),
    NumWorkers = application:get_env(vmq_server, reg_trie_workers, 8),
    NumWorkers = length(Workers),

    %% Verify all workers are alive
    lists:foreach(
        fun({_Name, Pid, worker, _}) ->
            true = is_pid(Pid)
        end,
        Workers
    ),

    %% Verify rest_for_one strategy
    {ok, {SupFlags, _ChildSpecs}} =
        vmq_reg_trie_sup:init([]),
    {rest_for_one, 5, 10} = SupFlags,
    ok.

table_owner_holds_tables_test(_Config) ->
    %% Verify the table owner process owns all 6 ETS tables
    TableOwnerPid = whereis(vmq_reg_trie_table_owner),
    true = is_pid(TableOwnerPid),

    Tables = [
        vmq_trie,
        vmq_trie_node,
        vmq_trie_topic,
        vmq_trie_subs,
        vmq_trie_subs_fanout,
        vmq_trie_remote_subs
    ],
    lists:foreach(
        fun(Table) ->
            TableOwnerPid = ets:info(Table, owner)
        end,
        Tables
    ),
    ok.

%%--------------------------------------------------------------------
%% DEL_TRIE_SUBS CONSISTENCY TEST
%%--------------------------------------------------------------------

del_trie_subs_consistency_test(_Config) ->
    %% Test that after adding and removing a subscriber, the marker is
    %% properly cleaned up
    MP = "test_mp",
    Topic = [<<"del_test">>],
    SubInfo = 0,

    %% Add subscriber
    sync_event(updated_event(MP, 1, [{Topic, SubInfo}])),

    %% Verify marker exists
    Key = {MP, Topic},
    [{Key, fanout}] = ets:lookup(vmq_trie_subs, Key),

    %% Remove subscriber
    sync_event(deleted_event(MP, 1, [{Topic, SubInfo}])),

    %% Verify marker is removed
    [] = ets:lookup(vmq_trie_subs, Key),
    %% Verify fanout is empty
    0 = ets:select_count(vmq_trie_subs_fanout, [{{{Key, '_'}}, [], [true]}]),
    ok.

%%--------------------------------------------------------------------
%% HELPERS
%%--------------------------------------------------------------------

updated_event(MP, ClientIdInt, Topics) ->
    {updated, {vmq, subscriber}, {MP, integer_to_binary(ClientIdInt)}, undefined, [
        {node(), true, Topics}
    ]}.

deleted_event(MP, ClientIdInt, Topics) ->
    {deleted, {vmq, subscriber}, {MP, integer_to_binary(ClientIdInt)}, [{node(), true, Topics}]}.

%% Send an event through the coordinator and wait for it to be fully
%% processed. The coordinator dispatches to workers via async `!`, so
%% we must also drain all workers before returning.
sync_event(Event) ->
    Hour = 1000 * 3600,
    ok = gen_server:call(vmq_reg_trie, {event, Event}, Hour),
    sync_all_workers().

%% Ensure a specific worker has processed all pending messages.
sync_worker(WorkerId) ->
    Name = vmq_reg_trie_worker:worker_name(WorkerId),
    _ = sys:get_state(Name),
    ok.

%% Drain all workers so any pending async messages are processed.
sync_all_workers() ->
    NumWorkers = application:get_env(vmq_server, reg_trie_workers, 8),
    lists:foreach(fun sync_worker/1, lists:seq(0, NumWorkers - 1)).
