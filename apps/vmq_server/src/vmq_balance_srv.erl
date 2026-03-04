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

-module(vmq_balance_srv).
-include_lib("kernel/include/logger.hrl").

-behaviour(gen_server).

%% API
-export([
    start_link/0,
    is_accepting/0,
    balance_stats/0,
    get_node_counts/0,
    incr_rejections/0
]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-define(SERVER, ?MODULE).
-define(DEFAULT_CHECK_INTERVAL, 5000).
-define(DEFAULT_THRESHOLD, 1.2).
-define(DEFAULT_HYSTERESIS, 0.1).
-define(DEFAULT_MIN_CONNECTIONS, 100).
-define(RPC_TIMEOUT, 2000).

-record(state, {
    local_count = 0 :: non_neg_integer(),
    cluster_avg = 0.0 :: float(),
    node_counts = #{} :: #{node() => non_neg_integer()},
    accepting = true :: boolean(),
    enabled = false :: boolean(),
    threshold = ?DEFAULT_THRESHOLD :: float(),
    hysteresis = ?DEFAULT_HYSTERESIS :: float(),
    min_connections = ?DEFAULT_MIN_CONNECTIONS :: non_neg_integer(),
    check_interval = ?DEFAULT_CHECK_INTERVAL :: pos_integer(),
    tref :: reference() | undefined,
    rejection_count = 0 :: non_neg_integer(),
    hooks_registered = false :: boolean()
}).

%%%===================================================================
%%% API
%%%===================================================================

-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

-spec is_accepting() -> boolean().
is_accepting() ->
    persistent_term:get({vmq_balance_srv, accepting}, true).

-spec balance_stats() ->
    {
        IsAccepting :: non_neg_integer(),
        LocalConnections :: non_neg_integer(),
        ClusterAvg :: non_neg_integer(),
        IsEnabled :: non_neg_integer(),
        RejectionCount :: non_neg_integer()
    }.
balance_stats() ->
    try
        gen_server:call(?SERVER, balance_stats, 1000)
    catch
        exit:{timeout, _} -> {1, 0, 0, 0, 0};
        exit:{noproc, _} -> {1, 0, 0, 0, 0};
        _:_ -> {1, 0, 0, 0, 0}
    end.

-spec get_node_counts() -> #{node() => non_neg_integer()}.
get_node_counts() ->
    try
        gen_server:call(?SERVER, get_node_counts, 2000)
    catch
        Class:Reason ->
            ?LOG_WARNING("get_node_counts failed: ~p:~p", [Class, Reason]),
            #{node() => 0}
    end.

-spec incr_rejections() -> ok.
incr_rejections() ->
    gen_server:cast(?SERVER, incr_rejections).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
    process_flag(trap_exit, true),
    Enabled = vmq_config:get_env(balance_enabled, false),
    RejectEnabled = vmq_config:get_env(balance_reject_enabled, false),
    Threshold = parse_float_env(balance_threshold, ?DEFAULT_THRESHOLD),
    Hysteresis = parse_float_env(balance_hysteresis, ?DEFAULT_HYSTERESIS),
    MinConnections = vmq_config:get_env(balance_min_connections, ?DEFAULT_MIN_CONNECTIONS),
    CheckInterval = vmq_config:get_env(balance_check_interval, ?DEFAULT_CHECK_INTERVAL),
    TRef = schedule_check(CheckInterval),
    ?LOG_INFO(
        "starting: enabled=~p, reject_enabled=~p, threshold=~p, hysteresis=~p, "
        "min_connections=~p, check_interval=~pms",
        [Enabled, RejectEnabled, Threshold, Hysteresis, MinConnections, CheckInterval]
    ),
    %% Initialize persistent_term so is_accepting/0 returns the correct
    %% value before the first balance check runs.
    persistent_term:put({vmq_balance_srv, accepting}, true),
    HooksRegistered = maybe_update_hooks(false, Enabled andalso RejectEnabled),
    {ok, #state{
        enabled = Enabled,
        threshold = Threshold,
        hysteresis = Hysteresis,
        min_connections = MinConnections,
        check_interval = CheckInterval,
        tref = TRef,
        hooks_registered = HooksRegistered
    }}.

handle_call(is_accepting, _From, #state{accepting = Accepting, enabled = Enabled} = State) ->
    %% When disabled, always accept
    Result = (not Enabled) orelse Accepting,
    {reply, Result, State};
handle_call(balance_stats, _From, #state{} = State) ->
    #state{
        accepting = Accepting,
        local_count = LocalCount,
        cluster_avg = ClusterAvg,
        enabled = Enabled,
        rejection_count = RejectionCount
    } = State,
    EffectiveAccepting = (not Enabled) orelse Accepting,
    Reply = {
        bool_to_int(EffectiveAccepting),
        LocalCount,
        round(ClusterAvg),
        bool_to_int(Enabled),
        RejectionCount
    },
    {reply, Reply, State};
handle_call(get_node_counts, _From, #state{node_counts = NodeCounts} = State) ->
    {reply, NodeCounts, State};
handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(incr_rejections, #state{rejection_count = Count} = State) ->
    {noreply, State#state{rejection_count = Count + 1}};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(check_balance, State) ->
    Enabled = vmq_config:get_env(balance_enabled, false),
    RejectEnabled = vmq_config:get_env(balance_reject_enabled, false),
    Interval = vmq_config:get_env(balance_check_interval, ?DEFAULT_CHECK_INTERVAL),
    ShouldHaveHooks = Enabled andalso RejectEnabled,
    NewHooksRegistered = maybe_update_hooks(State#state.hooks_registered, ShouldHaveHooks),
    NewState =
        case Enabled of
            true -> do_balance_check(State#state{enabled = true});
            false ->
                %% When disabled, always accept — update persistent_term.
                persistent_term:put({vmq_balance_srv, accepting}, true),
                State#state{enabled = false, accepting = true}
        end,
    TRef = schedule_check(Interval),
    {noreply, NewState#state{
        check_interval = Interval,
        tref = TRef,
        hooks_registered = NewHooksRegistered
    }};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{tref = TRef, hooks_registered = true}) ->
    cancel_timer(TRef),
    log_plugin_result(
        "disable",
        auth_on_register,
        vmq_plugin_mgr:disable_module_plugin(vmq_balance_hook, auth_on_register, 5)
    ),
    log_plugin_result(
        "disable",
        auth_on_register_m5,
        vmq_plugin_mgr:disable_module_plugin(vmq_balance_hook, auth_on_register_m5, 6)
    ),
    ok;
terminate(_Reason, #state{tref = TRef}) ->
    cancel_timer(TRef),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @doc Register or deregister balance hooks based on state transitions.
%% Only calls enable/disable on actual transitions to avoid redundant work.
maybe_update_hooks(true, true) ->
    true;
maybe_update_hooks(false, false) ->
    false;
maybe_update_hooks(false, true) ->
    R1 = vmq_plugin_mgr:enable_module_plugin(vmq_balance_hook, auth_on_register, 5),
    R2 = vmq_plugin_mgr:enable_module_plugin(vmq_balance_hook, auth_on_register_m5, 6),
    log_plugin_result("enable", auth_on_register, R1),
    log_plugin_result("enable", auth_on_register_m5, R2),
    R1 =:= ok andalso R2 =:= ok;
maybe_update_hooks(true, false) ->
    log_plugin_result(
        "disable",
        auth_on_register,
        vmq_plugin_mgr:disable_module_plugin(vmq_balance_hook, auth_on_register, 5)
    ),
    log_plugin_result(
        "disable",
        auth_on_register_m5,
        vmq_plugin_mgr:disable_module_plugin(vmq_balance_hook, auth_on_register_m5, 6)
    ),
    false.

log_plugin_result(_Action, _Hook, ok) ->
    ok;
log_plugin_result(Action, Hook, {error, Reason}) ->
    ?LOG_WARNING("failed to ~s hook ~p: ~p", [Action, Hook, Reason]).

do_balance_check(State) ->
    #state{
        threshold = Threshold,
        hysteresis = Hysteresis,
        min_connections = MinConnections,
        accepting = WasAccepting
    } = State,

    LocalCount = get_local_connection_count(),
    {NodeCounts, ClusterAvg, TotalConnections, NumNodes} = gather_cluster_counts(LocalCount),

    NewAccepting = compute_accepting(
        WasAccepting,
        LocalCount,
        ClusterAvg,
        TotalConnections,
        Threshold,
        Hysteresis,
        MinConnections,
        NumNodes
    ),

    case WasAccepting =/= NewAccepting of
        true ->
            %% Update persistent_term so is_accepting/0 reads the new value
            %% without going through gen_server:call.
            EffectiveAccepting = (not State#state.enabled) orelse NewAccepting,
            persistent_term:put({vmq_balance_srv, accepting}, EffectiveAccepting),
            ?LOG_WARNING(
                "accepting changed ~p -> ~p "
                "(local=~p, avg=~.1f, total=~p, nodes=~p)",
                [
                    WasAccepting,
                    NewAccepting,
                    LocalCount,
                    ClusterAvg,
                    TotalConnections,
                    NumNodes
                ]
            );
        false ->
            ok
    end,

    State#state{
        local_count = LocalCount,
        cluster_avg = ClusterAvg,
        node_counts = NodeCounts,
        accepting = NewAccepting
    }.

-spec get_local_connection_count() -> non_neg_integer().
get_local_connection_count() ->
    try
        {MQTTCount, WSCount} = vmq_ranch_sup:active_mqtt_connections(),
        MQTTCount + WSCount
    catch
        _:_ -> 0
    end.

-spec gather_cluster_counts(non_neg_integer()) ->
    {#{node() => non_neg_integer()}, float(), non_neg_integer(), pos_integer()}.
gather_cluster_counts(LocalCount) ->
    OtherNodes = [N || N <- vmq_cluster_nodes(), N =/= node()],
    RemoteCounts =
        case OtherNodes of
            [] ->
                #{};
            _ ->
                {Results, _BadNodes} = rpc:multicall(
                    OtherNodes,
                    vmq_ranch_sup,
                    active_mqtt_connections,
                    [],
                    ?RPC_TIMEOUT
                ),
                collect_remote_counts(OtherNodes, Results)
        end,
    AllCounts = RemoteCounts#{node() => LocalCount},
    TotalConnections = maps:fold(fun(_N, C, Acc) -> Acc + C end, 0, AllCounts),
    NumNodes = map_size(AllCounts),
    ClusterAvg =
        case NumNodes of
            0 -> 0.0;
            _ -> TotalConnections / NumNodes
        end,
    {AllCounts, ClusterAvg, TotalConnections, NumNodes}.

collect_remote_counts(Nodes, Results) ->
    collect_remote_counts(Nodes, Results, #{}).

collect_remote_counts([], _, Acc) ->
    Acc;
collect_remote_counts([_Node | RestNodes], [], Acc) ->
    %% Fewer results than nodes (shouldn't happen with multicall, but be safe)
    collect_remote_counts(RestNodes, [], Acc);
collect_remote_counts([Node | RestNodes], [{badrpc, Reason} | RestResults], Acc) ->
    ?LOG_DEBUG("RPC to ~p failed: ~p", [Node, Reason]),
    collect_remote_counts(RestNodes, RestResults, Acc);
collect_remote_counts([Node | RestNodes], [{MQTTCount, WSCount} | RestResults], Acc) when
    is_integer(MQTTCount), is_integer(WSCount)
->
    collect_remote_counts(RestNodes, RestResults, Acc#{Node => MQTTCount + WSCount});
collect_remote_counts([Node | RestNodes], [_Other | RestResults], Acc) ->
    ?LOG_DEBUG("unexpected RPC result from ~p", [Node]),
    collect_remote_counts(RestNodes, RestResults, Acc).

-spec compute_accepting(
    boolean(),
    non_neg_integer(),
    float(),
    non_neg_integer(),
    float(),
    float(),
    non_neg_integer(),
    pos_integer()
) -> boolean().
compute_accepting(
    _WasAccepting,
    _LocalCount,
    _ClusterAvg,
    _TotalConnections,
    _Threshold,
    _Hysteresis,
    _MinConnections,
    NumNodes
) when NumNodes =< 1 ->
    %% Single node or no nodes — always accept
    true;
compute_accepting(
    _WasAccepting,
    _LocalCount,
    _ClusterAvg,
    TotalConnections,
    _Threshold,
    _Hysteresis,
    MinConnections,
    _NumNodes
) when TotalConnections < MinConnections ->
    %% Below minimum — always accept
    true;
compute_accepting(
    true = _WasAccepting,
    LocalCount,
    ClusterAvg,
    _TotalConnections,
    Threshold,
    _Hysteresis,
    _MinConnections,
    _NumNodes
) ->
    %% Currently accepting: stop accepting when above threshold
    LocalCount =< ClusterAvg * Threshold;
compute_accepting(
    false = _WasAccepting,
    LocalCount,
    ClusterAvg,
    _TotalConnections,
    Threshold,
    Hysteresis,
    _MinConnections,
    _NumNodes
) ->
    %% Currently rejecting: resume accepting when below (threshold - hysteresis)
    LocalCount < ClusterAvg * (Threshold - Hysteresis).

vmq_cluster_nodes() ->
    try
        case vmq_cluster:status() of
            Status when is_list(Status) ->
                [Node || {Node, true} <- Status];
            _ ->
                [node()]
        end
    catch
        _:_ -> [node()]
    end.

schedule_check(Interval) ->
    erlang:send_after(Interval, self(), check_balance).

cancel_timer(undefined) -> ok;
cancel_timer(TRef) -> erlang:cancel_timer(TRef).

bool_to_int(true) -> 1;
bool_to_int(false) -> 0.

parse_float_env(Key, Default) ->
    case vmq_config:get_env(Key, undefined) of
        undefined ->
            Default;
        Val when is_float(Val) ->
            Val;
        Val when is_integer(Val) ->
            float(Val);
        Val when is_list(Val) ->
            try
                list_to_float(Val)
            catch
                _:_ ->
                    try
                        float(list_to_integer(Val))
                    catch
                        _:_ -> Default
                    end
            end;
        Val when is_binary(Val) ->
            parse_float_env_bin(Val, Default);
        _ ->
            Default
    end.

parse_float_env_bin(Bin, Default) ->
    try
        binary_to_float(Bin)
    catch
        _:_ ->
            try
                float(binary_to_integer(Bin))
            catch
                _:_ -> Default
            end
    end.
