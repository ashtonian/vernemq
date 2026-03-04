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

-module(vmq_balance_rebalancer).
-include("vmq_server.hrl").
-include_lib("kernel/include/logger.hrl").

-behaviour(gen_server).

%% API
-export([
    start_link/0,
    trigger_rebalance/1,
    rebalance_stats/0
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
-define(MAX_ROUNDS, 10).
-define(DEFAULT_REBALANCE_THRESHOLD, 1.3).
-define(DEFAULT_REBALANCE_BATCH_SIZE, 50).
-define(DEFAULT_REBALANCE_COOLDOWN, 30).
-define(DEFAULT_REBALANCE_STABLE_INTERVAL, 60).

-record(state, {
    cluster_stable_since :: non_neg_integer() | undefined,
    total_disconnections = 0 :: non_neg_integer(),
    total_rounds = 0 :: non_neg_integer(),
    rebalance_in_progress = false :: boolean(),
    rebalance_caller :: gen_server:from() | undefined,
    auto_rebalance_tref :: reference() | undefined,
    auto_interval_tref :: reference() | undefined
}).

%%%===================================================================
%%% API
%%%===================================================================

-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

-spec trigger_rebalance(Force :: boolean()) ->
    {ok, #{disconnected => non_neg_integer(), rounds => non_neg_integer()}}
    | {error, disabled | already_running | cluster_unstable | term()}.
trigger_rebalance(Force) ->
    gen_server:call(?SERVER, {trigger_rebalance, Force}, infinity).

-spec rebalance_stats() -> {non_neg_integer(), non_neg_integer()}.
rebalance_stats() ->
    gen_server:call(?SERVER, rebalance_stats, 2000).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
    process_flag(trap_exit, true),
    net_kernel:monitor_nodes(true),
    Now = erlang:monotonic_time(second),
    ?LOG_INFO("starting"),
    TRef = schedule_auto_interval(),
    {ok, #state{cluster_stable_since = Now, auto_interval_tref = TRef}}.

handle_call({trigger_rebalance, Force}, From, State) ->
    case check_rebalance_preconditions(Force, State) of
        ok ->
            %% Spawn a linked worker so the gen_server stays responsive
            %% for rebalance_stats queries during the rebalance.
            Self = self(),
            spawn_link(fun() ->
                Result = run_rebalance(),
                gen_server:cast(Self, {rebalance_done, Result})
            end),
            {noreply, State#state{rebalance_in_progress = true, rebalance_caller = From}};
        {error, Reason} ->
            {reply, {error, Reason}, State}
    end;
handle_call(
    rebalance_stats,
    _From,
    #state{
        total_disconnections = Disconnections,
        total_rounds = Rounds
    } = State
) ->
    {reply, {Disconnections, Rounds}, State};
handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast({rebalance_done, Result}, State) ->
    NewState0 = State#state{rebalance_in_progress = false, rebalance_caller = undefined},
    NewState =
        case Result of
            {ok, {TotalDisconnected, TotalRounds}} ->
                Reply = #{disconnected => TotalDisconnected, rounds => TotalRounds},
                maybe_reply(State#state.rebalance_caller, {ok, Reply}),
                NewState0#state{
                    total_disconnections = State#state.total_disconnections + TotalDisconnected,
                    total_rounds = State#state.total_rounds + TotalRounds
                };
            {error, _} = Err ->
                maybe_reply(State#state.rebalance_caller, Err),
                NewState0
        end,
    {noreply, NewState};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({nodeup, Node}, State) ->
    ?LOG_INFO("node joined: ~p, resetting cluster stability timer", [Node]),
    Now = erlang:monotonic_time(second),
    NewState = cancel_auto_rebalance_timer(State),
    StableInterval = vmq_config:get_env(
        rebalance_stable_interval, ?DEFAULT_REBALANCE_STABLE_INTERVAL
    ),
    TRef = erlang:send_after((StableInterval + 5) * 1000, self(), auto_rebalance_check),
    {noreply, NewState#state{cluster_stable_since = Now, auto_rebalance_tref = TRef}};
handle_info({nodedown, Node}, State) ->
    ?LOG_INFO("node left: ~p, resetting cluster stability timer", [Node]),
    Now = erlang:monotonic_time(second),
    NewState = cancel_auto_rebalance_timer(State),
    {noreply, NewState#state{cluster_stable_since = Now}};
handle_info(auto_rebalance_check, State0) ->
    State = State0#state{auto_rebalance_tref = undefined},
    case vmq_config:get_env(rebalance_on_node_join, false) of
        true ->
            case is_cluster_stable(State) andalso not State#state.rebalance_in_progress of
                true ->
                    ?LOG_INFO("auto-rebalance triggered after node join"),
                    case check_rebalance_preconditions(false, State) of
                        ok ->
                            Self = self(),
                            spawn_link(fun() ->
                                Result = run_rebalance(),
                                gen_server:cast(Self, {rebalance_done, Result})
                            end),
                            {noreply, State#state{rebalance_in_progress = true}};
                        {error, Reason} ->
                            ?LOG_INFO("auto-rebalance skipped: ~p", [Reason]),
                            {noreply, State}
                    end;
                false ->
                    ?LOG_DEBUG("auto-rebalance skipped: not stable or already running"),
                    {noreply, State}
            end;
        false ->
            {noreply, State}
    end;
handle_info(auto_interval_rebalance, State0) ->
    State = State0#state{auto_interval_tref = undefined},
    NewState =
        case not State#state.rebalance_in_progress of
            true ->
                case check_rebalance_preconditions(false, State) of
                    ok ->
                        ?LOG_INFO("periodic auto-rebalance triggered"),
                        Self = self(),
                        spawn_link(fun() ->
                            Result = run_rebalance(),
                            gen_server:cast(Self, {rebalance_done, Result})
                        end),
                        State#state{rebalance_in_progress = true};
                    {error, Reason} ->
                        ?LOG_DEBUG("periodic auto-rebalance skipped: ~p", [Reason]),
                        State
                end;
            false ->
                ?LOG_DEBUG("periodic auto-rebalance skipped: already running"),
                State
        end,
    TRef = schedule_auto_interval(),
    {noreply, NewState#state{auto_interval_tref = TRef}};
handle_info({'EXIT', _Pid, normal}, State) ->
    %% Worker finished normally (result delivered via cast)
    {noreply, State};
handle_info({'EXIT', Pid, Reason}, State) ->
    ?LOG_ERROR("rebalance worker ~p crashed: ~p", [Pid, Reason]),
    maybe_reply(State#state.rebalance_caller, {error, {worker_crashed, Reason}}),
    {noreply, State#state{
        rebalance_in_progress = false,
        rebalance_caller = undefined
    }};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{auto_rebalance_tref = RTRef, auto_interval_tref = ITRef}) ->
    cancel_timer(RTRef),
    cancel_timer(ITRef),
    net_kernel:monitor_nodes(false),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

check_rebalance_preconditions(Force, State) ->
    BalanceEnabled = vmq_config:get_env(balance_enabled, false),
    RebalanceEnabled = vmq_config:get_env(rebalance_enabled, false),
    case BalanceEnabled andalso RebalanceEnabled of
        false ->
            {error, disabled};
        true ->
            case State#state.rebalance_in_progress of
                true ->
                    {error, already_running};
                false ->
                    case Force orelse is_cluster_stable(State) of
                        false ->
                            {error, cluster_unstable};
                        true ->
                            ok
                    end
            end
    end.

is_cluster_stable(#state{cluster_stable_since = undefined}) ->
    false;
is_cluster_stable(#state{cluster_stable_since = Since}) ->
    StableInterval = vmq_config:get_env(
        rebalance_stable_interval, ?DEFAULT_REBALANCE_STABLE_INTERVAL
    ),
    Now = erlang:monotonic_time(second),
    (Now - Since) >= StableInterval.

%% Runs in a spawned worker process, not inside the gen_server.
run_rebalance() ->
    RebalanceFun = fun() ->
        execute_rebalance_rounds(0, 0, sets:new([{version, 2}]))
    end,
    Cooldown = vmq_config:get_env(rebalance_cooldown, ?DEFAULT_REBALANCE_COOLDOWN),
    %% Timeout must accommodate worst-case: MAX_ROUNDS * cooldown + work time
    SyncTimeout = (?MAX_ROUNDS * Cooldown + 60) * 1000,
    try
        vmq_reg_sync:sync(<<"cluster_rebalance">>, RebalanceFun, SyncTimeout)
    catch
        Class:Reason:Stack ->
            ?LOG_ERROR(
                "rebalance failed: ~p:~p~n~p",
                [Class, Reason, Stack]
            ),
            {error, {rebalance_exception, Reason}}
    end.

execute_rebalance_rounds(Disconnected, Rounds, _Seen) when Rounds >= ?MAX_ROUNDS ->
    {ok, {Disconnected, Rounds}};
execute_rebalance_rounds(Disconnected, Rounds, Seen) ->
    NodeCounts = vmq_balance_srv:get_node_counts(),
    TotalConns = maps:fold(fun(_N, C, Acc) -> Acc + C end, 0, NodeCounts),
    NumNodes = max(1, map_size(NodeCounts)),
    case NumNodes of
        N when N =< 1 ->
            {ok, {Disconnected, Rounds}};
        _ ->
            Target = TotalConns / NumNodes,
            LocalCount = maps:get(node(), NodeCounts, 0),
            Threshold = parse_float_config(rebalance_threshold, ?DEFAULT_REBALANCE_THRESHOLD),
            case LocalCount > Target * Threshold of
                false ->
                    {ok, {Disconnected, Rounds}};
                true ->
                    Excess = LocalCount - trunc(Target),
                    BatchSize = vmq_config:get_env(
                        rebalance_batch_size, ?DEFAULT_REBALANCE_BATCH_SIZE
                    ),
                    ToDisconnect = min(Excess, BatchSize),
                    {DisconnectedThisRound, NewSeen} =
                        disconnect_sessions(ToDisconnect, Seen),
                    ?LOG_INFO(
                        "rebalance round ~p: disconnected ~p sessions "
                        "(local=~p, target=~.1f, threshold=~.2f)",
                        [Rounds + 1, DisconnectedThisRound, LocalCount, Target, Threshold]
                    ),
                    case DisconnectedThisRound of
                        0 ->
                            {ok, {Disconnected, Rounds + 1}};
                        _ ->
                            Cooldown = vmq_config:get_env(
                                rebalance_cooldown, ?DEFAULT_REBALANCE_COOLDOWN
                            ),
                            timer:sleep(Cooldown * 1000),
                            execute_rebalance_rounds(
                                Disconnected + DisconnectedThisRound,
                                Rounds + 1,
                                NewSeen
                            )
                    end
            end
    end.

disconnect_sessions(Count, Seen) ->
    Sessions = collect_session_info(),
    Sorted = sort_sessions(Sessions),
    %% Filter out sessions we already disconnected in a prior round
    Eligible = lists:filter(
        fun({_SortKey, QPid}) -> not sets:is_element(QPid, Seen) end,
        Sorted
    ),
    ToDisconnect = lists:sublist(Eligible, Count),
    lists:foldl(
        fun({_SortKey, QPid}, {Acc, SeenAcc}) ->
            try
                vmq_queue:force_disconnect(QPid, ?ADMINISTRATIVE_ACTION, false),
                {Acc + 1, sets:add_element(QPid, SeenAcc)}
            catch
                _:_ -> {Acc, SeenAcc}
            end
        end,
        {0, Seen},
        ToDisconnect
    ).

%% Collect online queue info for sorting. Uses vmq_queue:info/1 to get
%% per-session cleanup_on_disconnect and started_at for prioritization.
collect_session_info() ->
    vmq_queue_sup_sup:fold_queues(
        fun(_SubscriberId, QPid, Acc) ->
            try vmq_queue:status(QPid) of
                {online, _, _, _, true} ->
                    %% Plugin queue, skip
                    Acc;
                {online, _, _, _, _} ->
                    case get_queue_sort_info(QPid) of
                        {ok, SortInfo} -> [SortInfo | Acc];
                        error -> Acc
                    end;
                _ ->
                    %% Offline/draining/wait_for_offline, skip
                    Acc
            catch
                _:_ -> Acc
            end
        end,
        []
    ).

%% Extract sort-relevant info from a queue via vmq_queue:info/1.
%% Returns {ok, {IsClean, NewestStartedAt, QPid}} or error.
get_queue_sort_info(QPid) ->
    try vmq_queue:info(QPid) of
        #{sessions := Sessions} when is_list(Sessions), Sessions =/= [] ->
            {IsClean, NewestTs} = classify_sessions(Sessions),
            {ok, {IsClean, NewestTs, QPid}};
        _ ->
            error
    catch
        _:_ -> error
    end.

%% Classify a queue's sessions:
%% - IsClean: true if ALL sessions have cleanup_on_disconnect=true
%% - NewestTs: the most recent started_at timestamp among sessions
classify_sessions(Sessions) ->
    lists:foldl(
        fun({_Pid, CleanupOnDisconnect, StartedAt}, {AccClean, AccNewest}) ->
            NewClean = AccClean andalso (CleanupOnDisconnect =:= true),
            NewNewest =
                case AccNewest of
                    undefined -> StartedAt;
                    _ when is_integer(StartedAt), StartedAt > AccNewest -> StartedAt;
                    _ -> AccNewest
                end,
            {NewClean, NewNewest}
        end,
        {true, undefined},
        Sessions
    ).

%% Sort queues for disconnection priority:
%% 1. Clean sessions first (cleanup_on_disconnect=true for all sessions)
%% 2. Within same clean/persistent category, newest connections first
sort_sessions(Sessions) ->
    Keyed = [
        {sort_key(IsClean, NewestTs), QPid}
     || {IsClean, NewestTs, QPid} <- Sessions
    ],
    lists:sort(fun({K1, _}, {K2, _}) -> K1 =< K2 end, Keyed).

%% Lower sort key = disconnected first.
%% Clean sessions get key 0, persistent get key 1.
%% Within each group, newer sessions (higher timestamp) sort first
%% via negated timestamp.
sort_key(true, Ts) -> {0, negate_ts(Ts)};
sort_key(false, Ts) -> {1, negate_ts(Ts)}.

negate_ts(undefined) -> 0;
negate_ts(Ts) when is_integer(Ts) -> -Ts;
negate_ts(_) -> 0.

maybe_reply(undefined, _Reply) ->
    ok;
maybe_reply(From, Reply) ->
    gen_server:reply(From, Reply).

cancel_auto_rebalance_timer(#state{auto_rebalance_tref = undefined} = State) ->
    State;
cancel_auto_rebalance_timer(#state{auto_rebalance_tref = TRef} = State) ->
    erlang:cancel_timer(TRef),
    %% Flush any already-delivered message to prevent stale processing
    receive
        auto_rebalance_check -> ok
    after 0 -> ok
    end,
    State#state{auto_rebalance_tref = undefined}.

cancel_timer(undefined) -> ok;
cancel_timer(TRef) -> erlang:cancel_timer(TRef).

%% Always schedule a tick. If auto_interval is 0 (disabled), schedule
%% a slow re-check so runtime config changes are picked up without
%% requiring a process restart.
-define(AUTO_INTERVAL_RECHECK, 60000).

schedule_auto_interval() ->
    case vmq_config:get_env(rebalance_auto_interval, 0) of
        Seconds when is_integer(Seconds), Seconds > 0 ->
            erlang:send_after(Seconds * 1000, self(), auto_interval_rebalance);
        _ ->
            erlang:send_after(?AUTO_INTERVAL_RECHECK, self(), auto_interval_rebalance)
    end.

parse_float_config(Key, Default) ->
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
            try
                binary_to_float(Val)
            catch
                _:_ ->
                    try
                        float(binary_to_integer(Val))
                    catch
                        _:_ -> Default
                    end
            end;
        _ ->
            Default
    end.
