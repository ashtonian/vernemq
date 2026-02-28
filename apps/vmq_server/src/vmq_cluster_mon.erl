%% Copyright 2018 Erlio GmbH Basel Switzerland (http://erl.io)
%% Copyright 2018-2024 Octavo Labs/VerneMQ (https://vernemq.com/)
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
%%
-module(vmq_cluster_mon).

-behaviour(gen_server).
-include_lib("kernel/include/logger.hrl").

%% API functions
-export([
    start_link/0,
    dead_node_status/0
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

-record(state, {
    down_nodes = #{} :: #{node() => integer()},
    cleanup_pid = undefined :: pid() | undefined,
    cleanup_node = undefined :: node() | undefined
}).

-define(RECHECK_INTERVAL, 10000).
-define(RECHECK_INTERVAL_NOT_READY, 2000).
-define(CONSISTENCY_CHECK_TIMEOUT, 60).

%%%===================================================================
%%% API functions
%%%===================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec dead_node_status() -> [{node(), non_neg_integer()}].
dead_node_status() ->
    gen_server:call(?MODULE, dead_node_status).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
    case net_kernel:monitor_nodes(true) of
        ok ->
            _ = ets:new(vmq_status, [{read_concurrency, true}, public, named_table]),
            erlang:send_after(?RECHECK_INTERVAL, self(), recheck),
            %% trap_exit is needed so terminate/2 can remove the event handler
            process_flag(trap_exit, true),
            {ok, #state{}, 0};
        {error, Reason} ->
            {stop, Reason}
    end.

handle_call(dead_node_status, _From, #state{down_nodes = DownNodes} = State) ->
    Now = erlang:monotonic_time(second),
    Status = [{Node, Now - Ts} || {Node, Ts} <- maps:to_list(DownNodes)],
    {reply, Status, State};
handle_call(_Request, _From, State) ->
    {reply, {error, unknown_call}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(timeout, State) ->
    vmq_peer_service:add_event_handler(vmq_cluster, []),
    {noreply, State};
handle_info({nodedown, Node}, #state{down_nodes = DownNodes} = State) ->
    ?LOG_WARNING("cluster node ~p DOWN", [Node]),
    vmq_cluster:recheck(),
    NewDownNodes =
        case maps:is_key(Node, DownNodes) of
            true -> DownNodes;
            false -> maps:put(Node, erlang:monotonic_time(second), DownNodes)
        end,
    {noreply, State#state{down_nodes = NewDownNodes}};
handle_info({nodeup, Node}, #state{down_nodes = DownNodes} = State) ->
    ?LOG_INFO("cluster node ~p UP", [Node]),
    vmq_cluster:recheck(),
    {noreply, State#state{down_nodes = maps:remove(Node, DownNodes)}};
handle_info({gen_event_EXIT, vmq_cluster, _}, State) ->
    vmq_peer_service:add_event_handler(vmq_cluster, []),
    {noreply, State};
handle_info(recheck, State) ->
    vmq_cluster:recheck(),
    erlang:send_after(
        case vmq_cluster:is_ready() of
            true -> ?RECHECK_INTERVAL;
            false -> ?RECHECK_INTERVAL_NOT_READY
        end,
        self(),
        recheck
    ),
    State1 = sync_down_nodes(State),
    State2 = maybe_trigger_cleanup(State1),
    {noreply, State2};
handle_info(
    {'DOWN', _MRef, process, Pid, Reason},
    #state{cleanup_pid = Pid, cleanup_node = CleanupNode} = State
) ->
    case Reason of
        normal ->
            ?LOG_INFO("dead node cleanup completed for ~p", [CleanupNode]),
            {noreply, State#state{
                cleanup_pid = undefined,
                cleanup_node = undefined,
                down_nodes = maps:remove(CleanupNode, State#state.down_nodes)
            }};
        _ ->
            ?LOG_WARNING("dead node cleanup failed for ~p: ~p", [CleanupNode, Reason]),
            %% Keep node in down_nodes so it will be retried on next cycle
            {noreply, State#state{
                cleanup_pid = undefined,
                cleanup_node = undefined
            }}
    end;
handle_info({'DOWN', _MRef, process, _Pid, _Reason}, State) ->
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(Reason, _State) ->
    vmq_peer_service:delete_event_handler(vmq_cluster, Reason),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

sync_down_nodes(#state{down_nodes = DownNodes} = State) ->
    ClusterStatus = vmq_cluster:status(),
    Members = [Node || {Node, _} <- ClusterStatus],
    DownFromStatus = [Node || {Node, false} <- ClusterStatus],
    Now = erlang:monotonic_time(second),
    %% Add nodes that are down in cluster status but not yet tracked
    DownNodes1 = lists:foldl(
        fun(Node, Acc) ->
            case maps:is_key(Node, Acc) of
                true -> Acc;
                false -> maps:put(Node, Now, Acc)
            end
        end,
        DownNodes,
        DownFromStatus
    ),
    %% Remove nodes that are no longer members or have recovered
    DownNodes2 = maps:filter(
        fun(Node, _Ts) ->
            lists:member(Node, Members) andalso
                lists:member(Node, DownFromStatus)
        end,
        DownNodes1
    ),
    State#state{down_nodes = DownNodes2}.

maybe_trigger_cleanup(#state{cleanup_pid = Pid} = State) when is_pid(Pid) ->
    %% Already running a cleanup
    State;
maybe_trigger_cleanup(#state{down_nodes = DownNodes} = State) ->
    Timeout = vmq_config:get_env(dead_node_cleanup_timeout, 0),
    case Timeout > 0 of
        false ->
            State;
        true ->
            Now = erlang:monotonic_time(second),
            Expired = [
                {Node, Ts}
             || {Node, Ts} <- maps:to_list(DownNodes),
                (Now - Ts) >= Timeout
            ],
            case Expired of
                [] ->
                    State;
                [{Node, _} | _] ->
                    maybe_start_cleanup(Node, State)
            end
    end.

maybe_start_cleanup(Node, State) ->
    case has_quorum() of
        true ->
            ?LOG_INFO("starting automatic dead node cleanup for ~p", [Node]),
            {Pid, _MRef} = spawn_monitor(fun() -> do_cleanup(Node) end),
            State#state{cleanup_pid = Pid, cleanup_node = Node};
        false ->
            ?LOG_WARNING(
                "skipping dead node cleanup for ~p: quorum not met",
                [Node]
            ),
            State
    end.

%% Uses the already-computed cluster status from the ETS table
%% (updated by vmq_cluster:recheck() which runs before this is called).
%% This avoids blocking the gen_server on net_adm:ping calls.
has_quorum() ->
    Status = vmq_cluster:status(),
    TotalMembers = length(Status),
    case TotalMembers =< 1 of
        true ->
            false;
        false ->
            Reachable = length([N || {N, true} <- Status]),
            Reachable > TotalMembers / 2
    end.

do_cleanup(Node) ->
    try
        do_cleanup_(Node)
    catch
        Class:Reason:Stacktrace ->
            ?LOG_ERROR(
                "dead node cleanup crashed for ~p: ~p:~p~n~p",
                [Node, Class, Reason, Stacktrace]
            ),
            exit({cleanup_error, Reason})
    end.

do_cleanup_(Node) ->
    %% Final check: if the node is actually reachable, abort
    case net_adm:ping(Node) of
        pong ->
            ?LOG_INFO("aborting dead node cleanup for ~p: node is reachable", [Node]),
            exit(normal);
        pang ->
            ok
    end,
    TargetNodes = vmq_peer_service:members() -- [Node],
    case vmq_peer_service:leave(Node) of
        ok ->
            ?LOG_INFO("dead node ~p removed from cluster membership", [Node]);
        {error, not_present} ->
            ?LOG_INFO("dead node ~p was already removed from cluster", [Node]);
        {error, Reason} ->
            ?LOG_ERROR("failed to remove dead node ~p from cluster: ~p", [Node, Reason]),
            exit({leave_failed, Reason})
    end,
    case wait_for_consistency(TargetNodes, ?CONSISTENCY_CHECK_TIMEOUT) of
        true ->
            ?LOG_INFO("cluster consistent, fixing dead queues for ~p", [Node]),
            vmq_reg:fix_dead_queues([Node], TargetNodes),
            ?LOG_INFO("dead queue fix completed for ~p", [Node]);
        false ->
            ?LOG_WARNING("cluster inconsistent after removing ~p, queue fix skipped", [Node]),
            exit({error, cluster_inconsistent})
    end.

wait_for_consistency([], _Retries) ->
    true;
wait_for_consistency(_Nodes, 0) ->
    false;
wait_for_consistency([Node | Rest] = All, Retries) ->
    case rpc:call(Node, vmq_cluster, is_ready, []) of
        true ->
            wait_for_consistency(Rest, Retries);
        _ ->
            timer:sleep(1000),
            wait_for_consistency(All, Retries - 1)
    end.
