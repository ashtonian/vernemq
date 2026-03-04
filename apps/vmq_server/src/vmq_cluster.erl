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

-module(vmq_cluster).

-include_lib("vmq_commons/include/vmq_types.hrl").
-include_lib("kernel/include/logger.hrl").

-behaviour(gen_event).

%% gen_server callbacks
-export([
    init/1,
    handle_event/2,
    handle_call/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-export([
    nodes/0,
    recheck/0,
    status/0,
    is_ready/0,
    if_ready/2,
    if_ready/3,
    cluster_tier/0,
    netsplit_statistics/0,
    degraded_statistics/0,
    publish/2,
    publish_async/2,
    remote_enqueue/3,
    remote_enqueue/4,
    remote_enqueue_async/3
]).

-type cluster_tier() :: healthy | degraded | partitioned.
-export_type([cluster_tier/0]).

-ifdef(TEST).
-export([
    compute_tier/2,
    compute_transition/2,
    migrate_old_format/1
]).
-endif.

-define(SERVER, ?MODULE).
%% table is owned by vmq_cluster_mon
-define(VMQ_CLUSTER_STATUS, vmq_status).

-record(state, {}).
-type state() :: #state{}.

%%%===================================================================
%%% API
%%%===================================================================

recheck() ->
    case vmq_peer_service:call_event_handler(?MODULE, recheck, infinity) of
        ok ->
            ok;
        E ->
            ?LOG_WARNING("error during cluster checkup due to ~p", [E]),
            E
    end.

-spec nodes() -> [any()].
nodes() ->
    [
        Node
     || [{Node, true}] <-
            ets:match(?VMQ_CLUSTER_STATUS, '$1'),
        Node /= ready
    ].

status() ->
    [
        {Node, Ready}
     || [{Node, Ready}] <-
            ets:match(?VMQ_CLUSTER_STATUS, '$1'),
        Node /= ready
    ].

-spec is_ready() -> boolean().
is_ready() ->
    case cluster_tier() of
        partitioned -> false;
        _ -> true
    end.

-spec cluster_tier() -> cluster_tier().
cluster_tier() ->
    case catch ets:lookup(?VMQ_CLUSTER_STATUS, ready) of
        [{ready, {Tier, _, _, _, _}}] when
            Tier =:= healthy; Tier =:= degraded; Tier =:= partitioned
        ->
            Tier;
        [{ready, {true, _, _}}] ->
            healthy;
        [{ready, {false, _, _}}] ->
            partitioned;
        _ ->
            partitioned
    end.

-spec netsplit_statistics() -> {non_neg_integer(), non_neg_integer()} | {error, atom()}.
netsplit_statistics() ->
    case catch ets:lookup(?VMQ_CLUSTER_STATUS, ready) of
        [{ready, {Tier, NetsplitDetectedCount, NetsplitResolvedCount, _, _}}] when
            Tier =:= healthy; Tier =:= degraded; Tier =:= partitioned
        ->
            {NetsplitDetectedCount, NetsplitResolvedCount};
        [{ready, {_Ready, NetsplitDetectedCount, NetsplitResolvedCount}}] ->
            {NetsplitDetectedCount, NetsplitResolvedCount};
        {'EXIT', {badarg, _}} ->
            {error, vmq_status_table_down};
        _ ->
            {0, 0}
    end.

-spec degraded_statistics() -> {non_neg_integer(), non_neg_integer()} | {error, atom()}.
degraded_statistics() ->
    case catch ets:lookup(?VMQ_CLUSTER_STATUS, ready) of
        [{ready, {Tier, _, _, DegradedDetectedCount, DegradedResolvedCount}}] when
            Tier =:= healthy; Tier =:= degraded; Tier =:= partitioned
        ->
            {DegradedDetectedCount, DegradedResolvedCount};
        [{ready, {_Ready, _, _}}] ->
            {0, 0};
        {'EXIT', {badarg, _}} ->
            {error, vmq_status_table_down};
        _ ->
            {0, 0}
    end.

-spec if_ready(_, _) -> any().
if_ready(Fun, Args) ->
    case is_ready() of
        true ->
            apply(Fun, Args);
        false ->
            {error, not_ready}
    end.
-spec if_ready(_, _, _) -> any().
if_ready(Mod, Fun, Args) ->
    case is_ready() of
        true ->
            apply(Mod, Fun, Args);
        false ->
            {error, not_ready}
    end.

publish(Node, Msg) ->
    case vmq_cluster_node_sup:get_cluster_node(Node) of
        {error, not_found} ->
            {error, not_found};
        {ok, Pid} ->
            vmq_cluster_node:publish(Pid, Msg)
    end.

publish_async(Node, Msg) ->
    case vmq_cluster_node_sup:get_cluster_node(Node) of
        {error, not_found} ->
            {error, not_found};
        {ok, Pid} ->
            vmq_cluster_node:publish_async(Pid, Msg)
    end.

-spec remote_enqueue(node(), Term, BufferIfUnreachable) ->
    ok | {error, term()}
when
    Term ::
        {enqueue_many, subscriber_id(), Msgs :: term(), Opts :: map()}
        | {enqueue, Queue :: term(), Msgs :: term()},
    BufferIfUnreachable :: boolean().
remote_enqueue(Node, Term, BufferIfUnreachable) ->
    Timeout = vmq_config:get_env(remote_enqueue_timeout),
    remote_enqueue(Node, Term, BufferIfUnreachable, Timeout).

-spec remote_enqueue(node(), Term, BufferIfUnreachable, Timeout) ->
    ok | {error, term()}
when
    Term ::
        {enqueue_many, subscriber_id(), Msgs :: term(), Opts :: map()}
        | {enqueue, Queue :: term(), Msgs :: term()},
    BufferIfUnreachable :: boolean(),
    Timeout :: non_neg_integer() | infinity.
remote_enqueue(Node, Term, BufferIfUnreachable, Timeout) ->
    case vmq_cluster_node_sup:get_cluster_node(Node) of
        {error, not_found} ->
            {error, not_found};
        {ok, Pid} ->
            vmq_cluster_node:enqueue(Pid, Term, BufferIfUnreachable, Timeout)
    end.

remote_enqueue_async(Node, Term, BufferIfUnreachable) ->
    case vmq_cluster_node_sup:get_cluster_node(Node) of
        {error, not_found} ->
            {error, not_found};
        {ok, Pid} ->
            vmq_cluster_node:enqueue_async(Pid, Term, BufferIfUnreachable)
    end.

%%%===================================================================
%%% gen_event callbacks
%%%===================================================================
-spec init([]) -> {'ok', state()}.
init([]) ->
    check_ready(),
    ?LOG_INFO("cluster event handler '~p' registered", [?MODULE]),
    {ok, #state{}}.

-spec handle_call(_, _) -> {'ok', 'ok', _}.
handle_call(recheck, State) ->
    _ = check_ready(),
    {ok, ok, State}.

-spec handle_event(_, _) -> {'ok', _}.
handle_event({update, _}, State) ->
    %% Cluster event
    _ = check_ready(),
    {ok, State}.

handle_info(Info, State) ->
    ?LOG_WARNING("got unhandled info ~p", [Info]),
    {ok, State}.

-spec terminate(_, _) -> 'ok'.
terminate(_Reason, _State) ->
    ok.

-spec code_change(_, _, _) -> {'ok', _}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
check_ready() ->
    Nodes = vmq_peer_service:members(),
    check_ready(Nodes).

check_ready(Nodes) ->
    check_ready(Nodes, []),
    ets:foldl(
        fun
            ({ready, _}, _) ->
                ignore;
            ({Node, _IsReady}, _) ->
                case lists:member(Node, Nodes) of
                    true ->
                        ignore;
                    false ->
                        %% Node is not part of the cluster anymore
                        ?LOG_WARNING("remove supervision for node ~p", [Node]),
                        _ = vmq_cluster_node_sup:del_cluster_node(Node),
                        ets:delete(?VMQ_CLUSTER_STATUS, Node)
                end
        end,
        ok,
        ?VMQ_CLUSTER_STATUS
    ),
    ok.

check_ready([Node | Rest], Acc) ->
    IsReady =
        case rpc:call(Node, erlang, whereis, [vmq_server_sup]) of
            Pid when is_pid(Pid) -> true;
            _ -> false
        end,
    ok = vmq_cluster_node_sup:ensure_cluster_node(Node),
    %% We should only say we're ready if we've established a
    %% connection to the remote node.
    Status = vmq_cluster_node_sup:node_status(Node),
    IsReady1 = IsReady andalso lists:member(Status, [up, init]),
    check_ready(Rest, [{Node, IsReady1} | Acc]);
check_ready([], Acc) ->
    OldObj = migrate_old_format(
        case ets:lookup(?VMQ_CLUSTER_STATUS, ready) of
            [] -> {healthy, 0, 0, 0, 0};
            [{ready, Obj}] -> Obj
        end
    ),
    Quorum = vmq_config:get_env(cluster_ready_quorum, 1.0),
    NewTier = compute_tier(Acc, Quorum),
    NewObj = compute_transition(NewTier, OldObj),
    ets:insert(?VMQ_CLUSTER_STATUS, [{ready, NewObj} | Acc]).

%% @doc Migrate old 3-tuple ETS format to new 5-tuple.
-spec migrate_old_format(tuple()) ->
    {cluster_tier(), non_neg_integer(), non_neg_integer(), non_neg_integer(), non_neg_integer()}.
migrate_old_format({Tier, NsDetected, NsResolved, DegDetected, DegResolved}) when
    Tier =:= healthy; Tier =:= degraded; Tier =:= partitioned
->
    {Tier, NsDetected, NsResolved, DegDetected, DegResolved};
migrate_old_format({true, NsDetected, NsResolved}) ->
    {healthy, NsDetected, NsResolved, 0, 0};
migrate_old_format({false, NsDetected, NsResolved}) ->
    {partitioned, NsDetected, NsResolved, 0, 0};
migrate_old_format(_Unexpected) ->
    ?LOG_WARNING("unexpected cluster status ETS format, resetting counters"),
    {healthy, 0, 0, 0, 0}.

%% @doc Compute cluster tier from node status list using quorum threshold.
%% Uses integer arithmetic to avoid float comparison precision issues.
-spec compute_tier([{atom(), boolean()}], float()) -> cluster_tier().
compute_tier([], _Quorum) ->
    healthy;
compute_tier(Acc, Quorum) ->
    Total = length(Acc),
    Alive = length([N || {N, true} <- Acc]),
    case Alive =:= Total of
        true ->
            healthy;
        false ->
            case Alive * 100 >= round(Quorum * 100) * Total of
                true -> degraded;
                false -> partitioned
            end
    end.

%% @doc Track state transitions and update counters.
-spec compute_transition(cluster_tier(), tuple()) ->
    {cluster_tier(), non_neg_integer(), non_neg_integer(), non_neg_integer(), non_neg_integer()}.
compute_transition(NewTier, {OldTier, NsDetected, NsResolved, DegDetected, DegResolved}) ->
    {NsDetected1, NsResolved1} = update_netsplit_counters(OldTier, NewTier, NsDetected, NsResolved),
    {DegDetected1, DegResolved1} = update_degraded_counters(
        OldTier, NewTier, DegDetected, DegResolved
    ),
    log_transition(OldTier, NewTier),
    {NewTier, NsDetected1, NsResolved1, DegDetected1, DegResolved1}.

update_netsplit_counters(OldTier, partitioned, NsDetected, NsResolved) when
    OldTier =/= partitioned
->
    {NsDetected + 1, NsResolved};
update_netsplit_counters(partitioned, NewTier, NsDetected, NsResolved) when
    NewTier =/= partitioned
->
    {NsDetected, NsResolved + 1};
update_netsplit_counters(_, _, NsDetected, NsResolved) ->
    {NsDetected, NsResolved}.

update_degraded_counters(OldTier, degraded, DegDetected, DegResolved) when
    OldTier =/= degraded
->
    {DegDetected + 1, DegResolved};
update_degraded_counters(degraded, NewTier, DegDetected, DegResolved) when
    NewTier =/= degraded
->
    {DegDetected, DegResolved + 1};
update_degraded_counters(_, _, DegDetected, DegResolved) ->
    {DegDetected, DegResolved}.

log_transition(Same, Same) ->
    ok;
log_transition(OldTier, NewTier) ->
    ?LOG_WARNING("cluster tier changed: ~p -> ~p", [OldTier, NewTier]).
