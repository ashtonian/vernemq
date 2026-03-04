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
%% TODO: merge upstream — this module adds connection pool management
%% and nodeup backoff reset for faster recovery.
-module(vmq_cluster_node_sup).

-behaviour(supervisor).

%% API
-export([
    start_link/0,
    ensure_cluster_node/1,
    get_cluster_node/1,
    get_cluster_node/2,
    del_cluster_node/1,
    node_status/1,
    pool_size/0,
    reset_node_backoff/1
]).

%% Supervisor callbacks
-export([init/1]).

%% Helper macro for declaring children of supervisor
-define(CHILD(I, Type, Args), {I, {I, start_link, Args}, permanent, 5000, Type, [I]}).
%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the supervisor
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

pool_size() ->
    case ets:lookup(vmq_cluster_node_pool, pool_size) of
        [{pool_size, N}] -> N;
        [] -> 1
    end.

ensure_cluster_node(Node) when Node == node() ->
    %% cluster node not needed
    ok;
ensure_cluster_node(Node) ->
    N = pool_size(),
    lists:foreach(
        fun(Idx) ->
            ChildId = {vmq_cluster_node, Node, Idx},
            case lists:keyfind(ChildId, 1, supervisor:which_children(?MODULE)) of
                false ->
                    {ok, _} = supervisor:start_child(?MODULE, child_spec(Node, Idx));
                {_, undefined, _, _} ->
                    _ = supervisor:delete_child(?MODULE, ChildId),
                    {ok, _} = supervisor:start_child(?MODULE, child_spec(Node, Idx));
                {_, restarting, _, _} ->
                    ok;
                {_, Pid, _, _} when is_pid(Pid) ->
                    ok
            end
        end,
        lists:seq(0, N - 1)
    ),
    ok.

del_cluster_node(Node) ->
    N = pool_size(),
    lists:foreach(
        fun(Idx) ->
            ChildId = {vmq_cluster_node, Node, Idx},
            case supervisor:terminate_child(?MODULE, ChildId) of
                ok ->
                    ets:delete(vmq_cluster_node_pool, {Node, Idx}),
                    supervisor:delete_child(?MODULE, ChildId);
                {error, not_found} ->
                    ok
            end
        end,
        lists:seq(0, N - 1)
    ),
    ok.

%% Reset backoff on all pool connections to Node, triggering immediate reconnect.
reset_node_backoff(Node) ->
    N = pool_size(),
    lists:foreach(
        fun(Idx) ->
            case get_cluster_node(Node, Idx) of
                {ok, Pid} -> vmq_cluster_node:reset_backoff(Pid);
                {error, not_found} -> ok
            end
        end,
        lists:seq(0, N - 1)
    ),
    ok.

get_cluster_node(Node, ShardIdx) ->
    case ets:lookup(vmq_cluster_node_pool, {Node, ShardIdx}) of
        [{{Node, ShardIdx}, Pid}] ->
            {ok, Pid};
        [] ->
            {error, not_found}
    end.

get_cluster_node(Node) ->
    N = pool_size(),
    get_any_cluster_node(Node, 0, N).

get_any_cluster_node(_Node, Idx, N) when Idx >= N ->
    {error, not_found};
get_any_cluster_node(Node, Idx, N) ->
    case get_cluster_node(Node, Idx) of
        {ok, Pid} -> {ok, Pid};
        {error, not_found} -> get_any_cluster_node(Node, Idx + 1, N)
    end.

-spec node_status(node()) -> init | up | down.
node_status(Node) when Node == node() ->
    up;
node_status(Node) ->
    N = pool_size(),
    pool_status(Node, 0, N, init).

pool_status(_Node, Idx, N, BestStatus) when Idx >= N ->
    BestStatus;
pool_status(Node, Idx, N, BestStatus) ->
    case get_cluster_node(Node, Idx) of
        {ok, Pid} ->
            case vmq_cluster_node:status(Pid) of
                up ->
                    up;
                down ->
                    pool_status(Node, Idx + 1, N, down);
                init ->
                    NewBest =
                        case BestStatus of
                            down -> down;
                            _ -> init
                        end,
                    pool_status(Node, Idx + 1, N, NewBest)
            end;
        {error, not_found} ->
            pool_status(Node, Idx + 1, N, down)
    end.

%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a supervisor is started using supervisor:start_link/[2,3],
%% this function is called by the new process to find out about
%% restart strategy, maximum restart frequency and child
%% specifications.
%%
%% @spec init(Args) -> {ok, {SupFlags, [ChildSpec]}} |
%%                     ignore |
%%                     {error, Reason}
%% @end
%%--------------------------------------------------------------------
init([]) ->
    _ = ets:new(vmq_cluster_node_pool,
                [public, set, named_table, {read_concurrency, true}]),
    PoolSize = max(1, application:get_env(vmq_server,
                      outgoing_clustering_connection_count, 4)),
    ets:insert(vmq_cluster_node_pool, {pool_size, PoolSize}),
    {ok,
        {{one_for_one, 5, 10}, [
            ?CHILD(vmq_cluster_mon, worker, [])
        ]}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
child_spec(Node, Idx) ->
    {{vmq_cluster_node, Node, Idx},
     {vmq_cluster_node, start_link, [Node, Idx]},
     permanent, 5000, worker, [vmq_cluster_node]}.
