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

-module(vmq_reg_trie).

-include("vmq_server.hrl").
-include("vmq_reg_trie.hrl").
-include_lib("kernel/include/logger.hrl").

-dialyzer(no_undefined_callbacks).

-behaviour(gen_server).
-behaviour(vmq_reg_view).

%% API
-export([
    start_link/0,
    fold/4,
    stats/0,
    init_subscriptions/0
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
    status = init,
    event_handler,
    event_queue = queue:new(),
    num_workers
}).

%%%===================================================================
%%% API
%%%===================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init_subscriptions() ->
    gen_server:call(?MODULE, init_subs, 60000).

-spec fold(subscriber_id(), topic(), fun(), any()) -> any().
fold({MP, _} = SubscriberId, Topic, FoldFun, Acc) when is_list(Topic) ->
    fold_(
        SubscriberId,
        FoldFun,
        Acc,
        %% local subscriptions without wildcard
        [
            {Topic, node()}
            | lists:append(
                %% local & remote subscriptions with wildcard
                match(MP, Topic),
                %% remote subscriptions without wildcards
                get_remote_subscribers(MP, Topic)
            )
        ],
        []
    ).

fold_({MP, _} = SubscriberId, FoldFun, Acc, [{Topic, {Node, Group}} | MatchedTopics], Remotes) ->
    fold_(
        SubscriberId,
        FoldFun,
        fold__(
            FoldFun,
            SubscriberId,
            Acc,
            lookup_subs({MP, Group, Node, Topic})
        ),
        MatchedTopics,
        Remotes
    );
fold_({MP, _} = SubscriberId, FoldFun, Acc, [{Topic, Node} | MatchedTopics], Remotes) when
    Node == node()
->
    fold_(
        SubscriberId,
        FoldFun,
        fold__(
            FoldFun,
            SubscriberId,
            Acc,
            lookup_subs({MP, Topic})
        ),
        MatchedTopics,
        Remotes
    );
fold_(SubscriberId, FoldFun, Acc, [{_Topic, Node} | MatchedTopics], Remotes) ->
    case lists:member(Node, Remotes) of
        true ->
            fold_(SubscriberId, FoldFun, Acc, MatchedTopics, Remotes);
        false ->
            fold_(SubscriberId, FoldFun, FoldFun(Node, SubscriberId, Acc), MatchedTopics, [
                Node | Remotes
            ])
    end;
fold_(_, _, Acc, [], _) ->
    Acc.

lookup_subs(Key) ->
    case ets:lookup(vmq_trie_subs, Key) of
        [{_, fanout}] ->
            MS = [{{{Key, '$1'}}, [], [{{{Key}, '$1'}}]}],
            ets:select(vmq_trie_subs_fanout, MS);
        [] ->
            []
    end.

fold__(FoldFun, SubscriberId, Acc, [{_, SubsIdQoS} | Rest]) ->
    fold__(FoldFun, SubscriberId, FoldFun(SubsIdQoS, SubscriberId, Acc), Rest);
fold__(_, _, Acc, []) ->
    Acc.

stats() ->
    NrOfSubs = info(vmq_trie_subs, size),
    NrOfRemoteSubs = info(vmq_trie_remote_subs, size),
    Mem1 = info(vmq_trie_subs, memory),
    Mem2 = info(vmq_trie_topic, memory),
    Mem3 = info(vmq_trie, memory),
    Mem4 = info(vmq_trie_node, memory),
    Mem5 = info(vmq_trie_remote_subs, memory),
    Mem6 = info(vmq_trie_subs_fanout, memory),
    Memory = Mem1 + Mem2 + Mem3 + Mem4 + Mem5 + Mem6,
    WordSize = erlang:system_info(wordsize),
    {NrOfSubs + NrOfRemoteSubs, Memory * WordSize}.

info(T, What) ->
    case ets:info(T, What) of
        undefined -> 0;
        V -> V
    end.

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
    NumWorkers = application:get_env(vmq_server, reg_trie_workers, 8),
    persistent_term:put(subscribe_trie_ready, 0),
    Self = self(),
    spawn_link(
        fun() ->
            %% Wait for all workers to be registered before dispatching
            wait_for_workers(NumWorkers),
            ok = vmq_reg:fold_subscriptions(
                fun(Entry, Acc) ->
                    initialize_trie_entry(Entry, NumWorkers),
                    Acc
                end,
                ok
            ),
            Self ! subscribers_loaded
        end
    ),
    EventHandler = vmq_reg:subscribe_subscriber_changes(),
    {ok, #state{event_handler = EventHandler, num_workers = NumWorkers}}.

handle_call({event, Event}, _From, #state{event_handler = Handler, num_workers = N} = State) ->
    %% used only for testing/microbenchmarking
    handle_event(Handler, Event, N),
    {reply, ok, State};
handle_call(init_subs, _From, #state{num_workers = N} = State) ->
    persistent_term:put(subscribe_trie_ready, 0),
    Coordinator = self(),
    spawn_link(
        fun() ->
            ok = vmq_reg:fold_subscriptions(
                fun(Entry, Acc) ->
                    initialize_trie_entry(Entry, N),
                    Acc
                end,
                ok
            ),
            Coordinator ! subscribers_loaded
        end
    ),
    {reply, ok, State#state{status = init, event_queue = queue:new()}};
handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(
    subscribers_loaded,
    #state{
        event_handler = Handler,
        event_queue = Q,
        num_workers = N
    } = State
) ->
    lists:foreach(
        fun(Event) ->
            handle_event(Handler, Event, N)
        end,
        queue:to_list(Q)
    ),
    NrOfSubscribers = ets:info(vmq_trie_subs, size),
    NrOfRemoteSubscribers = ets:info(vmq_trie_remote_subs, size),
    persistent_term:put(subscribe_trie_ready, 1),
    ?LOG_INFO("loaded ~p local subscriptions and ~p remote subscriptions into ~p", [
        NrOfSubscribers, NrOfRemoteSubscribers, ?MODULE
    ]),
    {noreply, State#state{status = ready, event_queue = undefined}};
handle_info(Event, #state{status = init, event_queue = Q} = State) ->
    {noreply, State#state{event_queue = queue:in(Event, Q)}};
handle_info(Event, #state{event_handler = Handler, num_workers = N} = State) ->
    handle_event(Handler, Event, N),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions — event routing
%%%===================================================================

handle_event(Handler, Event, NumWorkers) ->
    case Handler(Event) of
        {delete, SubscriberId, Subscriptions} ->
            Removed = vmq_subscriber:get_changes(Subscriptions),
            WorkerId = route_worker(SubscriberId, NumWorkers),
            vmq_reg_trie_worker:worker_name(WorkerId) !
                {trie_event, {delete, SubscriberId, Removed}};
        {update, SubscriberId, OldValue, NewValue} ->
            {ToRemove, ToAdd} = vmq_subscriber:get_changes(OldValue, NewValue),
            WorkerId = route_worker(SubscriberId, NumWorkers),
            Worker = vmq_reg_trie_worker:worker_name(WorkerId),
            case ToRemove of
                [] -> ok;
                _ -> Worker ! {trie_event, {delete, SubscriberId, ToRemove}}
            end,
            case ToAdd of
                [] -> ok;
                _ -> Worker ! {trie_event, {add, SubscriberId, ToAdd}}
            end;
        ignore ->
            ok
    end.

route_worker(SubscriberId, NumWorkers) ->
    erlang:phash2(SubscriberId, NumWorkers).

initialize_trie_entry({_, _, {_, _, Node, CleanSession}}, _NumWorkers) when
    Node =:= node(), CleanSession == true
->
    ok;
initialize_trie_entry(
    {_MP, _Topic, {SubscriberId, _SubInfo, _Node, _CleanSession}} = Entry, NumWorkers
) ->
    WorkerId = route_worker(SubscriberId, NumWorkers),
    vmq_reg_trie_worker:init_entry(WorkerId, Entry);
initialize_trie_entry(Entry, _NumWorkers) ->
    ?LOG_WARNING("vmq_reg_trie: unexpected init entry: ~p", [Entry]),
    ok.

wait_for_workers(NumWorkers) ->
    wait_for_workers(0, NumWorkers).

wait_for_workers(Id, NumWorkers) when Id >= NumWorkers ->
    ok;
wait_for_workers(Id, NumWorkers) ->
    Name = vmq_reg_trie_worker:worker_name(Id),
    case whereis(Name) of
        undefined ->
            timer:sleep(1),
            wait_for_workers(Id, NumWorkers);
        _Pid ->
            wait_for_workers(Id + 1, NumWorkers)
    end.

%%%===================================================================
%%% Internal functions — read path (ETS reads, no mutations)
%%%===================================================================

match(MP, Topic) when is_list(MP) and is_list(Topic) ->
    TrieNodes = trie_match(MP, Topic),
    match(MP, Topic, TrieNodes, []).

%% [MQTT-4.7.2-1] The Server MUST NOT match Topic Filters starting with a
%% wildcard character (# or +) with Topic Names beginning with a $ character.
match(MP, [<<"$", _/binary>> | _] = Topic, [#trie_node{topic = [<<"#">>]} | Rest], Acc) ->
    match(MP, Topic, Rest, Acc);
match(MP, [<<"$", _/binary>> | _] = Topic, [#trie_node{topic = [<<"+">> | _]} | Rest], Acc) ->
    match(MP, Topic, Rest, Acc);
match(MP, Topic, [#trie_node{topic = Name} | Rest], Acc) when Name =/= undefined ->
    case ets:lookup(vmq_trie_topic, {MP, Name}) of
        [] ->
            match(MP, Topic, Rest, Acc);
        [{_, _, Nodes}] ->
            match(MP, Topic, Rest, match_(Name, Nodes, Acc))
    end;
match(MP, Topic, [_ | Rest], Acc) ->
    match(MP, Topic, Rest, Acc);
match(_, _, [], Acc) ->
    Acc.

match_(Topic, [{NodeOrGroup, _} | Rest], Acc) ->
    match_(Topic, Rest, [{Topic, NodeOrGroup} | Acc]);
match_(_, [], Acc) ->
    Acc.

trie_match(MP, Words) ->
    trie_match(MP, root, Words, []).

trie_match(MP, Node, [], ResAcc) ->
    NodeId = {MP, Node},
    ets:lookup(vmq_trie_node, NodeId) ++ 'trie_match_#'(NodeId, ResAcc);
trie_match(MP, Node, [W | Words], ResAcc) ->
    NodeId = {MP, Node},
    lists:foldl(
        fun(WArg, Acc) ->
            case
                ets:lookup(
                    vmq_trie,
                    #trie_edge{node_id = NodeId, word = WArg}
                )
            of
                [#trie{node_id = ChildId}] ->
                    trie_match(MP, ChildId, Words, Acc);
                [] ->
                    Acc
            end
        end,
        'trie_match_#'(NodeId, ResAcc),
        [W, <<"+">>]
    ).

'trie_match_#'({MP, _} = NodeId, ResAcc) ->
    case ets:lookup(vmq_trie, #trie_edge{node_id = NodeId, word = <<"#">>}) of
        [#trie{node_id = ChildId}] ->
            ets:lookup(vmq_trie_node, {MP, ChildId}) ++ ResAcc;
        [] ->
            ResAcc
    end.

get_remote_subscribers(MP, Topic) ->
    Key = {MP, Topic},
    case ets:lookup(vmq_trie_remote_subs, Key) of
        [] -> [];
        [{_, Remotes}] -> [{Topic, Node} || {Node, _} <- Remotes]
    end.
