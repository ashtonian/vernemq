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

-module(vmq_reg_trie_worker).

-include("vmq_server.hrl").
-include("vmq_reg_trie.hrl").
-include_lib("kernel/include/logger.hrl").

-behaviour(gen_server).

%% API
-export([
    start_link/1,
    init_entry/2,
    worker_name/1,
    add_and_inc/2,
    rem_and_dec/2
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

%%%===================================================================
%%% API
%%%===================================================================

start_link(Id) ->
    Name = worker_name(Id),
    gen_server:start_link({local, Name}, ?MODULE, [], []).

-spec init_entry(non_neg_integer(), tuple()) -> ok.
init_entry(WorkerId, Entry) ->
    worker_name(WorkerId) ! {init_entry, Entry},
    ok.

-spec worker_name(non_neg_integer()) -> atom().
worker_name(Id) ->
    list_to_atom("vmq_reg_trie_worker_" ++ integer_to_list(Id)).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
    {ok, #{}}.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({init_entry, Entry}, State) ->
    handle_init_entry(Entry),
    {noreply, State};
handle_info({trie_event, {add, SubscriberId, Changes}}, State) ->
    vmq_subscriber:fold(fun handle_add_event/2, SubscriberId, Changes),
    {noreply, State};
handle_info({trie_event, {delete, SubscriberId, Changes}}, State) ->
    vmq_subscriber:fold(fun handle_delete_event/2, SubscriberId, Changes),
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions — event handling
%%%===================================================================

handle_add_event({[<<"$share">>, Group | Topic], SubInfo, Node}, {MP, _} = SubscriberId) ->
    add_complex_topic(MP, Topic, {Node, Group}, true),
    add_subscriber_group(MP, Node, Group, Topic, SubscriberId, SubInfo),
    SubscriberId;
handle_add_event({Topic, SubInfo, Node}, {MP, _} = SubscriberId) when Node == node() ->
    add_complex_topic(MP, Topic, Node, vmq_topic:contains_wildcard(Topic)),
    add_subscriber(MP, Topic, SubscriberId, SubInfo),
    SubscriberId;
handle_add_event({Topic, _, Node}, {MP, _} = SubscriberId) ->
    add_complex_topic(MP, Topic, Node, vmq_topic:contains_wildcard(Topic)),
    add_remote_subscriber(MP, Topic, Node),
    SubscriberId.

handle_delete_event({[<<"$share">>, Group | Topic], SubInfo, Node}, {MP, _} = SubscriberId) ->
    del_complex_topic(MP, Topic, {Node, Group}, true),
    del_subscriber_group(MP, Node, Group, Topic, SubscriberId, SubInfo),
    SubscriberId;
handle_delete_event({Topic, SubInfo, Node}, {MP, _} = SubscriberId) when Node == node() ->
    del_complex_topic(MP, Topic, Node, vmq_topic:contains_wildcard(Topic)),
    del_subscriber(MP, Topic, SubscriberId, SubInfo),
    SubscriberId;
handle_delete_event({Topic, _, Node}, {MP, _} = SubscriberId) ->
    del_complex_topic(MP, Topic, Node, vmq_topic:contains_wildcard(Topic)),
    del_remote_subscriber(MP, Topic, Node),
    SubscriberId.

handle_init_entry(
    {MP, [<<"$share">>, Group | Topic], {SubscriberId, SubInfo, Node, _CleanSession}}
) ->
    add_complex_topic(MP, Topic, {Node, Group}, true),
    add_subscriber_group(MP, Node, Group, Topic, SubscriberId, SubInfo);
handle_init_entry({_, _, {_, _, Node, CleanSession}}) when
    Node =:= node(), CleanSession == true
->
    ok;
handle_init_entry({MP, Topic, {SubscriberId, SubInfo, Node, _CleanSession}}) when
    Node =:= node()
->
    add_complex_topic(MP, Topic, Node, vmq_topic:contains_wildcard(Topic)),
    add_subscriber(MP, Topic, SubscriberId, SubInfo);
handle_init_entry({MP, Topic, {_SubscriberId, _SubInfo, Node, _CleanSession}}) ->
    add_complex_topic(MP, Topic, Node, vmq_topic:contains_wildcard(Topic)),
    add_remote_subscriber(MP, Topic, Node).

%%%===================================================================
%%% Internal functions — ETS mutations (concurrent-safe)
%%%===================================================================

add_complex_topic(_, _, _, false) ->
    ignore;
add_complex_topic(MP, Topic, Node, true) ->
    MPTopic = {MP, Topic},
    add_complex_topic_cas(MPTopic, Node),

    case ets:lookup(vmq_trie_node, MPTopic) of
        [#trie_node{topic = Topic}] ->
            ignore;
        _ ->
            _ = [trie_add_path(MP, Triple) || Triple <- vmq_topic:triples(Topic)],
            ets:insert(vmq_trie_node, #trie_node{node_id = MPTopic, topic = Topic})
    end.

add_complex_topic_cas(MPTopic, Node) ->
    case ets:lookup(vmq_trie_topic, MPTopic) of
        [] ->
            case ets:insert_new(vmq_trie_topic, {MPTopic, 1, [{Node, 1}]}) of
                true -> ok;
                false -> add_complex_topic_cas(MPTopic, Node)
            end;
        [{_, TotalCnt, Nodes} = Old] ->
            New = {MPTopic, TotalCnt + 1, add_and_inc(Node, Nodes)},
            case ets:select_replace(vmq_trie_topic, [{Old, [], [{const, New}]}]) of
                1 -> ok;
                0 -> add_complex_topic_cas(MPTopic, Node)
            end
    end.

del_complex_topic(_, _, _, false) ->
    ignore;
del_complex_topic(MP, Topic, NodeOrGroup, true) ->
    MPTopic = {MP, Topic},
    del_complex_topic_cas(MP, Topic, MPTopic, NodeOrGroup).

del_complex_topic_cas(MP, Topic, MPTopic, NodeOrGroup) ->
    case ets:lookup(vmq_trie_topic, MPTopic) of
        [{_, TotalCnt, Nodes} = Old] when TotalCnt > 1 ->
            New = {MPTopic, TotalCnt - 1, rem_and_dec(NodeOrGroup, Nodes)},
            case ets:select_replace(vmq_trie_topic, [{Old, [], [{const, New}]}]) of
                1 -> ok;
                0 -> del_complex_topic_cas(MP, Topic, MPTopic, NodeOrGroup)
            end;
        [{_, 1, _} = Old] ->
            case ets:select_delete(vmq_trie_topic, [{Old, [], [true]}]) of
                1 -> trie_delete(MP, Topic);
                0 -> del_complex_topic_cas(MP, Topic, MPTopic, NodeOrGroup)
            end;
        [] ->
            ignore
    end.

trie_add_path(MP, {Node, Word, Child}) ->
    NodeId = {MP, Node},
    Edge = #trie_edge{node_id = NodeId, word = Word},
    ets:insert_new(vmq_trie_node, #trie_node{node_id = {MP, Child}, edge_count = 0}),
    case ets:insert_new(vmq_trie, #trie{edge = Edge, node_id = Child}) of
        true ->
            ets:insert_new(vmq_trie_node, #trie_node{node_id = NodeId, edge_count = 0}),
            ets:update_counter(vmq_trie_node, NodeId, {#trie_node.edge_count, 1});
        false ->
            ok
    end.

trie_delete(MP, Topic) ->
    NodeId = {MP, Topic},
    ets:select_delete(vmq_trie_node, [
        {#trie_node{node_id = NodeId, edge_count = 0, topic = '_'}, [], [true]}
    ]),
    trie_delete_path(MP, lists:reverse(vmq_topic:triples(Topic))).

trie_delete_path(_, []) ->
    ok;
trie_delete_path(MP, [{Node, Word, _} | RestPath]) ->
    NodeId = {MP, Node},
    Edge = #trie_edge{node_id = NodeId, word = Word},
    ets:delete(vmq_trie, Edge),
    try ets:update_counter(vmq_trie_node, NodeId, {#trie_node.edge_count, -1}) of
        0 ->
            ets:select_delete(vmq_trie_node, [
                {#trie_node{node_id = NodeId, edge_count = 0, topic = undefined}, [], [true]}
            ]),
            trie_delete_path(MP, RestPath);
        _ ->
            ok
    catch
        error:badarg -> ok
    end.

add_subscriber_group(MP, Node, Group, Topic, SubscriberId, QoS) ->
    Key = {MP, Group, Node, Topic},
    Val = {Node, Group, SubscriberId, QoS},
    insert_trie_subs(Key, Val).

insert_trie_subs(Key, Val) ->
    ets:insert(vmq_trie_subs_fanout, {{Key, Val}}),
    ets:insert(vmq_trie_subs, {Key, fanout}).

del_subscriber_group(MP, Node, Group, Topic, SubscriberId, QoS) ->
    Key = {MP, Group, Node, Topic},
    Val = {Node, Group, SubscriberId, QoS},
    del_trie_subs(Key, Val).

%% Concurrent-safe delete: after removing the fanout entry and deleting
%% the marker, re-check whether a concurrent insert_trie_subs added new
%% entries. If so, re-insert the marker to avoid making them invisible.
del_trie_subs(Key, Val) ->
    ets:delete(vmq_trie_subs_fanout, {Key, Val}),
    case ets:select_count(vmq_trie_subs_fanout, [{{{Key, '_'}}, [], [true]}]) of
        0 ->
            ets:delete(vmq_trie_subs, Key),
            %% Re-check: a concurrent insert may have added entries
            %% between our select_count and delete. If so, restore marker.
            case ets:select_count(vmq_trie_subs_fanout, [{{{Key, '_'}}, [], [true]}]) of
                0 -> ok;
                _ -> ets:insert(vmq_trie_subs, {Key, fanout})
            end;
        _ ->
            ok
    end.

add_subscriber(MP, Topic, SubscriberId, QoS) ->
    Key = {MP, Topic},
    Val = {SubscriberId, QoS},
    insert_trie_subs(Key, Val).

del_subscriber(MP, Topic, SubscriberId, QoS) ->
    Key = {MP, Topic},
    Val = {SubscriberId, QoS},
    del_trie_subs(Key, Val).

add_remote_subscriber(MP, Topic, Node) ->
    Key = {MP, Topic},
    add_remote_subscriber_cas(Key, Node).

add_remote_subscriber_cas(Key, Node) ->
    case ets:lookup(vmq_trie_remote_subs, Key) of
        [] ->
            case ets:insert_new(vmq_trie_remote_subs, {Key, [{Node, 1}]}) of
                true -> ok;
                false -> add_remote_subscriber_cas(Key, Node)
            end;
        [{_, Remotes} = Old] ->
            New = {Key, add_and_inc(Node, Remotes)},
            case ets:select_replace(vmq_trie_remote_subs, [{Old, [], [{const, New}]}]) of
                1 -> ok;
                0 -> add_remote_subscriber_cas(Key, Node)
            end
    end.

del_remote_subscriber(MP, Topic, Node) ->
    Key = {MP, Topic},
    del_remote_subscriber_cas(Key, Node).

del_remote_subscriber_cas(Key, Node) ->
    case ets:lookup(vmq_trie_remote_subs, Key) of
        [] ->
            ignore;
        [{_, Remotes} = Old] ->
            case rem_and_dec(Node, Remotes) of
                [] ->
                    case ets:select_delete(vmq_trie_remote_subs, [{Old, [], [true]}]) of
                        1 -> ok;
                        0 -> del_remote_subscriber_cas(Key, Node)
                    end;
                NewRemotes ->
                    New = {Key, NewRemotes},
                    case ets:select_replace(vmq_trie_remote_subs, [{Old, [], [{const, New}]}]) of
                        1 -> ok;
                        0 -> del_remote_subscriber_cas(Key, Node)
                    end
            end
    end.

%%%===================================================================
%%% Shared helpers (exported for use by vmq_reg_trie coordinator)
%%%===================================================================

-spec add_and_inc(term(), [{term(), non_neg_integer()}]) -> [{term(), non_neg_integer()}].
add_and_inc(Node, Nodes) ->
    case lists:keyfind(Node, 1, Nodes) of
        {N, C} ->
            lists:keyreplace(Node, 1, Nodes, {N, C + 1});
        false ->
            [{Node, 1} | Nodes]
    end.

-spec rem_and_dec(term(), [{term(), non_neg_integer()}]) -> [{term(), non_neg_integer()}].
rem_and_dec(Node, Nodes) ->
    case lists:keyfind(Node, 1, Nodes) of
        {_, 1} ->
            lists:keydelete(Node, 1, Nodes);
        {N, C} ->
            lists:keyreplace(Node, 1, Nodes, {N, C - 1});
        false ->
            Nodes
    end.
