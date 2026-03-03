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

-module(vmq_reg_trie_table_owner).

-behaviour(gen_server).

-export([start_link/0]).
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    DefaultETSOpts = [
        public,
        named_table,
        {read_concurrency, true},
        {write_concurrency, true}
    ],
    _ = ets:new(vmq_trie, [{keypos, 2} | DefaultETSOpts]),
    _ = ets:new(vmq_trie_node, [{keypos, 2} | DefaultETSOpts]),
    _ = ets:new(vmq_trie_topic, [{keypos, 1} | DefaultETSOpts]),
    _ = ets:new(vmq_trie_subs, [set | DefaultETSOpts]),
    _ = ets:new(vmq_trie_subs_fanout, [ordered_set | DefaultETSOpts]),
    _ = ets:new(vmq_trie_remote_subs, [{keypos, 1} | DefaultETSOpts]),
    {ok, #{}}.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
