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

-module(vmq_reg_trie_worker_sup).

-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    NumWorkers = application:get_env(vmq_server, reg_trie_workers, 8),
    WorkerChildren = [
        {
            vmq_reg_trie_worker:worker_name(Id),
            {vmq_reg_trie_worker, start_link, [Id]},
            permanent,
            5000,
            worker,
            [vmq_reg_trie_worker]
        }
     || Id <- lists:seq(0, NumWorkers - 1)
    ],
    {ok, {{one_for_one, 5, 10}, WorkerChildren}}.
