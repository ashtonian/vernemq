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

%% Long-lived worker process for async webhook dispatch.
%% Processes webhook calls sequentially from its mailbox,
%% eliminating per-call process creation/teardown overhead.
-module(vmq_webhooks_async_worker).
-include_lib("kernel/include/logger.hrl").

-export([start_link/1, worker_name/1]).

-spec start_link(non_neg_integer()) -> {ok, pid()}.
start_link(Id) ->
    Pid = proc_lib:spawn_link(fun() ->
        register(worker_name(Id), self()),
        loop()
    end),
    {ok, Pid}.

-spec worker_name(non_neg_integer()) -> atom().
worker_name(Id) ->
    list_to_atom("vmq_webhooks_async_" ++ integer_to_list(Id)).

loop() ->
    receive
        {call_endpoint, Endpoint, EOpts, HookName, Args, InflightRef} ->
            try
                _ = vmq_webhooks_plugin:call_endpoint(Endpoint, EOpts, HookName, Args)
            catch
                Class:Reason:Stack ->
                    ?LOG_ERROR("async webhook ~p crashed: ~p:~p~n~p",
                               [HookName, Class, Reason, Stack])
            after
                atomics:sub(InflightRef, 1, 1)
            end,
            loop()
    end.
