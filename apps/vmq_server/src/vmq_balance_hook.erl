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

-module(vmq_balance_hook).
-include_lib("vernemq_dev/include/vernemq_dev.hrl").

-behaviour(auth_on_register_hook).
-behaviour(auth_on_register_m5_hook).

-export([
    auth_on_register/5,
    auth_on_register_m5/6
]).

-spec auth_on_register(_, _, _, _, _) ->
    ok | {ok, [auth_on_register_hook:reg_modifiers()]} | {error, any()} | next.
auth_on_register(_Peer, SubscriberId, _UserName, _Password, _CleanSession) ->
    case maybe_reject(SubscriberId) of
        accept -> next;
        reject -> {error, not_authorized}
    end.

-spec auth_on_register_m5(_, _, _, _, _, _) ->
    ok
    | {ok, auth_on_register_m5_hook:reg_modifiers()}
    | {error, #{reason_code => auth_on_register_m5_hook:err_reason_code_name()}}
    | {error, atom()}
    | next.
auth_on_register_m5(_Peer, SubscriberId, _UserName, _Password, _CleanStart, _Properties) ->
    case maybe_reject(SubscriberId) of
        accept -> next;
        reject -> {error, #{reason_code => server_busy}}
    end.

-spec maybe_reject(subscriber_id()) -> accept | reject.
maybe_reject(SubscriberId) ->
    case vmq_balance_srv:is_accepting() of
        true ->
            accept;
        false ->
            case vmq_reg:get_queue_pid(SubscriberId) of
                Pid when is_pid(Pid) ->
                    %% Existing local session — allow reconnect/takeover
                    accept;
                not_found ->
                    vmq_balance_srv:incr_rejections(),
                    reject
            end
    end.
