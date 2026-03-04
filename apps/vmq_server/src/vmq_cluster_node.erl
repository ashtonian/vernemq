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
-module(vmq_cluster_node).
-include("vmq_server.hrl").
-include_lib("kernel/include/logger.hrl").

%% API
-export([
    start_link/1,
    publish/2,
    publish_async/2,
    enqueue/4,
    enqueue_async/3,
    connect_params/1,
    status/1
]).

%% gen_server callbacks
-export([
    init/1,
    loop/1
]).

-export([system_continue/3]).
-export([system_terminate/4]).
-export([system_code_change/4]).

-record(state, {
    parent,
    node,
    socket,
    transport,
    reachable = false,
    %% Two-queue buffer: q0 holds QoS 0 messages (evicted first),
    %% q12 holds QoS 1/2 messages (evicted last). q_size tracks total bytes.
    q0 = queue:new(),
    q12 = queue:new(),
    q_size = 0,
    max_queue_size,
    drop_policy,
    reconnect_tref,
    async_connect_pid,
    backoff_count = 0,
    bytes_dropped = {os:timestamp(), 0},
    bytes_send = {os:timestamp(), 0}
}).

-define(DEFAULT_RECONNECT_BASE, 1000).
-define(DEFAULT_RECONNECT_MAX, 10000).

%%%===================================================================
%%% API
%%%===================================================================

start_link(RemoteNode) ->
    proc_lib:start_link(?MODULE, init, [[self(), RemoteNode]]).

publish(Pid, Msg) ->
    Ref = make_ref(),
    MRef = monitor(process, Pid),
    Pid ! {msg, self(), Ref, Msg},
    receive
        {Ref, Reply} ->
            demonitor(MRef, [flush]),
            Reply;
        {'DOWN', MRef, process, Pid, Reason} ->
            {error, Reason}
    end.

publish_async(Pid, Msg) ->
    Pid ! {msg_async, Msg},
    ok.

enqueue(Pid, Term, BufferIfUnreachable, Timeout) ->
    Ref = make_ref(),
    MRef = monitor(process, Pid),
    Pid ! {enq, self(), Ref, Term, BufferIfUnreachable},
    case Timeout of
        infinity ->
            receive
                {Ref, Reply} ->
                    demonitor(MRef, [flush]),
                    Reply;
                {'DOWN', MRef, process, Pid, Reason} ->
                    {error, Reason}
            end;
        _ ->
            receive
                {Ref, Reply} ->
                    demonitor(MRef, [flush]),
                    Reply;
                {'DOWN', MRef, process, Pid, Reason} ->
                    {error, Reason}
            after Timeout ->
                demonitor(MRef, [flush]),
                {error, timeout}
            end
    end.

enqueue_async(Pid, Term, BufferIfUnreachable) ->
    Ref = make_ref(),
    MRef = monitor(process, Pid),
    Pid ! {enq, self(), Ref, Term, BufferIfUnreachable},
    {MRef, Ref}.

status(Pid) ->
    Ref = make_ref(),
    MRef = monitor(process, Pid),
    Pid ! {status, self(), Ref},
    receive
        {Ref, Reply} ->
            demonitor(MRef, [flush]),
            Reply;
        {'DOWN', MRef, process, Pid, Reason} ->
            {error, Reason}
    end.

init([Parent, RemoteNode]) ->
    MaxQueueSize = vmq_config:get_env(outgoing_clustering_buffer_size),
    DropPolicy = vmq_config:get_env(outgoing_clustering_buffer_drop_policy),
    proc_lib:init_ack(Parent, {ok, self()}),
    % Delay the initial connect attempt, this is useful when automating
    % cluster node setup, where multiple nodes are concurrently setup.
    % Without a delay a node may try to connect to a cluster node that
    % hasn't finished setting up the vmq cluster listener.
    InitDelay = vmq_config:get_env(
        outgoing_clustering_reconnect_base_delay, ?DEFAULT_RECONNECT_BASE
    ),
    erlang:send_after(InitDelay, self(), reconnect),
    loop(#state{
        parent = Parent,
        node = RemoteNode,
        max_queue_size = MaxQueueSize,
        drop_policy = DropPolicy
    }).

loop(#state{q_size = QSize, reachable = Reachable} = State) when
    QSize == 0;
    Reachable == false
->
    receive
        M ->
            loop(handle_message(M, State))
    end;
loop(#state{} = State) ->
    receive
        M ->
            loop(handle_message(M, State))
    after 0 ->
        loop(internal_flush(State))
    end.

buffer_message(
    BinMsg,
    QoS,
    #state{
        q_size = QSize,
        max_queue_size = Max,
        reachable = Reachable,
        drop_policy = DropPolicy,
        bytes_dropped = {{M, S, _}, V}
    } = State
) ->
    MsgSize = byte_size(BinMsg),
    {NewState, Evicted, IncomingDropped} =
        case Reachable of
            true ->
                {buf_enqueue(BinMsg, QoS, MsgSize, State), 0, 0};
            false ->
                case QSize + MsgSize =< Max of
                    true ->
                        {buf_enqueue(BinMsg, QoS, MsgSize, State), 0, 0};
                    false ->
                        drop_and_buf_enqueue(BinMsg, QoS, MsgSize, DropPolicy, State)
                end
        end,
    TotalDropped = Evicted + IncomingDropped,
    NewBytesDropped =
        case os:timestamp() of
            {M, S, _} = TS ->
                {TS, V + TotalDropped};
            TS ->
                _ = vmq_metrics:incr_cluster_bytes_dropped(V + TotalDropped),
                {TS, 0}
        end,
    {IncomingDropped, maybe_flush(NewState#state{bytes_dropped = NewBytesDropped})}.

%% Both policies are QoS-aware: evict QoS 0 messages before QoS 1/2.
%% lifo: evict newest buffered messages first (queue:out_r)
%% fifo: evict oldest buffered messages first (queue:out)
drop_and_buf_enqueue(BinMsg, QoS, MsgSize, Policy, State) ->
    OutFun =
        case Policy of
            fifo -> fun queue:out/1;
            lifo -> fun queue:out_r/1
        end,
    {State1, Evicted} = evict_until_fits(MsgSize, OutFun, State, 0),
    case State1#state.q_size + MsgSize =< State1#state.max_queue_size of
        true ->
            {buf_enqueue(BinMsg, QoS, MsgSize, State1), Evicted, 0};
        false ->
            %% Could not free enough space (e.g. single message > max) — drop incoming
            {State1, Evicted, MsgSize}
    end.

buf_enqueue(BinMsg, 0, MsgSize, #state{q0 = Q0, q_size = QSize} = State) ->
    State#state{q0 = queue:in(BinMsg, Q0), q_size = QSize + MsgSize};
buf_enqueue(BinMsg, _QoS, MsgSize, #state{q12 = Q12, q_size = QSize} = State) ->
    State#state{q12 = queue:in(BinMsg, Q12), q_size = QSize + MsgSize}.

%% Evict messages until the new message fits. QoS 0 messages are evicted
%% before QoS 1/2. OutFun determines eviction direction:
%%   queue:out/1  (fifo) - evict from front (oldest first)
%%   queue:out_r/1 (lifo) - evict from rear (newest first)
evict_until_fits(MsgSize, OutFun, #state{q0 = Q0, q12 = Q12, q_size = QSize, max_queue_size = Max} = State, Evicted) ->
    case QSize + MsgSize =< Max of
        true ->
            {State, Evicted};
        false ->
            case OutFun(Q0) of
                {{value, Old}, Q0New} ->
                    Freed = byte_size(Old),
                    evict_until_fits(
                        MsgSize,
                        OutFun,
                        State#state{q0 = Q0New, q_size = QSize - Freed},
                        Evicted + Freed
                    );
                {empty, _} ->
                    case OutFun(Q12) of
                        {{value, Old}, Q12New} ->
                            Freed = byte_size(Old),
                            evict_until_fits(
                                MsgSize,
                                OutFun,
                                State#state{q12 = Q12New, q_size = QSize - Freed},
                                Evicted + Freed
                            );
                        {empty, _} ->
                            {State, Evicted}
                    end
            end
    end.

%% Extract QoS from the term for buffer priority classification.
extract_qos(#vmq_msg{qos = QoS}) -> QoS;
extract_qos({enqueue_many, _, [{deliver, QoS, _} | _], _}) -> QoS;
extract_qos(_) -> 1.

handle_message(
    {enq, CallerPid, Ref, _, BufferIfUnreachable},
    #state{reachable = false} = State
) when
    BufferIfUnreachable =:= false
->
    CallerPid ! {Ref, {error, not_reachable}},
    State;
handle_message({enq, CallerPid, Ref, Term, _}, State) ->
    Bin = term_to_binary({CallerPid, Ref, Term}),
    L = byte_size(Bin),
    BinMsg = <<"enq", L:32, Bin/binary>>,
    QoS = extract_qos(Term),
    %% buffering is allowed, but will only happen if the remote node
    %% is unreachable
    {Dropped, NewState} = buffer_message(BinMsg, QoS, State),
    case Dropped > 0 of
        true ->
            CallerPid ! {Ref, {error, msg_dropped}};
        false ->
            %% reply directly from other node
            ignore
    end,
    NewState;
handle_message({msg, CallerPid, Ref, #vmq_msg{qos = QoS} = Msg}, State) ->
    Bin = term_to_binary(Msg),
    L = byte_size(Bin),
    BinMsg = <<"msg", L:32, Bin/binary>>,
    {Dropped, NewState} = buffer_message(BinMsg, QoS, State),
    case Dropped > 0 of
        true ->
            CallerPid ! {Ref, {error, msg_dropped}};
        false ->
            CallerPid ! {Ref, ok}
    end,
    NewState;
handle_message({msg, CallerPid, Ref, Msg}, State) ->
    Bin = term_to_binary(Msg),
    L = byte_size(Bin),
    BinMsg = <<"msg", L:32, Bin/binary>>,
    {Dropped, NewState} = buffer_message(BinMsg, 1, State),
    case Dropped > 0 of
        true ->
            CallerPid ! {Ref, {error, msg_dropped}};
        false ->
            CallerPid ! {Ref, ok}
    end,
    NewState;
handle_message({msg_async, Msg}, State) ->
    Bin = term_to_binary(Msg),
    L = byte_size(Bin),
    BinMsg = <<"msg", L:32, Bin/binary>>,
    {_Dropped, NewState} = buffer_message(BinMsg, State),
    NewState;
handle_message(
    {connect_async_done, AsyncPid, {ok, {Transport, Socket}}},
    #state{async_connect_pid = AsyncPid, node = RemoteNode} = State
) ->
    NodeName = term_to_binary(node()),
    L = byte_size(NodeName),
    Msg = [<<"vmq-connect">>, <<L:32, NodeName/binary>>],
    case send(Transport, Socket, Msg) of
        ok ->
            ?LOG_INFO("successfully connected to cluster node ~p", [RemoteNode]),
            State#state{
                socket = Socket,
                transport = Transport,
                %% !!! remote node is reachable
                async_connect_pid = undefined,
                reachable = true,
                backoff_count = 0
            };
        {error, Reason} ->
            ?LOG_WARNING("can't initiate connect to cluster node ~p due to ~p", [
                RemoteNode, Reason
            ]),
            close_reconnect(State)
    end;
handle_message({connect_async_done, AsyncPid, error}, #state{async_connect_pid = AsyncPid} = State) ->
    % connect_async already logged the error details
    close_reconnect(State);
handle_message(reconnect, #state{reachable = false} = State) ->
    connect(State#state{reconnect_tref = undefined});
handle_message({status, CallerPid, Ref}, #state{socket = Socket, reachable = Reachable} = State) ->
    Status =
        case Reachable of
            true ->
                up;
            false when Socket == undefined ->
                init;
            false ->
                down
        end,
    CallerPid ! {Ref, Status},
    State;
handle_message({system, From, Request}, #state{parent = Parent} = State) ->
    sys:handle_system_msg(Request, From, Parent, ?MODULE, [], State);
handle_message(
    {NetEvClosed, Socket}, #state{node = RemoteNode, socket = Socket, backoff_count = Count} = State
) when
    NetEvClosed == tcp_closed;
    NetEvClosed == ssl_closed
->
    NextDelay = reconnect_delay(Count + 1),
    ?LOG_WARNING(
        "connection to node ~p has been closed, reconnect in ~pms",
        [RemoteNode, NextDelay]
    ),
    close_reconnect(State);
handle_message(
    {NetEvError, Socket, Reason},
    #state{node = RemoteNode, socket = Socket, backoff_count = Count} = State
) when
    NetEvError == tcp_error;
    NetEvError == ssl_error
->
    NextDelay = reconnect_delay(Count + 1),
    ?LOG_WARNING(
        "connection to node ~p has been closed due to error ~p, reconnect in ~pms",
        [RemoteNode, Reason, NextDelay]
    ),
    close_reconnect(State);
handle_message(Msg, #state{node = Node, reachable = Reachable} = State) ->
    ?LOG_WARNING(
        "got unknown message ~p for node ~p (reachable ~p)",
        [Msg, Node, Reachable]
    ),
    State.

% tcp-over-ethernet MSS 1460
-define(FLUSH_THRESHOLD, 1460).
maybe_flush(#state{q_size = QSize} = State) ->
    case QSize >= ?FLUSH_THRESHOLD of
        true ->
            internal_flush(State);
        false ->
            State
    end.

internal_flush(#state{reachable = false} = State) ->
    State;
internal_flush(#state{q_size = 0} = State) ->
    State;
internal_flush(
    #state{
        q0 = Q0,
        q12 = Q12,
        q_size = QSize,
        node = Node,
        transport = Transport,
        socket = Socket,
        bytes_send = {{M, S, _}, V}
    } = State
) ->
    %% Drain both queues in FIFO order. QoS 1/2 messages are sent first
    %% (higher priority), followed by QoS 0.
    Pending = queue:to_list(Q12) ++ queue:to_list(Q0),
    L = QSize,
    Msg = [<<"vmq-send", L:32>> | Pending],
    case send(Transport, Socket, Msg) of
        ok ->
            NewBytesSend =
                case os:timestamp() of
                    {M, S, _} = TS ->
                        {TS, V + L};
                    TS ->
                        _ = vmq_metrics:incr_cluster_bytes_sent(V + L),
                        {TS, 0}
                end,
            State#state{
                q0 = queue:new(), q12 = queue:new(), q_size = 0,
                bytes_send = NewBytesSend
            };
        {error, Reason} ->
            ?LOG_WARNING(
                "can't send ~p bytes to ~p due to ~p, reconnect!",
                [QSize, Node, Reason]
            ),
            close_reconnect(State)
    end.

connect(#state{node = RemoteNode, reachable = false} = State) ->
    Self = self(),
    ConnectAsyncPid = spawn_link(fun() -> connect_async(Self, RemoteNode) end),
    State#state{async_connect_pid = ConnectAsyncPid}.

connect_async(ParentPid, RemoteNode) ->
    ConnectOpts = vmq_config:get_env(outgoing_connect_options),
    % the outgoing_connect_params_module must implement the connect_params/1 function
    ConnectParamsMod = vmq_config:get_env(outgoing_connect_params_module),
    ConnectTimeout = vmq_config:get_env(outgoing_connect_timeout),
    Reply =
        case rpc:call(RemoteNode, ConnectParamsMod, connect_params, [node()]) of
            {Transport, Host, Port} ->
                case
                    connect(
                        Transport,
                        Host,
                        Port,
                        lists:usort([
                            binary,
                            {active, true}
                            | ConnectOpts
                        ]),
                        ConnectTimeout
                    )
                of
                    {ok, Socket} ->
                        % at least tune 'buffer'
                        MaskedSocket = mask_socket(Transport, Socket),
                        {ok, BufSizes} = getopts(MaskedSocket, [sndbuf, recbuf, buffer]),
                        BufSize = lists:max([Sz || {_, Sz} <- BufSizes]),
                        setopts(MaskedSocket, [{buffer, BufSize}]),
                        case controlling_process(Transport, MaskedSocket, ParentPid) of
                            ok ->
                                {ok, {Transport, MaskedSocket}};
                            {error, Reason} ->
                                ?LOG_DEBUG("can't assign socket ownership to ~p due to ~p", [
                                    ParentPid, Reason
                                ]),
                                error
                        end;
                    {error, Reason} ->
                        ?LOG_WARNING("can't connect to cluster node ~p due to ~p", [
                            RemoteNode, Reason
                        ]),
                        error
                end;
            {badrpc, nodedown} ->
                %% we don't scream.. vmq_cluster_mon screams
                error;
            E ->
                ?LOG_WARNING("can't connect to cluster node ~p due to ~p", [RemoteNode, E]),
                error
        end,
    ParentPid ! {connect_async_done, self(), Reply}.

close_reconnect(#state{transport = Transport, socket = Socket, backoff_count = Count} = State) ->
    close(Transport, Socket),
    NewCount = Count + 1,
    State#state{
        async_connect_pid = undefined,
        reachable = false,
        socket = undefined,
        backoff_count = NewCount,
        reconnect_tref = reconnect_timer(NewCount)
    }.

reconnect_delay(Count) ->
    Base = vmq_config:get_env(
        outgoing_clustering_reconnect_base_delay, ?DEFAULT_RECONNECT_BASE
    ),
    Max = vmq_config:get_env(
        outgoing_clustering_reconnect_max_delay, ?DEFAULT_RECONNECT_MAX
    ),
    min(Base bsl min(Count - 1, 14), Max).

reconnect_timer(Count) ->
    Delay = reconnect_delay(Count),
    %% Add +/- 20% jitter to prevent synchronized reconnect storms.
    Jitter = Delay div 5,
    JitteredDelay = Delay - Jitter + rand:uniform(2 * Jitter + 1) - 1,
    erlang:send_after(JitteredDelay, self(), reconnect).

%% connect_params is called by a RPC
connect_params(_Node) ->
    case whereis(vmq_server_sup) of
        undefined ->
            %% vmq_server app not ready
            {error, not_ready};
        _ ->
            Listeners = vmq_config:get_env(listeners),
            MaybeSSLConfig = proplists:get_value(vmqs, Listeners, []),
            case connect_params(ssl, MaybeSSLConfig) of
                no_config ->
                    case proplists:get_value(vmq, Listeners) of
                        undefined ->
                            exit("can't connect to cluster node");
                        Config ->
                            connect_params(tcp, Config)
                    end;
                Config ->
                    Config
            end
    end.
mask_socket(gen_tcp, Socket) -> Socket;
mask_socket(ssl, Socket) -> {ssl, Socket}.
getopts({ssl, Socket}, Opts) ->
    ssl:getopts(Socket, Opts);
getopts(Socket, Opts) ->
    inet:getopts(Socket, Opts).

setopts({ssl, Socket}, Opts) ->
    ssl:setopts(Socket, Opts);
setopts(Socket, Opts) ->
    inet:setopts(Socket, Opts).

connect_params(tcp, [{{Addr, Port}, _} | _]) ->
    {gen_tcp, Addr, Port};
connect_params(ssl, [{{Addr, Port}, _} | _]) ->
    {ssl, Addr, Port};
connect_params(_, []) ->
    no_config.

send(gen_tcp, Socket, Msg) ->
    gen_tcp:send(Socket, Msg);
send(ssl, {'ssl', Socket}, Msg) ->
    ssl:send(Socket, Msg).

close(_, undefined) -> ok;
close(gen_tcp, Socket) -> gen_tcp:close(Socket);
close(ssl, {'ssl', Socket}) -> ssl:close(Socket).

connect(gen_tcp, Host, Port, Opts, Timeout) ->
    gen_tcp:connect(Host, Port, Opts, Timeout);
connect(ssl, Host, Port, Opts, Timeout) ->
    ssl:connect(Host, Port, Opts, Timeout).

controlling_process(gen_tcp, Socket, Pid) ->
    gen_tcp:controlling_process(Socket, Pid);
controlling_process(ssl, {'ssl', Socket}, Pid) ->
    ssl:controlling_process(Socket, Pid).

teardown(#state{socket = Socket, transport = Transport, async_connect_pid = AsyncPid}, Reason) ->
    case AsyncPid of
        undefined -> ignore;
        Pid -> exit(Pid, normal)
    end,
    case Reason of
        normal ->
            ?LOG_DEBUG("normally stopped", []);
        shutdown ->
            ?LOG_DEBUG("stopped due to shutdown", []);
        _ ->
            ?LOG_WARNING("stopped abnormally due to '~p'", [Reason])
    end,
    close(Transport, Socket),
    ok.

system_continue(_, _, State) ->
    loop(State).

-spec system_terminate(any(), _, _, _) -> no_return().
system_terminate(Reason, _, _, State) -> teardown(State, Reason).

system_code_change(Misc, _, _, _) ->
    {ok, Misc}.
