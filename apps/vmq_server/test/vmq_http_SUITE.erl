-module(vmq_http_SUITE).
-export([
         init_per_suite/1,
         end_per_suite/1,
         init_per_testcase/2,
         end_per_testcase/2,
         all/0
        ]).

-export([
          simple_healthcheck_test/1,
          health_includes_cluster_state_test/1,
          health_cluster_endpoint_test/1
        ]).

init_per_suite(_Config) ->
    cover:start(),
    _Config.

end_per_suite(_Config) ->
    _Config.

init_per_testcase(_Case, Config) ->
    vmq_test_utils:setup(),
    vmq_server_cmd:set_config(allow_anonymous, true),
    Config.

end_per_testcase(_, Config) ->
    vmq_test_utils:teardown(),
    Config.

all() ->
    [simple_healthcheck_test,
     health_includes_cluster_state_test,
     health_cluster_endpoint_test].

simple_healthcheck_test(_) ->
    %% we have to setup the listener here, because vmq_test_utils is overriding
    %% the default set in vmq_server.app.src
    vmq_server_cmd:listener_start(8888, [{http, true},
                                         {config_mod, vmq_health_http},
                                         {config_fun, routes}]),
    application:ensure_all_started(inets),
    {ok, {_Status, _Headers, Body}} = httpc:request("http://localhost:8888/health"),
    JsonResponse = vmq_json:decode(list_to_binary(Body), [return_maps, {labels, binary}]),
    <<"OK">> = maps:get(<<"status">>, JsonResponse).

%% Verify /health returns cluster_state field
health_includes_cluster_state_test(_) ->
    vmq_server_cmd:listener_start(8888, [{http, true},
                                         {config_mod, vmq_health_http},
                                         {config_fun, routes}]),
    application:ensure_all_started(inets),
    {ok, {{_, 200, _}, _Headers, Body}} =
        httpc:request("http://localhost:8888/health"),
    JsonResponse = vmq_json:decode(list_to_binary(Body), [return_maps, {labels, binary}]),
    <<"OK">> = maps:get(<<"status">>, JsonResponse),
    %% Single node is healthy
    <<"healthy">> = maps:get(<<"cluster_state">>, JsonResponse).

%% Verify /health/cluster endpoint returns tier details
health_cluster_endpoint_test(_) ->
    vmq_server_cmd:listener_start(8888, [{http, true},
                                         {config_mod, vmq_health_http},
                                         {config_fun, routes}]),
    application:ensure_all_started(inets),
    {ok, {{_, 200, _}, _Headers, Body}} =
        httpc:request("http://localhost:8888/health/cluster"),
    JsonResponse = vmq_json:decode(list_to_binary(Body), [return_maps, {labels, binary}]),
    <<"healthy">> = maps:get(<<"cluster_tier">>, JsonResponse),
    true = is_integer(maps:get(<<"total_nodes">>, JsonResponse)),
    true = is_integer(maps:get(<<"alive_nodes">>, JsonResponse)),
    true = is_list(maps:get(<<"unreachable_nodes_list">>, JsonResponse)).
