%% Copyright 2019, OpenX.  All rights reserved.
%% Licensed under the conditions specified in the accompanying LICENSE file.

-module(thrift_shackle_tests).

-include("ox_thrift.hrl").
-include("test_types.hrl").

-include_lib("eunit/include/eunit.hrl").

-define(POOL_NAME, thrift_pool).
-define(LOCALHOST, "127.0.0.1").
-define(PORT, 8024).
-define(SERVICE, test_service_thrift).
-define(PROTOCOL, ox_thrift_protocol_binary).
-define(HANDLER, ox_thrift_binary_tests).

setup () ->
  %% Start the thrift server.
  socket_transport:start_server(?PORT, ?SERVICE, ?PROTOCOL, ?HANDLER, undefined, []),

  %% Start the shackle application.
  Apps = {ok, _} = application:ensure_all_started(shackle),

  %% Start the shackle pool.
  InitOptions =
    [ {service_module, test_service_thrift},
      {protocol_module, ox_thrift_protocol_binary} ],
  ClientOptions =
    [ {port, ?PORT},
      {reconnect_time_min, 1},
      {socket_options, [ binary, {active, false}, {packet, raw}, {nodelay, true} ]},
      {init_options, InitOptions} ],
  PoolOptions =
    [ {backlog_size, 1},
      {pool_size, 2} ],
  shackle_pool:start(?POOL_NAME, ox_thrift_shackle_client, ClientOptions, PoolOptions),

  Apps.

cleanup ({ok, Apps}) ->
  shackle_pool:stop(?POOL_NAME),
  lists:foreach(fun (App) -> application:stop(App) end, Apps),
  socket_transport:stop_server(),
  ok.

shackle_test_ () ->
  {setup,
   fun setup/0, fun cleanup/1,
   [ fun add_one/0,
     fun throw_exception/0 ]
  }.

add_one () ->
  ?assertEqual({ok, 100}, shackle:call(?POOL_NAME, {add_one, [ 99 ]})),
  ok.

throw_exception () ->
  ?assertMatch({throw, _}, shackle:call(?POOL_NAME, {throw_exception, [ ?TEST_THROWTYPE_DECLAREDEXCEPTION ]})),
  ok.
