%% Copyright 2016-2018, OpenX.  All rights reserved.
%% Licensed under the conditions specified in the accompanying LICENSE file.

-module(ox_thrift_tests).

-include_lib("eunit/include/eunit.hrl").

-include("test_types.hrl").
-include("ox_thrift.hrl").

-behaviour(ox_thrift_server).

-export([ handle_function/2, handle_error/2, handle_stats/2 ]).
-export([ sum_ints/2, echo/1, throw_exception/1 ]). % Export so that compiler doesn't complain about unused function.

-define(SERVICE, test_service_thrift).
-define(HANDLER, ?MODULE).
-define(STATS_MODULE, ?MODULE).
-define(STATS_TABLE, ox_thrift_stats).
-define(RECV_TIMEOUT_CLIENT, 200).

new_client_direct (Protocol) ->
  create_stats_table(),
  ConnectionState = direct_transport:new({?SERVICE, Protocol, ?HANDLER, ?STATS_MODULE}),
  {ok, Client} = ox_thrift_client:new(direct_transport, ConnectionState, direct_transport, Protocol, ?SERVICE),
  Client.

destroy_client_direct (Client) ->
  direct_transport:destroy(ox_thrift_client:get_connection_state(Client)),
  destroy_stats_table(),
  ok.


-define(LOCALHOST, "127.0.0.1").
-define(PORT, 8024).
-define(PROTOCOL, ox_thrift_protocol_binary).

new_client_socket () -> new_client_socket(dict).

new_client_socket (MapModule) ->
  create_stats_table(),
  socket_transport:start_server(?PORT, ?SERVICE, ?PROTOCOL, ?HANDLER, ?STATS_MODULE),
  ConnectionState = ox_thrift_reconnecting_socket:new({?LOCALHOST, ?PORT}),
  {ok, Client} = ox_thrift_client:new(ox_thrift_reconnecting_socket, ConnectionState, socket_transport, ?PROTOCOL, ?SERVICE,
                                      [ {map_module, MapModule}, {recv_timeout, ?RECV_TIMEOUT_CLIENT} ]),
  Client.

destroy_client_socket (Client) ->
  ConnectionState = ox_thrift_client:get_connection_state(Client),
  ox_thrift_reconnecting_socket:destroy(ConnectionState),
  timer:sleep(1), % Give handle_error a better chance to be called.
  socket_transport:stop_server(),
  destroy_stats_table(),
  ok.


new_client_skip (Protocol) ->
  %% ConnectionState = direct_transport:new({?SERVICE, Protocol, skip_handler, ?STATS_MODULE}),
  ConnectionState = direct_transport:new({skip_service_thrift, Protocol, skip_handler, undefined}),
  {ok, Client} = ox_thrift_client:new(direct_transport, ConnectionState, direct_transport, Protocol, ?SERVICE),
  Client.

destroy_client_skip (Client) ->
  direct_transport:destroy(ox_thrift_client:get_connection_state(Client)),
  ok.


create_stats_table () ->
  case ets:info(?STATS_TABLE, size) of
    Size when is_integer(Size) ->
      %% Table already exists.  EUnit runs tests in parallel, so it's easier
      %% to just create the ets table if it doesn't exist than to create and
      %% destroy the table for each test.
      ok;
    undefined ->
      ets:new(?STATS_TABLE, [ named_table, public ])
  end.

destroy_stats_table () ->
  ok.

%% print_stats_table () ->
%%   io:format(standard_error, "~p\n", [ ets:tab2list(?STATS_TABLE) ]).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

direct_binary_test_ () ->
  make_tests(direct, dict, fun () -> new_client_direct(ox_thrift_protocol_binary) end, fun destroy_client_direct/1).

direct_compact_test_ () ->
  make_tests(direct, dict, fun () -> new_client_direct(ox_thrift_protocol_compact) end, fun destroy_client_direct/1).

socket_dict_test_ () ->
  make_tests(socket, dict, fun () -> new_client_socket(dict) end, fun destroy_client_socket/1).

-ifndef(OXTHRIFT_NO_MAPS).
socket_maps_test_ () ->
  make_tests(socket, maps, fun () -> new_client_socket(maps) end, fun destroy_client_socket/1).
-endif. %% not OXTHRIFT_NO_MAPS

timeout_server_test () ->
  Client0 = new_client_socket(),
  ?assertEqual([], ets:lookup(?STATS_TABLE, {undefined, timeout})),
  application:set_env(ox_thrift, exceptions_include_traces, true),
  {ok, Client1, Res1} = ox_thrift_client:call(Client0, add_one, [ 1 ]),
  ?assertEqual(2, Res1),
  ?assertEqual([], ets:lookup(?STATS_TABLE, {undefined, timeout})),
  timer:sleep(200), %% Server recv should time out.
  ?assertEqual([ {{undefined, timeout}, 1} ], ets:lookup(?STATS_TABLE, {undefined, timeout})),
  {ok, Client2, Res2} = ox_thrift_client:call(Client1, add_one, [ 2 ]),
  ?assertEqual(3, Res2),
  ?assertEqual([ {{undefined, timeout}, 1} ], ets:lookup(?STATS_TABLE, {undefined, timeout})),
  destroy_client_socket(Client2).


timeout_client_test () ->
  Client0 = new_client_socket(),
  {ok, Client1, Res1} = ox_thrift_client:call(Client0, wait, [ ?RECV_TIMEOUT_CLIENT - 100 ]),
  ?assertEqual(ok, Res1),
  {error, Client2, timeout} = ox_thrift_client:call(Client1, wait, [ ?RECV_TIMEOUT_CLIENT + 100 ]),
  %% Check that ox_thrift_client successfully reconnects after connection is closed.
  {ok, Client3, Res3} = ox_thrift_client:call(Client2, add_one, [ 123 ]),
  ?assertEqual(124, Res3),
  destroy_client_socket(Client3).


skip_test () ->
  [ skip_test(ox_thrift_protocol_binary)
  , skip_test(ox_thrift_protocol_compact)
  ].

-define(F(TestName), {atom_to_list(TestType) ++ ":" ++ atom_to_list(TestName) ++ ":" ++ atom_to_list(MapModule),
                      fun () -> TestName(TestType, MapModule, NewClient, DestroyClient) end}).

make_tests (TestType, MapModule, NewClient, DestroyClient) ->
  [ ?F(add_one_test)
  , ?F(sum_ints_test)
  , ?F(all_types_test)
  , ?F(throw_exception_test)
  , ?F(cast_test)
  , ?F(proplist_as_map_test)
  , ?F(map_as_map_test)
  ].

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

add_one_test (TestType, _MapModule, NewClientFun, DestroyClientFun) ->
  Client0 = NewClientFun(),

  {ok, Client1, Reply1} = ox_thrift_client:call(Client0, add_one, [ 99 ]),
  ?assertEqual(100, Reply1),

  {ok, Client2, Reply2} = ox_thrift_client:call(Client1, add_one, [ 42 ]),
  ?assertEqual(43, Reply2),

  ConnectionModule = ox_thrift_client:get_connection_module(Client2),
  ConnectionModule:destroy(ox_thrift_client:get_connection_state(Client2)),

  %% Wait until server notices that connection was closed.
  timer:sleep(100),

  %% Check that stats were recorded.
  %% io:format(standard_error, "~p\n", [ ets:tab2list(?STATS_TABLE) ]),
  ?assertMatch([ {{add_one, encode_time}, CallCount, EncodeMillis} ] when CallCount >= 2 andalso EncodeMillis > 0,
               ets:lookup(?STATS_TABLE, {add_one, encode_time})),

  ?assertMatch([ {{add_one, decode_time}, CallCount, DecodeMillis} ] when CallCount >= 2 andalso DecodeMillis > 0,
               ets:lookup(?STATS_TABLE, {add_one, decode_time})),

  TestType =:= socket andalso
    %% The direct tests don't generate call_count and connect_time stats.
    begin
      ?assertMatch([ {call_count, CallCount} ] when CallCount >= 2,
                   ets:lookup(?STATS_TABLE, call_count)),

      ?assertMatch([ {connect_time, ConnectMillis} ] when ConnectMillis > 0,
                   ets:lookup(?STATS_TABLE, connect_time))
    end,

  DestroyClientFun(Client2).


sum_ints_test (_TestType, _MapModule, NewClientFun, DestroyClientFun) ->
  Client0 = NewClientFun(),

  {ok, Client1, Reply1} =
    ox_thrift_client:call(Client0, sum_ints, [ #'Container'{first_field = 1, third_field = 10}, 35 ]),
  ?assertEqual(46, Reply1),

  {ok, Client2, Reply2} =
    ox_thrift_client:call(Client1, sum_ints,
                          [ #'Container'{first_field = 1,
                                         second_struct = #'Integers'{int_field = 2,
                                                                     int_list = [ 4, 8 ],
                                                                     int_set = sets:from_list([ 16, 32 ])},
                                         third_field = 64}, 128 ]),
  ?assertEqual(255, Reply2),

  DestroyClientFun(Client2).


all_types_test (_TestType, MapModule, NewClientFun, DestroyClientFun) ->
  Client0 = NewClientFun(),

  V1 = #'AllTypes'{
          bool_field = true,
          byte_field = 42,
          i16_field = 16#7f0e,
          i32_field = -123,
          i64_field = 16#12345678abcdef0,
          double_field = 10.125,
          string_field = <<"xyzzy">>,
          int_list = [ 1, 2, 3 ],
          string_set = sets:from_list([ <<"a">>, <<"bb">>, <<"ccc">> ]),
          string_int_map = MapModule:from_list([ {<<"I">>, 1}, {<<"V">>, 5}, {<<"X">>, 10} ]),
          bool_list = [ true, true, false, true ],
          byte_list = [], %% empty list
          double_list = [ 1.0, -2.0 ],
          string_list = [ <<"one">>, <<"two">>, <<"three">> ]},
  {ok, Client1, Reply1} = ox_thrift_client:call(Client0, echo, [ V1 ]),
  ?assertEqual(V1, Reply1),

  %% Round-tripping an integer in a double field returns a float.
  V2 = #'AllTypes'{double_field = 123},
  {ok, Client2, Reply2} = ox_thrift_client:call(Client1, echo, [ V2 ]),
  ?assertEqual(#'AllTypes'{double_field = 123.0}, Reply2),

  %% Round-tripping a string in a string field returns a binary.
  V3 = #'AllTypes'{string_field = "string"},
  {ok, Client3, Reply3} = ox_thrift_client:call(Client2, echo, [ V3 ]),
  ?assertEqual(#'AllTypes'{string_field = <<"string">>}, Reply3),

  DestroyClientFun(Client3).


is_open (Client) ->
  ConnectionModule = ox_thrift_client:get_connection_module(Client),
  ConnectionModule:is_open(ox_thrift_client:get_connection_state(Client)).


throw_exception_test (_TestType, _MapModule, NewClientFun, DestroyClientFun) ->
  Client0 = NewClientFun(),

  {ok, Client1, Reply1} = ox_thrift_client:call(Client0, throw_exception, [ ?TEST_THROWTYPE_NORMALRETURN ]),
  ?assertEqual(101, Reply1),

  {Throw2, Client2, Reply2} =
    ox_thrift_client:call(Client1, throw_exception, [ ?TEST_THROWTYPE_DECLAREDEXCEPTION ]),
  %% Declared exception should not cause connection to be closed.
  ?assertEqual(throw, Throw2),
  ?assertEqual(true, is_open(Client2)),
  ?assertEqual(simple_exception(), Reply2),

  {Error3, Client3, Reply3} =
    ox_thrift_client:call(Client2, throw_exception, [ ?TEST_THROWTYPE_UNDECLAREDEXCEPTION ]),
  %% Undeclared exception should cause connection to be closed.
  ?assertEqual(error, Error3),
  ?assertEqual(false, is_open(Client3)),
  ?assertMatch(#application_exception{type=?tApplicationException_UNKNOWN}, Reply3),

  {Error4, Client4, Reply4} =
    ox_thrift_client:call(Client3, throw_exception, [ ?TEST_THROWTYPE_ERROR ]),
  %% Error should cause connection to be closed.
  ?assertEqual(error, Error4),
  ?assertEqual(false, is_open(Client4)),
  ?assertMatch(#application_exception{type=?tApplicationException_UNKNOWN}, Reply4),

  {Error5, Client5, Reply5} =
    ox_thrift_client:call(Client3, throw_exception, [ ?TEST_THROWTYPE_BADTHROW ]),
  %% Error should cause connection to be closed.
  ?assertEqual(error, Error5),
  ?assertEqual(false, is_open(Client5)),
  ?assertMatch(#application_exception{type=?tApplicationException_UNKNOWN}, Reply5),

  DestroyClientFun(Client4).


cast_test (_TestType, _MapModule, NewClientFun, DestroyClientFun) ->
  Client0 = NewClientFun(),

  Message = <<"hello, world">>,
  {ok, Client1, Reply1} = ox_thrift_client:call(Client0, put, [ Message ]),
  %% ?assertNotEqual(undefined, ox_thrift_client:get_socket(Client1)), FIXME
  ?assertEqual(ok, Reply1),

  {ok, Client2, Reply2} = ox_thrift_client:call(Client1, get, []),

  ?assertEqual(Message, Reply2),

  DestroyClientFun(Client2).


proplist_as_map_test (_TestType, MapModule, NewClientFun, DestroyClientFun) ->
  Client0 = NewClientFun(),

  PL0 = [ {1, <<"one">>}, {2, <<"two">>} ],
  PL1 = lists:map(fun ({K, V}) -> {V, K} end, PL0),
  D0 = MapModule:from_list(PL0),
  D1 = MapModule:from_list(PL1),

  {ok, Client1, Reply1} = ox_thrift_client:call(Client0, swapkv, [ ?TEST_MAPRET_RETURNDICT, PL0 ]),
  ?assertEqual(D1, Reply1),

  {ok, Client2, Reply2} = ox_thrift_client:call(Client1, swapkv, [ ?TEST_MAPRET_RETURNDICT, D0 ]),
  ?assertEqual(D1, Reply2),

  {ok, Client3, Reply3} = ox_thrift_client:call(Client2, swapkv, [ ?TEST_MAPRET_RETURNPROPLIST, PL0 ]),
  ?assertEqual(D1, Reply3),

  {ok, Client4, Reply4} = ox_thrift_client:call(Client3, swapkv, [ ?TEST_MAPRET_RETURNPROPLIST, D0 ]),
  ?assertEqual(D1, Reply4),

  DestroyClientFun(Client4).


-ifdef(OXTHRIFT_NO_MAPS).
map_as_map_test (_TestType, _MapModule, _NewClientFun, _DestroyClientFun) -> ok.
-else. %% ! OXTHRIFT_NO_MAPS
map_as_map_test (_TestType, MapModule, NewClientFun, DestroyClientFun) ->
  Client0 = NewClientFun(),

  PL0 = [ {1, <<"one">>}, {2, <<"two">>} ],
  PL1 = lists:map(fun ({K, V}) -> {V, K} end, PL0),
  M0 = maps:from_list(PL0),
  D1 = MapModule:from_list(PL1),

  {ok, Client1, Reply1} = ox_thrift_client:call(Client0, swapkv, [ ?TEST_MAPRET_RETURNMAP, M0 ]),
  ?assertEqual(D1, Reply1),

  DestroyClientFun(Client1).
-endif. %% ! OXTHRIFT_NO_MAPS


skip_test (Protocol) ->
  Client0 = new_client_skip(Protocol),

  List = [ 2, 3, 5, 8, 13 ],
  StringIntMap = dict:from_list([{ <<"one">>, 1}, {<<"two">>, 2} ]),

  ContainerOne = #'Container'{
                    first_field = 1,
                    second_struct = #'Integers'{int_field = 2, int_list = [ 3, 4, 5 ], int_set = [ 6, 7, 8 ]},
                    third_field = 9},
  ContainerTwo = #'Container'{first_field = 10, second_struct = #'Integers'{int_list = [ 11 ]}, third_field = 12},
  ContainerMap = dict:from_list([{ <<"one">>, ContainerOne}, {<<"two">>, ContainerTwo} ]),
  Expected = #'MissingFields'{
             first = 111,
             third = 3.1416,
             fifth = <<"ef-aye-vee-e">>,
             seventh = false,
             ninth = 99
            },
  Input1 = Expected#'MissingFields'{
             second_skip = 222,
             fourth_skip = List,
             sixth_skip = #'AllTypes'{
                             bool_field = true,
                             byte_field = 151,
                             double_field = 1.25,
                             string_field = <<"zyzyx">>,
                             int_list = List,
                             string_int_map = StringIntMap},
             eighth_skip = ContainerMap
            },

  {ok, Client1, Output1} = ox_thrift_client:call(Client0, missing, [ Input1 ]),
  ?assertEqual(Expected, Output1),

  %% Test empty maps with compact protocol.
  Input2 = Expected#'MissingFields'{
             second_skip = 222,
             fourth_skip = List,
             sixth_skip = #'AllTypes'{
                             bool_field = true,
                             byte_field = 151,
                             double_field = 1.25,
                             string_field = <<"zyzyx">>}
            },
  {ok, Client2, Output2} = ox_thrift_client:call(Client0, missing, [ Input2 ]),
  ?assertEqual(Expected, Output2),

  destroy_client_skip(Client2).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

handle_function (Function, Args) ->
  case Function of
    add_one -> [ In ] = Args, {reply, In + 1};
    wait    -> [ Milliseconds ] = Args, timer:sleep(Milliseconds), ok;
    put     -> [ Message ] = Args, io:format("~p\n", [ Message ]), put(message, Message), ok;
    get     -> {reply, get(message)};
    swapkv  -> [ RetType, Dict ] = Args,
               Proplist = dict:fold(fun (K, V, Acc) -> [ {V, K} | Acc ] end, [], Dict),
               Reply = case RetType of
                         ?TEST_MAPRET_RETURNDICT     -> dict:from_list(Proplist);
                         ?TEST_MAPRET_RETURNPROPLIST -> Proplist;
                         ?TEST_MAPRET_RETURNMAP      -> maps:from_list(Proplist)
                       end,
               {reply, Reply};
    missing -> {reply, hd(Args)};
    _       -> Reply = apply(?MODULE, Function, Args),
               {reply, Reply}
  end.

handle_error (Function, Reason) ->
  %% io:format(standard_error, "handle_error ~p ~p\n", [ Function, Reason ]),
  incr_stat({Function, Reason}, [ {2, 1} ]),
  ok.

handle_stats (Function, Stats) ->
  %% io:format(standard_error, "handle_stats ~p ~p ~p\n", [ Function, Type, Value ]),
  lists:foreach(
    fun ({Type, Value}) ->
        {Key, Increment} =
          case Type of
            call_count   -> {Type, {2, Value}};
            connect_time -> {Type, {2, Value}};
            decode_time  -> {{Function, Type}, [ {2, 1}, {3, Value} ]};
            encode_time  -> {{Function, Type}, [ {2, 1}, {3, Value} ]};
            _            -> {skip, undefined}

          end,
        incr_stat(Key, Increment)
    end, Stats).

incr_stat (skip, _Increment) -> ok;
incr_stat (Key, Increment) ->
  try ets:update_counter(?STATS_TABLE, Key, Increment)
  catch error:badarg ->
      IncrementList = if is_list(Increment)    -> Increment;
                         is_tuple(Increment)   -> [ Increment ];
                         is_integer(Increment) -> [ {2, Increment} ]
                      end,
      ets:insert_new(?STATS_TABLE, erlang:make_tuple(length(IncrementList) + 1, 0, [ {1, Key} | IncrementList ]))
  end.


sum_ints (#'Container'{first_field=FirstInt, second_struct=SecondStruct, third_field=ThirdInt}, SecondArg) ->
  SecondSum =
    case SecondStruct of
      undefined -> 0;
      #'Integers'{int_field=A1, int_list=A2, int_set=A3} ->
        if A1 == undefined -> 0;
           true            -> A1
        end +
          if A2 == undefined -> 0;
             true            -> lists:foldl(fun (E, Acc) -> E + Acc end, 0, A2)
          end +
          if A3 == undefined -> 0;
             true            -> sets:fold(fun (E, Acc) -> E + Acc end, 0, A3)
          end
    end,
  FirstInt + SecondSum + ThirdInt + SecondArg.

echo (V) ->
  V.

simple_exception () ->
  #'SimpleException'{message = <<"hello">>, line_number = 201}.

throw_exception (ThrowType) ->
  case ThrowType of
    ?TEST_THROWTYPE_NORMALRETURN        -> 101;
    ?TEST_THROWTYPE_DECLAREDEXCEPTION   -> throw(simple_exception());
    ?TEST_THROWTYPE_UNDECLAREDEXCEPTION -> throw({unhandled_exception, 1, 2, 3});
    ?TEST_THROWTYPE_ERROR               -> error(unhandled_error);
    ?TEST_THROWTYPE_BADTHROW            -> throw(not_a_tuple)
  end.
