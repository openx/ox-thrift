-module(ox_thrift_tests).

-include_lib("eunit/include/eunit.hrl").

-include("test_types.hrl").
-include("ox_thrift.hrl").

-behaviour(ox_thrift_server).

-export([ handle_function/2, handle_error/2, handle_stat/3 ]).
-export([ sum_ints/2, echo/1, throw_exception/1 ]). % Export so that compiler doesn't complain about unused function.

-define(SERVICE, test_service_thrift).
-define(PROTOCOL, ox_thrift_protocol_binary).
-define(HANDLER, ?MODULE).
-define(STATS_MODULE, ?MODULE).
-define(STATS_TABLE, ox_thrift_stats).

new_client_direct () ->
  create_stats_table(),
  SocketFun = direct_transport:make_get_socket(?SERVICE, ?PROTOCOL, ?HANDLER, ?STATS_MODULE),
  {ok, Client} = ox_thrift_client:new(SocketFun, direct_transport, ?PROTOCOL, ?SERVICE),
  Client.

destroy_client_direct (Client) ->
  ox_thrift_client:close(Client),
  destroy_stats_table(),
  ok.


-define(PORT, 8024).

new_client_socket () ->
  create_stats_table(),
  socket_transport:start_server(?PORT, ?SERVICE, ?PROTOCOL, ?HANDLER, ?STATS_MODULE),
  SocketFun = socket_transport:make_get_socket(?PORT),
  {ok, Client} = ox_thrift_client:new(SocketFun, socket_transport, ?PROTOCOL, ?SERVICE),
  Client.

destroy_client_socket (Client) ->
  ox_thrift_client:close(Client),
  timer:sleep(1), % Give handle_error a better chance to be called.
  socket_transport:stop_server(),
  destroy_stats_table(),
  ok.


new_client_skip () ->
  SocketFun = direct_transport:make_get_socket(skip_service_thrift, ?PROTOCOL, skip_handler, undefined),
  %% SocketFun = direct_transport:make_get_socket(?SERVICE, ?PROTOCOL, ?MODULE, undefined),
  {ok, Client} = ox_thrift_client:new(SocketFun, direct_transport, ?PROTOCOL, ?SERVICE),
  Client.

destroy_client_skip (Client) ->
  ox_thrift_client:close(Client),
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

direct_test_ () ->
  make_tests(direct, fun new_client_direct/0, fun destroy_client_direct/1).

socket_test_ () ->
  make_tests(socket, fun new_client_socket/0, fun destroy_client_socket/1).

timeout_test () ->
  Client0 = new_client_socket(),
  ?assertEqual([], ets:lookup(?STATS_TABLE, {undefined, timeout})),
  application:set_env(ox_thrift, exceptions_include_traces, true),
  {Client1, Res1} = ox_thrift_client:call(Client0, add_one, [ 1 ]),
  ?assertEqual(2, Res1),
  ?assertEqual([], ets:lookup(?STATS_TABLE, {undefined, timeout})),
  timer:sleep(200), %% Server recv should time out.
  ?assertEqual([ {{undefined, timeout}, 1} ], ets:lookup(?STATS_TABLE, {undefined, timeout})),
  {Client2, Res2} = ox_thrift_client:call(Client1, add_one, [ 2 ]),
  ?assertEqual(3, Res2),
  ?assertEqual([ {{undefined, timeout}, 1} ], ets:lookup(?STATS_TABLE, {undefined, timeout})),
  destroy_client_socket(Client2).


skip_test () ->
  Client0 = new_client_skip(),
  List = [ 2, 3, 5, 8, 13 ],
  Map = dict:from_list([{ <<"one">>, 1}, {<<"two">>, 2} ]),
  Expected = #'MissingFields'{
             first = 111,
             third = 3.1416,
             fifth = <<"ef-aye-vee-e">>,
             seventh = false,
             ninth = 99
            },
  Input = Expected#'MissingFields'{
             second_skip = 222,
             fourth_skip = List,
             sixth_skip = #'AllTypes'{
                             bool_field = true,
                             byte_field = 151,
                             double_field = 1.25,
                             string_field = <<"zyzyx">>,
                             int_list = List,
                             string_int_map = Map},
             eighth_skip = Map
           },

  {Client1, Output} = ox_thrift_client:call(Client0, missing, [ Input ]),
  %% ?assertEqual(Input, Output),
  ?assertEqual(Expected, Output),

  destroy_client_skip(Client1).


-define(F(TestName), fun () -> TestName(TestType, NewClient, DestroyClient) end).

make_tests (TestType, NewClient, DestroyClient) ->
  [ ?F(add_one_test)
  , ?F(sum_ints_test)
  , ?F(all_types_test)
  , ?F(throw_exception_test)
  , ?F(cast_test)
  ].

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

add_one_test (TestType, NewClientFun, DestroyClientFun) ->
  Client0 = NewClientFun(),

  {Client1, Reply1} = ox_thrift_client:call(Client0, add_one, [ 99 ]),
  ?assertEqual(100, Reply1),

  {Client2, Reply2} = ox_thrift_client:call(Client1, add_one, [ 42 ]),
  ?assertEqual(43, Reply2),

  Client3 = ox_thrift_client:close(Client2),

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
      ?assertMatch([ {call_count, 2} ],
                   ets:lookup(?STATS_TABLE, call_count)),

      ?assertMatch([ {connect_time, ConnectMillis} ] when ConnectMillis > 0,
                   ets:lookup(?STATS_TABLE, connect_time))
    end,

  DestroyClientFun(Client3).


sum_ints_test (_TestType, NewClientFun, DestroyClientFun) ->
  Client0 = NewClientFun(),

  {Client1, Reply1} = ox_thrift_client:call(Client0, sum_ints, [ #'Container'{first_field = 1, third_field = 10}, 35 ]),
  ?assertEqual(46, Reply1),

  {Client2, Reply2} = ox_thrift_client:call(Client1, sum_ints,
                                             [ #'Container'{first_field = 1,
                                                            second_struct = #'Integers'{int_field = 2,
                                                                                        int_list = [ 4, 8 ],
                                                                                        int_set = sets:from_list([ 16, 32 ])},
                                                            third_field = 64}, 128 ]),
  ?assertEqual(255, Reply2),

  DestroyClientFun(Client2).


all_types_test (_TestType, NewClientFun, DestroyClientFun) ->
  V = #'AllTypes'{
         bool_field = true,
         byte_field = 42,
         i16_field = 16#7f0e,
         i32_field = -123,
         i64_field = 16#12345678abcdef0,
         double_field = 10.125,
         string_field = <<"xyzzy">>,
         int_list = [ 1, 2, 3 ],
         string_set = sets:from_list([ <<"a">>, <<"bb">>, <<"ccc">> ]),
         string_int_map = dict:from_list([ {<<"I">>, 1}, {<<"V">>, 5}, {<<"X">>, 10} ])},

  Client0 = NewClientFun(),

  {Client1, Reply1} = ox_thrift_client:call(Client0, echo, [ V ]),
  ?assertEqual(V, Reply1),

  DestroyClientFun(Client1).


throw_exception_test (_TestType, NewClientFun, DestroyClientFun) ->
  Client0 = NewClientFun(),

  {Client1, Reply1} = ox_thrift_client:call(Client0, throw_exception, [ ?TEST_THROWTYPE_NORMALRETURN ]),
  ?assertEqual(101, Reply1),

  Client2 =
    try ox_thrift_client:call(Client1, throw_exception, [ ?TEST_THROWTYPE_DECLAREDEXCEPTION ]) of
        Result2 -> error({unexpected_success, ?MODULE, ?LINE, Result2})
    catch throw:{Client2a, Reply2} ->
        ?assertNotEqual(undefined, ox_thrift_client:get_socket(Client2a)),
        ?assertEqual(simple_exception(), Reply2),
        Client2a
    end,

  Client3 =
    try ox_thrift_client:call(Client2, throw_exception, [ ?TEST_THROWTYPE_UNDECLAREDEXCEPTION ]) of
        Result3 -> error({unexpected_success, ?MODULE, ?LINE, Result3})
    catch error:{Client3a, Reply3} ->
        ?assertEqual(undefined, ox_thrift_client:get_socket(Client3a)),
        ?assertMatch(#application_exception{type=0}, Reply3),
        Client3a
    end,

  Client4 =
    try ox_thrift_client:call(Client3, throw_exception, [ ?TEST_THROWTYPE_ERROR ]) of
        Result4 -> error({unexpected_success, ?MODULE, ?LINE, Result4})
    catch error:{Client4a, Reply4} ->
        ?assertEqual(undefined, ox_thrift_client:get_socket(Client4a)),
        ?assertMatch(#application_exception{type=0}, Reply4),
        Client4a
    end,

  DestroyClientFun(Client4).


cast_test (_TestType, NewClientFun, DestroyClientFun) ->
  Client0 = NewClientFun(),

  {Client1, Reply1} = ox_thrift_client:call(Client0, cast, [ <<"hello world">> ]),
  ?assertNotEqual(undefined, ox_thrift_client:get_socket(Client1)),
  ?assertEqual(ok, Reply1),

  DestroyClientFun(Client1).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

handle_function (Function, Args) ->
  case Function of
    add_one -> [ In ] = Args, {reply, In + 1};
    cast    -> [ Message ] = Args, io:format("~p\n", [ Message ]), ok;
    missing -> {reply, hd(Args)};
    _       -> Reply = apply(?MODULE, Function, Args),
               {reply, Reply}
  end.

handle_error (Function, Reason) ->
  %% io:format(standard_error, "handle_error ~p ~p\n", [ Function, Reason ]),
  incr_stat({Function, Reason}, [ {2, 1} ]),
  ok.

handle_stat (Function, Type, Value) ->
  %% io:format(standard_error, "handle_stat ~p ~p ~p\n", [ Function, Type, Value ]),
  {Key, Increment} =
    case Type of
      call_count   -> {Type, {2, Value}};
      connect_time -> {Type, {2, Value}};
      decode_time  -> {{Function, Type}, [ {2, 1}, {3, Value} ]};
      encode_time  -> {{Function, Type}, [ {2, 1}, {3, Value} ]}
    end,
  incr_stat(Key, Increment),
  ok.

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
    ?TEST_THROWTYPE_ERROR               -> error(unhandled_error)
  end.
