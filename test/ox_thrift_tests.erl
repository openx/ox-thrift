-module(ox_thrift_tests).

-include_lib("eunit/include/eunit.hrl").

-include("test_types.hrl").
-include("ox_thrift.hrl").

-export([ handle_function/2, handle_error/2 ]).
-export([ sum_ints/2, echo/1, throw_exception/1 ]). % Export so that compiler doesn't complain about unused function.

-define(SERVICE, test_service_thrift).
-define(PROTOCOL, ox_thrift_protocol_binary).
-define(HANDLER, ?MODULE).

%% -define(USE_DIRECT, true).

-ifdef(USE_DIRECT).

-define(TRANSPORT, direct_transport).

new_client () ->
  SocketFun = ?TRANSPORT:make_get_socket(?SERVICE, ?PROTOCOL, ?HANDLER),
  {ok, Client} = ox_thrift_client:new(SocketFun, ?TRANSPORT, ?PROTOCOL, ?SERVICE),
  Client.

destroy_client (Client) ->
  ox_thrift_client:close(Client),
  ok.

-else. %% ! USE_DIRECT

-define(TRANSPORT, socket_transport).
-define(PORT, 8024).

new_client () ->
  ?TRANSPORT:start_server(?PORT, ?SERVICE, ?PROTOCOL, ?HANDLER),
  SocketFun = ?TRANSPORT:make_get_socket(?PORT),
  {ok, Client} = ox_thrift_client:new(SocketFun, ?TRANSPORT, ?PROTOCOL, ?SERVICE),
  Client.

destroy_client (Client) ->
  ox_thrift_client:close(Client),
  ?TRANSPORT:stop_server(),
  ok.

-endif. %% ! USE_DIRECT

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

add_one_test () ->
  Client0 = new_client(),

  {Client1, Reply1} = ox_thrift_client:call(Client0, add_one, [ 99 ]),
  ?assertEqual(100, Reply1),

  {Client2, Reply2} = ox_thrift_client:call(Client1, add_one, [ 42 ]),
  ?assertEqual(43, Reply2),

  destroy_client(Client2).


sum_ints_test () ->
  Client0 = new_client(),

  {Client1, Reply1} = ox_thrift_client:call(Client0, sum_ints, [ #'Container'{first_field = 1, third_field = 10}, 35 ]),
  ?assertEqual(46, Reply1),

  {Client2, Reply2} = ox_thrift_client:call(Client1, sum_ints,
                                             [ #'Container'{first_field = 1,
                                                            second_struct = #'Integers'{int_field = 2,
                                                                                        int_list = [ 4, 8 ],
                                                                                        int_set = sets:from_list([ 16, 32 ])},
                                                            third_field = 64}, 128 ]),
  ?assertEqual(255, Reply2),

  destroy_client(Client2).


all_types_test () ->
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

  Client0 = new_client(),

  {Client1, Reply1} = ox_thrift_client:call(Client0, echo, [ V ]),
  ?assertEqual(V, Reply1),

  destroy_client(Client1).


throw_exception_test () ->
  Client0 = new_client(),

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

  destroy_client(Client4).


cast_test () ->
  Client0 = new_client(),

  {Client1, Reply1} = ox_thrift_client:call(Client0, cast, [ <<"hello world">> ]),
  ?assertNotEqual(undefined, ox_thrift_client:get_socket(Client1)),
  ?assertEqual(ok, Reply1),

  destroy_client(Client1).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

handle_function (Function, Args) ->
  case Function of
    add_one -> [ In ] = Args, {reply, In + 1};
    cast    -> [ Message ] = Args, io:format("~p\n", [ Message ]), ok;
    _       -> Reply = apply(?MODULE, Function, Args),
               {reply, Reply}
  end.

handle_error (_Function, _Reason) ->
  %% io:format(standard_error, "handle_error ~p ~p\n", [ _Function, _Reason ]),
  ok.

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
