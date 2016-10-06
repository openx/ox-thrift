-module(encode_tests).

-include_lib("eunit/include/eunit.hrl").

-include("test_types.hrl").
-include("ox_thrift.hrl").

%% Modeled after assertEqual macro in assert.hrl.
-define(ROUND_TRIP_TEST(RecordDesc, Record),
        begin
          ((fun (__X) ->
                case ox_thrift_protocol_binary:decode_record(
                       RecordDesc,
                       ox_thrift_protocol_binary:encode_record(RecordDesc, Record)
                      ) of
                  __X -> ok;
                  __V -> erlang:error({assertEqual,
                                       [ {module, ?MODULE}
                                       , {line, ?LINE}
                                       , {expression, (??Record)}
                                       , {expected, __X}
                                       , {value, __V} ]})
                end
            end)(Record))
        end).

encode_decode_test () ->
  ?ROUND_TRIP_TEST({test_types, 'AllTypes'}, #'AllTypes'{}),
  ?ROUND_TRIP_TEST({test_types, 'AllTypes'}, #'AllTypes'{bool_field = true}),
  ?ROUND_TRIP_TEST({test_types, 'AllTypes'}, #'AllTypes'{bool_field = false}),
  ?ROUND_TRIP_TEST({test_types, 'AllTypes'}, #'AllTypes'{i16_field = 1}),
  ?ROUND_TRIP_TEST({test_types, 'AllTypes'}, #'AllTypes'{i16_field = -1}),
  ?ROUND_TRIP_TEST({test_types, 'AllTypes'}, #'AllTypes'{i16_field = 32767}),
  ?ROUND_TRIP_TEST({test_types, 'AllTypes'}, #'AllTypes'{i16_field = -32768}),
  ?ROUND_TRIP_TEST({test_types, 'AllTypes'}, #'AllTypes'{i32_field = 2147483647}),
  ?ROUND_TRIP_TEST({test_types, 'AllTypes'}, #'AllTypes'{i32_field = -2147483648}),
  ?ROUND_TRIP_TEST({test_types, 'AllTypes'}, #'AllTypes'{double_field = 2.0}),
  ?ROUND_TRIP_TEST({test_types, 'AllTypes'}, #'AllTypes'{string_field = <<"hello">>}),
  ?ROUND_TRIP_TEST({test_types, 'AllTypes'}, #'AllTypes'{int_list = []}),
  ?ROUND_TRIP_TEST({test_types, 'AllTypes'}, #'AllTypes'{int_list = [ 1, 2, 3 ]}),
  ?ROUND_TRIP_TEST({test_types, 'AllTypes'}, #'AllTypes'{string_set = sets:from_list([ <<"a">>, <<"b">>, <<"c">> ])}),
  ?ROUND_TRIP_TEST({test_types, 'AllTypes'}, #'AllTypes'{string_int_map = dict:from_list([ {<<"one">>, 1}, {<<"two">>, 2} ])}),
  ?ROUND_TRIP_TEST({test_types, 'Container'}, #'Container'{first_field = 1, second_struct = #'Integers'{int_field = 2}, third_field = 3}).
