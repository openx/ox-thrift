-module(ox_thrift_stats).

-export([ init/0, record/2, read/0 ]).

init () ->
  case ets:info(?MODULE, size) of
    Size when is_integer(Size) ->
      ok;
    undefined ->
      ets:new(?MODULE, [ named_table, public ]),
      true = ets:insert_new(?MODULE, {encode, 0, 0}),
      true = ets:insert_new(?MODULE, {decode, 0, 0}),
      ok
  end.


-spec record (Type::'encode'|'decode', MicroSeconds::integer()) -> ok.
record (Type, MicroSeconds) ->
  try
    ets:update_counter(?MODULE, Type, [ {2, 1}, {3, MicroSeconds} ])
  catch
    %% The ets:update_counter will fail if init/0 wasn't called.
    error:badarg -> ok
  end,
  ok.

read () ->
  ets:foldl(
    fun ({Type, Count, MicroSeconds}, Acc) ->
        [ { Type, Count, MicroSeconds, round(MicroSeconds / Count) } | Acc ]
    end, [], ?MODULE).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-ifdef(EUNIT).

record_test () ->
  ?assertEqual(ok, init()),
  ?assertEqual(ok, record(encode, 100)),
  ?assertEqual(ok, record(encode, 200)),
  ?assertEqual(ok, record(decode, 123)),
  ?assertEqual([ {decode, 1, 123, 123}, {encode, 2, 300, 150} ],
               lists:sort(read())).

-endif. %% EUNIT
