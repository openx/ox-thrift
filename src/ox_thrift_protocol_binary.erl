-module(ox_thrift_protocol_binary).

-include("ox_thrift_internal.hrl").

-export([ write/1
        , read/2
        ]).

%% -define(DEBUG_READ, true).

%% There is an asymmetry to the `write/1' and `read/2' interfaces.  `write/1'
%% returns an iolist for the converted data, which is expected to be
%% integrated into a larger iolist for the entire message, while `read/2'
%% parses the data from a binary buffer and returns the parsed data and the
%% remaining buffer.

-define(VERSION_MASK, 16#FFFF0000).
-define(VERSION_1, 16#80010000).
-define(TYPE_MASK, 16#000000ff).

write (#protocol_message_begin{
          name = Name,
          type = Type,
          seqid = Seqid}) ->
  %% Write a version 1 message header.
  [ write({?tType_I32, ?VERSION_1 bor Type}), write({?tType_STRING, Name}), write({?tType_I32, Seqid}) ];

write (message_end) ->
  [];

write (#protocol_field_begin{
          name = _Name,
          type = Type,
          id = Id}) ->
  <<Type:8/big-signed, Id:16/big-signed>>;

write (field_stop) ->
  <<?tType_STOP:8/big-signed>>;

write (field_end) ->
  [];

write (#protocol_map_begin{
          ktype = KType,
          vtype = VType,
          size = Size}) ->
  <<KType:8/big-signed, VType:8/big-signed, Size:32/big-signed>>;

write (map_end) ->
  [];

write (#protocol_list_begin{
          etype = EType,
          size = Size}) ->
  <<EType:8/big-signed, Size:32/big-signed>>;

write (list_end) ->
  [];

write (#protocol_set_begin{
          etype = EType,
          size = Size}) ->
  <<EType:8/big-signed, Size:32/big-signed>>;

write (set_end) ->
  [];

write (#protocol_struct_begin{}) ->
  [];

write (struct_end) ->
  [];

write ({?tType_BOOL, true}) ->
  <<1:8/big-signed>>;

write ({?tType_BOOL, false}) ->
  <<0:8/big-signed>>;

write ({?tType_BYTE, Byte}) ->
  <<Byte:8/big-signed>>;

write ({?tType_I16, I16}) ->
  <<I16:16/big-signed>>;

write ({?tType_I32, I32}) ->
  <<I32:32/big-signed>>;

write ({?tType_I64, I64}) ->
  <<I64:64/big-signed>>;

write ({?tType_DOUBLE, Double}) ->
  <<Double:64/big-signed-float>>;

write ({?tType_STRING, Str}) when is_list(Str) ->
  Bin = list_to_binary(Str),
  BinLen = size(Bin),
  <<BinLen:32/big-signed, Bin/binary>>;

write ({?tType_STRING, Bin}) when is_binary(Bin) ->
  BinLen = size(Bin),
  <<BinLen:32/big-signed, Bin/binary>>.


-ifdef(DEBUG_READ).
read(Type, DataIn) ->
  Ret = {Val, DataOut} = readx(Type, DataIn),
  io:format("read(~p) -> {~p, ~p}\n", [ Type, Val, DataOut ]),
  Ret.
-define(READ, read_real).
-else. %% ! DEBUG_READ
-define(READ, read).
-endif. %% ! DEBUG_READ


?READ (message_begin, Data0) ->
  {Initial, Data1} = read(ui32, Data0),
  case Initial band ?VERSION_MASK of
    ?VERSION_1 ->
      Type = Initial band ?TYPE_MASK,
      {Name, Data2} = read(?tType_STRING, Data1),
      {SeqId, Data3} = read(?tType_I32, Data2),
      {#protocol_message_begin{name = Name, type = Type, seqid = SeqId}, Data3};
    0 ->
      %% No version header; read the old way.
      {Name, Data2}  = read_data(Initial, Data1),
      {Type, Data3}  = read(?tType_BYTE, Data2),
      {SeqId, Data4} = read(?tType_I32, Data3),
      {#protocol_message_begin{name = Name, type = Type, seqid = SeqId}, Data4};
    _ ->
      %% Unexpected version number.
      error({bad_binary_protocol_version, Initial})
  end;

?READ (message_end, Data) ->
  {ok, Data};

?READ (struct_begin, Data) ->
  {ok, Data};

?READ (struct_end, Data) ->
  {ok, Data};

?READ (field_begin, Data0) ->
  case Data0 of
    <<?tType_STOP:8/big-signed, Data1/binary>>  ->
      {#protocol_field_begin{type = ?tType_STOP}, Data1};
    <<Type:8/big-signed, Id:16/big-signed, Data1/binary>> ->
      {#protocol_field_begin{type = Type, id = Id}, Data1}
  end;

?READ (field_end, Data) ->
  {ok, Data};

%% This isn't necessary, since we never explicitly read a `field_stop', we
%% just find it when trying to read a `field_begin'.
%%
%% ?READ (field_stop, Data0) ->
%%   {?tType_STOP, Data1} = read(?tType_BYTE, Data0),
%%   {ok, Data1};

?READ (map_begin, Data0) ->
  <<KType:8/big-signed, VType:8/big-signed, Size:32/big-signed, Data1/binary>> = Data0,
  {#protocol_map_begin{ktype = KType, vtype = VType, size = Size}, Data1};

?READ (map_end, Data) ->
  {ok, Data};

?READ (list_begin, Data0) ->
  <<EType:8/big-signed, Size:32/big-signed, Data1/binary>> = Data0,
  {#protocol_list_begin{etype = EType, size = Size}, Data1};

?READ (list_end, Data) ->
  {ok, Data};

?READ (set_begin, Data0) ->
  <<EType:8/big-signed, Size:32/big-signed, Data1/binary>> = Data0,
  {#protocol_set_begin{etype = EType, size = Size}, Data1};

?READ (set_end, Data) ->
  {ok, Data};

?READ (?tType_BOOL, Data0) ->
  <<Bool:8/big-signed, Data1/binary>> = Data0,
  {Bool =/= 0, Data1};

?READ (?tType_BYTE, Data0) ->
  <<Val:8/integer-signed-big, Data1/binary>> = Data0,
  {Val, Data1};

?READ (?tType_I16, Data0) ->
  <<Val:16/integer-signed-big, Data1/binary>> = Data0,
  {Val, Data1};

?READ (?tType_I32, Data0) ->
  <<Val:32/integer-signed-big, Data1/binary>> = Data0,
  {Val, Data1};

?READ (?tType_I64, Data0) ->
  <<Val:64/integer-signed-big, Data1/binary>> = Data0,
  {Val, Data1};

?READ (?tType_DOUBLE, Data0) ->
  <<Val:64/float-signed-big, Data1/binary>> = Data0,
  {Val, Data1};

?READ (?tType_STRING, Data0) ->
  <<Size:32/big-signed, String:Size/binary, Data1/binary>> = Data0,
  {String, Data1};

?READ (ui32, Data0) ->
  %% Used for reading the message version header.
  <<Val:32/integer-unsigned-big, Data1/binary>> = Data0,
  {Val, Data1}.


-spec read_data(Size::integer(), DataIn::binary()) -> {Value::binary(), DataOut::binary()}.
read_data (Size, Data0) ->
  case Size of
    0 -> {<<>>, Data0};
    _ -> <<Val:Size/binary, Data1/binary>> = Data0,
         {Val, Data1}
  end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-ifdef(EUNIT).

message_test () ->
  P = #protocol_message_begin{name = <<"test">>, type = ?tMessageType_CALL, seqid = 16#7FFFFFF0},
  ?assertEqual({P, <<>>}, read(message_begin, iolist_to_binary(write(P)))),

  %% Old-style message header.
  ?assertEqual({P, <<>>}, read(message_begin, <<4:32/big, "test", ?tMessageType_CALL, 16#7FFFFFF0:32/big>>)).

field_test () ->
  P = #protocol_field_begin{type = ?tType_I32, id = 16#7FF0},
  ?assertEqual({P, <<>>}, read(field_begin, iolist_to_binary(write(P)))),

  ?assertEqual({ok, <<>>}, read(field_end, iolist_to_binary(write(field_end)))).

map_test () ->
  P = #protocol_map_begin{ktype = ?tType_BYTE, vtype = ?tType_STRING, size = 16#7FFFFFF1},
  ?assertEqual({P, <<>>}, read(map_begin, iolist_to_binary(write(P)))),

  ?assertEqual({ok, <<>>}, read(map_end, iolist_to_binary(write(map_end)))).

list_test () ->
  P = #protocol_list_begin{etype = ?tType_BYTE, size = 16#7FFFFFF2},
  ?assertEqual({P, <<>>}, read(list_begin, iolist_to_binary(write(P)))),

  ?assertEqual({ok, <<>>}, read(list_end, iolist_to_binary(write(list_end)))).

set_test () ->
  P = #protocol_set_begin{etype = ?tType_BYTE, size = 16#7FFFFFF3},
  ?assertEqual({P, <<>>}, read(set_begin, iolist_to_binary(write(P)))),

  ?assertEqual({ok, <<>>}, read(set_end, iolist_to_binary(write(set_end)))).

basic_test () ->
  B = 16#7F,
  ?assertEqual({B, <<>>}, read(?tType_BYTE, iolist_to_binary(write({?tType_BYTE, B})))),

  S = 16#7FFF,
  ?assertEqual({S, <<>>}, read(?tType_I16, iolist_to_binary(write({?tType_I16, S})))),

  I = 16#7FFFFFFF,
  ?assertEqual({I, <<>>}, read(?tType_I32, iolist_to_binary(write({?tType_I32, I})))),

  F = 1234.25,
  ?assertEqual({F, <<>>}, read(?tType_DOUBLE, iolist_to_binary(write({?tType_DOUBLE, F})))),

  SB = <<"hello, world">>,
  ?assertEqual({SB, <<>>}, read(?tType_STRING, iolist_to_binary(write({?tType_STRING, SB})))),

  SL = "hello, world",
  ?assertEqual({SB, <<>>}, read(?tType_STRING, iolist_to_binary(write({?tType_STRING, SL})))).

-endif. %% EUNIT
