-module(ox_thrift_protocol_binary).

-include("ox_thrift_internal.hrl").

-export([ write/1
        , write/2
        , read/2
        ]).

-export([ encode_call/4, encode_message/5, decode_message/2 ]).

-include("ox_thrift_protocol.hrl").

term_to_wire(field_stop)        -> ?tType_STOP;
term_to_wire(bool)              -> ?tType_BOOL;
term_to_wire(byte)              -> ?tType_BYTE;
term_to_wire(double)            -> ?tType_DOUBLE;
term_to_wire(i16)               -> ?tType_I16;
term_to_wire(i32)               -> ?tType_I32;
term_to_wire(i64)               -> ?tType_I64;
term_to_wire(string)            -> ?tType_STRING;
term_to_wire(struct)            -> ?tType_STRUCT;
term_to_wire(map)               -> ?tType_MAP;
term_to_wire(set)               -> ?tType_SET;
term_to_wire(list)              -> ?tType_LIST.

wire_to_term(?tType_STOP)       -> field_stop;
wire_to_term(?tType_BOOL)       -> bool;
wire_to_term(?tType_BYTE)       -> byte;
wire_to_term(?tType_DOUBLE)     -> double;
wire_to_term(?tType_I16)        -> i16;
wire_to_term(?tType_I32)        -> i32;
wire_to_term(?tType_I64)        -> i64;
wire_to_term(?tType_STRING)     -> string;
wire_to_term(?tType_STRUCT)     -> struct;
wire_to_term(?tType_MAP)        -> map;
wire_to_term(?tType_SET)        -> set;
wire_to_term(?tType_LIST)       -> list.

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
  [ write(i32, ?VERSION_1 bor Type), write(string, Name), write(i32, Seqid) ];

write (message_end) ->
  [];

write (#protocol_field_begin{
          name = _Name,
          type = Type,
          id = Id}) ->
  TypeWire = term_to_wire(Type),
  <<TypeWire:8/big-signed, Id:16/big-signed>>;

write (field_stop) ->
  <<?tType_STOP:8/big-signed>>;

write (field_end) ->
  [];

write (#protocol_map_begin{
          ktype = KType,
          vtype = VType,
          size = Size}) ->
  KTypeWire = term_to_wire(KType),
  VTypeWire = term_to_wire(VType),
  <<KTypeWire:8/big-signed, VTypeWire:8/big-signed, Size:32/big-signed>>;

write (map_end) ->
  [];

write (#protocol_list_begin{
          etype = EType,
          size = Size}) ->
  ETypeWire = term_to_wire(EType),
  <<ETypeWire:8/big-signed, Size:32/big-signed>>;

write (list_end) ->
  [];

write (#protocol_set_begin{
          etype = EType,
          size = Size}) ->
  ETypeWire = term_to_wire(EType),
  <<ETypeWire:8/big-signed, Size:32/big-signed>>;

write (set_end) ->
  [];

write (#protocol_struct_begin{}) ->
  [];

write (struct_end) ->
  [].


write (bool, true) ->
  <<1:8/big-signed>>;

write (bool, false) ->
  <<0:8/big-signed>>;

write (byte, Byte) ->
  <<Byte:8/big-signed>>;

write (i16, I16) ->
  <<I16:16/big-signed>>;

write (i32, I32) ->
  <<I32:32/big-signed>>;

write (i64, I64) ->
  <<I64:64/big-signed>>;

write (double, Double) ->
  <<Double:64/big-signed-float>>;

write (string, Str) when is_list(Str) ->
  Bin = list_to_binary(Str),
  BinLen = size(Bin),
  <<BinLen:32/big-signed, Bin/binary>>;

write (string, Bin) when is_binary(Bin) ->
  BinLen = size(Bin),
  <<BinLen:32/big-signed, Bin/binary>>.


-ifdef(DEBUG_READ).
read(DataIn, Type) ->
  io:format("read(~p, ~p) ->\n", [ DataIn, Type ]),
  Ret = {DataOut, Val} = read_real(DataIn, Type),
  io:format("  {~p, ~p}\n", [ DataOut, Val ]),
  Ret.
-define(READ, read_real).
-else. %% ! DEBUG_READ
-define(READ, read).
-endif. %% ! DEBUG_READ


?READ (Data0, message_begin) ->
  {Data1, Initial} = read(Data0, ui32),
  case Initial band ?VERSION_MASK of
    ?VERSION_1 ->
      Type = Initial band ?TYPE_MASK,
      {Data2, Name} = read(Data1, string),
      {Data3, SeqId} = read(Data2, i32),
      {Data3, #protocol_message_begin{name = Name, type = Type, seqid = SeqId}};
    0 ->
      %% No version header; read the old way.
      {Data2, Name}  = read_data(Data1, Initial),
      {Data3, Type}  = read(Data2, byte),
      {Data4, SeqId} = read(Data3, i32),
      {Data4, #protocol_message_begin{name = Name, type = Type, seqid = SeqId}};
    _ ->
      %% Unexpected version number.
      error({bad_binary_protocol_version, Initial})
  end;

?READ (Data, message_end) ->
  {Data, ok};

?READ (Data, struct_begin) ->
  {Data, ok};

?READ (Data, struct_end) ->
  {Data, ok};

?READ (Data0, field_begin) ->
  case Data0 of
    <<?tType_STOP:8/big-signed, Data1/binary>>  ->
      {Data1, #protocol_field_begin{type = field_stop}};
    <<Type:8/big-signed, Id:16/big-signed, Data1/binary>> ->
      {Data1, #protocol_field_begin{type = wire_to_term(Type), id = Id}}
  end;

?READ (Data, field_end) ->
  {Data, ok};

%% This isn't necessary, since we never explicitly read a `field_stop', we
%% just find it when trying to read a `field_begin'.
%%
%% ?READ (field_stop, Data0) ->
%%   {?tType_STOP, Data1} = read(?tType_BYTE, Data0),
%%   {ok, Data1};

?READ (Data0, map_begin) ->
  <<KType:8/big-signed, VType:8/big-signed, Size:32/big-signed, Data1/binary>> = Data0,
  {Data1, #protocol_map_begin{ktype = wire_to_term(KType), vtype = wire_to_term(VType), size = Size}};

?READ (Data, map_end) ->
  {Data, ok};

?READ (Data0, list_begin) ->
  <<EType:8/big-signed, Size:32/big-signed, Data1/binary>> = Data0,
  {Data1, #protocol_list_begin{etype = wire_to_term(EType), size = Size}};

?READ (Data, list_end) ->
  {Data, ok};

?READ (Data0, set_begin) ->
  <<EType:8/big-signed, Size:32/big-signed, Data1/binary>> = Data0,
  {Data1, #protocol_set_begin{etype = wire_to_term(EType), size = Size}};

?READ (Data, set_end) ->
  {Data, ok};

?READ (Data0, bool) ->
  <<Bool:8/big-signed, Data1/binary>> = Data0,
  {Data1, Bool =/= 0};

?READ (Data0, byte) ->
  <<Val:8/integer-signed-big, Data1/binary>> = Data0,
  {Data1, Val};

?READ (Data0, i16) ->
  <<Val:16/integer-signed-big, Data1/binary>> = Data0,
  {Data1, Val};

?READ (Data0, i32) ->
  <<Val:32/integer-signed-big, Data1/binary>> = Data0,
  {Data1, Val};

?READ (Data0, i64) ->
  <<Val:64/integer-signed-big, Data1/binary>> = Data0,
  {Data1, Val};

?READ (Data0, double) ->
  <<Val:64/float-signed-big, Data1/binary>> = Data0,
  {Data1, Val};

?READ (Data0, string) ->
  <<Size:32/big-signed, String:Size/binary, Data1/binary>> = Data0,
  {Data1, String};

?READ (Data0, ui32) ->
  %% Used for reading the message version header.
  <<Val:32/integer-unsigned-big, Data1/binary>> = Data0,
  {Data1, Val}.


-spec read_data(DataIn::binary(), Size::non_neg_integer()) -> {DataOut::binary(), Value::binary()}.
read_data (Data0, Size) ->
  case Size of
    0 -> {Data0, <<>>};
    _ -> <<Val:Size/binary, Data1/binary>> = Data0,
         {Data1, Val}
  end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-ifdef(EUNIT).

message_test () ->
  P = #protocol_message_begin{name = <<"test">>, type = ?tMessageType_CALL, seqid = 16#7FFFFFF0},
  ?assertEqual({<<>>, P}, read(iolist_to_binary(write(P)), message_begin)),

  %% Old-style message header.
  ?assertEqual({<<>>, P}, read(<<4:32/big, "test", ?tMessageType_CALL, 16#7FFFFFF0:32/big>>, message_begin)).

field_test () ->
  P = #protocol_field_begin{type = i32, id = 16#7FF0},
  ?assertEqual({<<>>, P}, read(iolist_to_binary(write(P)), field_begin)),

  ?assertEqual({<<>>, ok}, read(iolist_to_binary(write(field_end)), field_end)).

map_test () ->
  P = #protocol_map_begin{ktype = byte, vtype = string, size = 16#7FFFFFF1},
  ?assertEqual({<<>>, P}, read(iolist_to_binary(write(P)), map_begin)),

  ?assertEqual({<<>>, ok}, read(iolist_to_binary(write(map_end)), map_end)).

list_test () ->
  P = #protocol_list_begin{etype = byte, size = 16#7FFFFFF2},
  ?assertEqual({<<>>, P}, read(iolist_to_binary(write(P)), list_begin)),

  ?assertEqual({<<>>, ok}, read(iolist_to_binary(write(list_end)), list_end)).

set_test () ->
  P = #protocol_set_begin{etype = byte, size = 16#7FFFFFF3},
  ?assertEqual({<<>>, P}, read(iolist_to_binary(write(P)), set_begin)),

  ?assertEqual({<<>>, ok}, read(iolist_to_binary(write(set_end)), set_end)).

basic_test () ->
  B = 16#7F,
  ?assertEqual({<<>>, B}, read(iolist_to_binary(write(byte, B)), byte)),

  S = 16#7FFF,
  ?assertEqual({<<>>, S}, read(iolist_to_binary(write(i16, S)), i16)),

  I = 16#7FFFFFFF,
  ?assertEqual({<<>>, I}, read(iolist_to_binary(write(i32, I)), i32)),

  F = 1234.25,
  ?assertEqual({<<>>, F}, read(iolist_to_binary(write(double, F)), double)),

  SB = <<"hello, world">>,
  ?assertEqual({<<>>, SB}, read(iolist_to_binary(write(string, SB)), string)),

  SL = "hello, world",
  ?assertEqual({<<>>, SB}, read(iolist_to_binary(write(string, SL)), string)).

-endif. %% EUNIT
