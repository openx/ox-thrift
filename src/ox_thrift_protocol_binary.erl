-module(ox_thrift_protocol_binary).

-include("ox_thrift_internal.hrl").

-export([ encode_call/4, encode_message/5, decode_message/2 ]).

-include("ox_thrift_protocol.hrl").

-compile({inline, [ write_message_begin/3
                  , write_message_end/0
                  , write_field_begin/3
                  , write_field_stop/0
                  , write_field_end/0
                  , write_map_begin/3
                  , write_map_end/0
                  , write_list_begin/2
                  , write_list_end/0
                  , write_set_begin/2
                  , write_set_end/0
                  , write_struct_begin/1
                  , write_struct_end/0
                  , write/2
                  , read/2 ]}).
%% -compile(inline_list_funcs).

%% term_to_wire(field_stop)        -> ?tType_STOP;
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

%% wire_to_term(?tType_STOP)       -> field_stop;
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

-define(VERSION_0, <<0, 0>>).
-define(VERSION_1, <<16#80, 16#01>>).

write_message_begin (Name, Type, SeqId) ->
  %% Write a version 1 message header.
  NameLen = size(Name),
  <<?VERSION_1/binary, 0, Type, NameLen:32/big-signed, Name/binary, SeqId:32/big-signed>>.

write_message_end () ->
  [].

write_field_begin (_Name, Type, Id) ->
  TypeWire = term_to_wire(Type),
  <<TypeWire:8/big-signed, Id:16/big-signed>>.

write_field_stop () ->
  <<?tType_STOP:8/big-signed>>.

write_field_end () ->
  [].

write_map_begin (KType, VType, Size) ->
  KTypeWire = term_to_wire(KType),
  VTypeWire = term_to_wire(VType),
  <<KTypeWire:8/big-signed, VTypeWire:8/big-signed, Size:32/big-signed>>.

write_map_end () ->
  [].

write_list_begin (EType, Size) ->
  ETypeWire = term_to_wire(EType),
  <<ETypeWire:8/big-signed, Size:32/big-signed>>.

write_list_end () ->
  [].

write_set_begin (EType, Size) ->
  ETypeWire = term_to_wire(EType),
  <<ETypeWire:8/big-signed, Size:32/big-signed>>.

write_set_end () ->
  [].

write_struct_begin (_Name) ->
  [].

write_struct_end () ->
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
  Version = binary_part(Data0, {0, 2}),
  case Version of
    ?VERSION_1 ->
      <<_:3/binary, Type, NameSize:32/big-signed, Name:NameSize/binary, SeqId:32/big-signed, Data1/binary>> = Data0,
      {Data1, #protocol_message_begin{name = Name, type = Type, seqid = SeqId}};
    ?VERSION_0 ->
      %% No version header; read the old way.
      <<_:2/binary, NameSize:16/big-signed, Name:NameSize/binary, Type, SeqId:32/big-signed, Data1/binary>> = Data0,
      {Data1, #protocol_message_begin{name = Name, type = Type, seqid = SeqId}};
    _  ->
      %% Unexpected version number.
      error({bad_binary_protocol_version, Version})
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


%% -spec read_data(DataIn::binary(), Size::non_neg_integer()) -> {DataOut::binary(), Value::binary()}.
%% read_data (Data0, Size) ->
%%   case Size of
%%     0 -> {Data0, <<>>};
%%     _ -> <<Val:Size/binary, Data1/binary>> = Data0,
%%          {Data1, Val}
%%   end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-ifdef(EUNIT).

message_test () ->
  Name = <<"test">>,
  NameLen = size(Name),
  Type = ?tMessageType_CALL,
  SeqId = 16#7FFFFFF0,
  P = #protocol_message_begin{name = Name, type = Type, seqid = SeqId},
  ?assertEqual({<<>>, P}, read(iolist_to_binary(write_message_begin(Name, Type, SeqId)), message_begin)),

  %% New-style message header.
  ?assertEqual({<<>>, P}, read(<<?VERSION_1/binary, 0, Type, NameLen:32/big, Name/binary, SeqId:32/big>>, message_begin)),

  %% Old-style message header.
  ?assertEqual({<<>>, P}, read(<<NameLen:32/big, Name/binary, Type, SeqId:32/big>>, message_begin)).

field_test () ->
  Name = <<"field">>,
  Type = i32,
  Id = 16#7FF0,
  %% Name is not sent in binary protocol.
  P = #protocol_field_begin{name = undefined, type = Type, id = Id},
  ?assertEqual({<<>>, P}, read(iolist_to_binary(write_field_begin(Name, Type, Id)), field_begin)),

  ?assertEqual({<<>>, ok}, read(iolist_to_binary(write_field_end()), field_end)).

map_test () ->
  KType = byte,
  VType = string,
  Size = 16#7FFFFFF1,
  P = #protocol_map_begin{ktype = KType, vtype = VType, size = Size},
  ?assertEqual({<<>>, P}, read(iolist_to_binary(write_map_begin(KType, VType, Size)), map_begin)),

  ?assertEqual({<<>>, ok}, read(iolist_to_binary(write_map_end()), map_end)).

list_test () ->
  EType = byte,
  Size = 16#7FFFFFF2,
  P = #protocol_list_begin{etype = EType, size = Size},
  ?assertEqual({<<>>, P}, read(iolist_to_binary(write_list_begin(EType, Size)), list_begin)),

  ?assertEqual({<<>>, ok}, read(iolist_to_binary(write_list_end()), list_end)).

set_test () ->
  EType = byte,
  Size = 16#7FFFFFF3,
  P = #protocol_set_begin{etype = EType, size = Size},
  ?assertEqual({<<>>, P}, read(iolist_to_binary(write_set_begin(EType, Size)), set_begin)),

  ?assertEqual({<<>>, ok}, read(iolist_to_binary(write_set_end()), set_end)).

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
