-module(ox_thrift_protocol_binary).

-include("ox_thrift_internal.hrl").

-export([ encode_call/4, encode_message/5, decode_message/2 ]).

-include("ox_thrift_protocol.hrl").

-compile({inline, [ write_message_begin/3
                  , write_message_end/0
                  , write_field_begin/2
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
                  , read_message_begin/1
                  , read_message_end/1
                  , read_struct_begin/1
                  , read_struct_end/1
                  , read_field_begin/1
                  , read_field_end/1
                  , read_map_begin/1
                  , read_map_end/1
                  , read_list_begin/1
                  , read_list_end/1
                  , read_set_begin/1
                  , read_set_end/1
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

write_field_begin (Type, Id) ->
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


-spec read_message_begin(IData::binary()) -> {OData::binary(), Name::binary(), Type::integer(), SeqId::integer()}.
read_message_begin (Data0) ->
  Version = binary_part(Data0, {0, 2}),
  case Version of
    ?VERSION_1 ->
      <<_:3/binary, Type, NameSize:32/big-signed, Name:NameSize/binary, SeqId:32/big-signed, Data1/binary>> = Data0,
      {Data1, Name, Type, SeqId};
    ?VERSION_0 ->
      %% No version header; read the old way.
      <<_:2/binary, NameSize:16/big-signed, Name:NameSize/binary, Type, SeqId:32/big-signed, Data1/binary>> = Data0,
      {Data1, Name, Type, SeqId};
    _  ->
      %% Unexpected version number.
      error({bad_binary_protocol_version, Version})
  end.

-spec read_message_end (IData::binary()) -> OData::binary().
read_message_end (Data) ->
  Data.

-spec read_struct_begin (IData::binary()) -> OData::binary().
read_struct_begin (Data) ->
  Data.

-spec read_struct_end (IData::binary()) -> OData::binary().
read_struct_end (Data) ->
  Data.

-spec read_field_begin (IData::binary()) -> {OData::binary(), Type::proto_type(), Id::integer()}
                                          | {OData::binary(), field_stop, 'undefined'}.
read_field_begin (Data0) ->
  case Data0 of
    <<?tType_STOP:8/big-signed, Data1/binary>>  ->
      {Data1, field_stop, undefined};
    <<Type:8/big-signed, Id:16/big-signed, Data1/binary>> ->
      {Data1, wire_to_term(Type), Id}
  end.

-spec read_field_end (IData::binary()) -> OData::binary().
read_field_end (Data) ->
  Data.

%% This isn't necessary, since we never explicitly read a `field_stop', we
%% just find it when trying to read a `field_begin'.
%%
%% ?READ (field_stop, Data0) ->
%%   {?tType_STOP, Data1} = read(?tType_BYTE, Data0),
%%   {ok, Data1};

-spec read_map_begin(IData::binary()) -> {OData::binary(), KType::proto_type(), VType::proto_type(), Size::integer()}.
read_map_begin (Data0) ->
  <<KType:8/big-signed, VType:8/big-signed, Size:32/big-signed, Data1/binary>> = Data0,
  {Data1, wire_to_term(KType), wire_to_term(VType), Size}.

-spec read_map_end (IData::binary()) -> OData::binary().
read_map_end (Data) ->
  Data.

-spec read_list_begin(IData::binary()) -> {OData::binary(), EType::proto_type(), Size::integer()}.
read_list_begin (Data0) ->
  <<EType:8/big-signed, Size:32/big-signed, Data1/binary>> = Data0,
  {Data1, wire_to_term(EType), Size}.

-spec read_list_end (IData::binary()) -> OData::binary().
read_list_end (Data) ->
  Data.

-spec read_set_begin(IData::binary()) -> {OData::binary(), EType::proto_type(), Size::integer()}.
read_set_begin (Data0) ->
  <<EType:8/big-signed, Size:32/big-signed, Data1/binary>> = Data0,
  {Data1, wire_to_term(EType), Size}.

-spec read_set_end (IData::binary()) -> OData::binary().
read_set_end (Data) ->
  Data.


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
  {Data1, String}.


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
  ?assertEqual({<<>>, Name, Type, SeqId},
               read_message_begin(iolist_to_binary(write_message_begin(Name, Type, SeqId)))),

  %% New-style message header.
  ?assertEqual({<<>>, Name, Type, SeqId},
               read_message_begin(<<?VERSION_1/binary, 0, Type, NameLen:32/big, Name/binary, SeqId:32/big>>)),

  %% Old-style message header.
  ?assertEqual({<<>>, Name, Type, SeqId},
               read_message_begin(<<NameLen:32/big, Name/binary, Type, SeqId:32/big>>)).

field_test () ->
  Type = i32,
  Id = 16#7FF0,
  %% Name is not sent in binary protocol.
  ?assertMatch({<<>>, Type, Id}, read_field_begin(iolist_to_binary(write_field_begin(Type, Id)))),

  ?assertEqual(<<>>, read_field_end(iolist_to_binary(write_field_end()))).

map_test () ->
  KType = byte,
  VType = string,
  Size = 16#7FFFFFF1,
  ?assertEqual({<<>>, KType, VType, Size}, read_map_begin(iolist_to_binary(write_map_begin(KType, VType, Size)))),

  ?assertEqual(<<>>, read_map_end(iolist_to_binary(write_map_end()))).

list_test () ->
  EType = byte,
  Size = 16#7FFFFFF2,
  ?assertEqual({<<>>, EType, Size}, read_list_begin(iolist_to_binary(write_list_begin(EType, Size)))),

  ?assertEqual(<<>>, read_list_end(iolist_to_binary(write_list_end()))).

set_test () ->
  EType = byte,
  Size = 16#7FFFFFF3,
  ?assertEqual({<<>>, EType, Size}, read_set_begin(iolist_to_binary(write_set_begin(EType, Size)))),

  ?assertEqual(<<>>, read_set_end(iolist_to_binary(write_set_end()))).

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
