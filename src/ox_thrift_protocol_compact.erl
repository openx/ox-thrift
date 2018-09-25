%% Copyright 2016-2018, OpenX.  All rights reserved.
%% Licensed under the conditions specified in the accompanying LICENSE file.

-module(ox_thrift_protocol_compact).

%% For a description of the compact protocol, see
%% https://erikvanoosten.github.io/thrift-missing-specification/.

%% For a description of the zigzag and base 128 integer encoding for protobuf,
%% see https://developers.google.com/protocol-buffers/docs/encoding.

-include("ox_thrift_internal.hrl").

-export([ encode_call/4, encode_message/5, decode_message/3 ]).

-export([ encode_record/2, decode_record/2, decode_record/3 ]).

-define(THRIFT_PROTOCOL, compact).
-include("ox_thrift_protocol.hrl").

-compile({inline, [ write_message_begin/3
                  , write_field_begin/4
                  , write_field_stop/0
                  , write_map_begin/3
                  , write_list_or_set_begin/2
                  , write/2
                  , write_varint/1
                  , read_message_begin/1
                  , read_field_begin/2
                  , read_map_begin/1
                  , read_list_or_set_begin/1
                  , read/2
                  , read_varint/1
                  , encode_zigzag/1
                  , decode_zigzag/1
                  ]}).
-compile(inline_list_funcs).

-define(TYPE_STRUCT_FIELD_STOP, 0).
-define(TYPE_STRUCT_FIELD_TRUE, 1).
-define(TYPE_STRUCT_FIELD_FALSE, 2).
-define(TYPE_STRUCT_FIELD_BYTE, 3).
-define(TYPE_STRUCT_FIELD_I16, 4).
-define(TYPE_STRUCT_FIELD_I32, 5).
-define(TYPE_STRUCT_FIELD_I64, 6).
-define(TYPE_STRUCT_FIELD_DOUBLE, 7).
-define(TYPE_STRUCT_FIELD_STRING, 8).
-define(TYPE_STRUCT_FIELD_LIST, 9).
-define(TYPE_STRUCT_FIELD_SET, 10).
-define(TYPE_STRUCT_FIELD_MAP, 11).
-define(TYPE_STRUCT_FIELD_STRUCT, 12).

term_to_wire_struct(bool)       -> ?TYPE_STRUCT_FIELD_TRUE; % list/set/map
term_to_wire_struct(true)       -> ?TYPE_STRUCT_FIELD_TRUE; % struct field
term_to_wire_struct(false)      -> ?TYPE_STRUCT_FIELD_FALSE;
term_to_wire_struct(byte)       -> ?TYPE_STRUCT_FIELD_BYTE;
term_to_wire_struct(double)     -> ?TYPE_STRUCT_FIELD_DOUBLE;
term_to_wire_struct(i16)        -> ?TYPE_STRUCT_FIELD_I16;
term_to_wire_struct(i32)        -> ?TYPE_STRUCT_FIELD_I32;
term_to_wire_struct(i64)        -> ?TYPE_STRUCT_FIELD_I64;
term_to_wire_struct(string)     -> ?TYPE_STRUCT_FIELD_STRING;
term_to_wire_struct(struct)     -> ?TYPE_STRUCT_FIELD_STRUCT;
term_to_wire_struct(map)        -> ?TYPE_STRUCT_FIELD_MAP;
term_to_wire_struct(set)        -> ?TYPE_STRUCT_FIELD_SET;
term_to_wire_struct(list)       -> ?TYPE_STRUCT_FIELD_LIST.

wire_to_term_struct(?TYPE_STRUCT_FIELD_TRUE)    -> true;
wire_to_term_struct(?TYPE_STRUCT_FIELD_FALSE)   -> false;
wire_to_term_struct(?TYPE_STRUCT_FIELD_BYTE)    -> byte;
wire_to_term_struct(?TYPE_STRUCT_FIELD_DOUBLE)  -> double;
wire_to_term_struct(?TYPE_STRUCT_FIELD_I16)     -> i16;
wire_to_term_struct(?TYPE_STRUCT_FIELD_I32)     -> i32;
wire_to_term_struct(?TYPE_STRUCT_FIELD_I64)     -> i64;
wire_to_term_struct(?TYPE_STRUCT_FIELD_STRING)  -> string;
wire_to_term_struct(?TYPE_STRUCT_FIELD_STRUCT)  -> struct;
wire_to_term_struct(?TYPE_STRUCT_FIELD_MAP)     -> map;
wire_to_term_struct(?TYPE_STRUCT_FIELD_SET)     -> set;
wire_to_term_struct(?TYPE_STRUCT_FIELD_LIST)    -> list.

wire_to_term_lsm(?TYPE_STRUCT_FIELD_TRUE)    -> bool;
wire_to_term_lsm(?TYPE_STRUCT_FIELD_BYTE)    -> byte;
wire_to_term_lsm(?TYPE_STRUCT_FIELD_DOUBLE)  -> double;
wire_to_term_lsm(?TYPE_STRUCT_FIELD_I16)     -> i16;
wire_to_term_lsm(?TYPE_STRUCT_FIELD_I32)     -> i32;
wire_to_term_lsm(?TYPE_STRUCT_FIELD_I64)     -> i64;
wire_to_term_lsm(?TYPE_STRUCT_FIELD_STRING)  -> string;
wire_to_term_lsm(?TYPE_STRUCT_FIELD_STRUCT)  -> struct;
wire_to_term_lsm(?TYPE_STRUCT_FIELD_MAP)     -> map;
wire_to_term_lsm(?TYPE_STRUCT_FIELD_SET)     -> set;
wire_to_term_lsm(?TYPE_STRUCT_FIELD_LIST)    -> list.

%% -define(DEBUG_READ, true).

%% There is an asymmetry to the `write/1' and `read/2' interfaces.  `write/1'
%% returns an iolist for the converted data, which is expected to be
%% integrated into a larger iolist for the entire message, while `read/2'
%% parses the data from a binary buffer and returns the parsed data and the
%% remaining buffer.

-define(COMPACT_BINARY_HEADER, 16#82).
-define(COMPACT_BINARY_VERSION, 1).

write_message_begin (Name, Type, SeqId) ->
  %% Write a version 1 message header.
  TypeAndVersion = Type * 32 + ?COMPACT_BINARY_VERSION,
  [ ?COMPACT_BINARY_HEADER, TypeAndVersion, write_varint(SeqId), write(string, Name) ].

write_field_begin (Type, Id, LastId, Data) ->
  DeltaId = Id - LastId,
  if DeltaId > 0 andalso DeltaId < 16 ->
      case Type of
        bool -> case Data of
                  true  -> DeltaId * 16 + ?TYPE_STRUCT_FIELD_TRUE;
                  false -> DeltaId * 16 + ?TYPE_STRUCT_FIELD_FALSE
                end;
        byte            -> [ DeltaId * 16 + ?TYPE_STRUCT_FIELD_BYTE, Data ];
        _               -> {DeltaId * 16 + term_to_wire_struct(Type)}
      end;
     true ->
      case Type of
        bool -> case Data of
                  true  -> [ ?TYPE_STRUCT_FIELD_TRUE | write_varint(encode_zigzag(Id)) ];
                  false -> [ ?TYPE_STRUCT_FIELD_FALSE | write_varint(encode_zigzag(Id)) ]
                end;
        byte            -> [ ?TYPE_STRUCT_FIELD_BYTE, write_varint(encode_zigzag(Id)), Data ];
        _               -> {[ term_to_wire_struct(Type) | write_varint(encode_zigzag(Id)) ]}
      end
  end.

write_field_stop () ->
  ?TYPE_STRUCT_FIELD_STOP.

write_map_begin (KType, VType, Size) ->
  case Size of
    0 -> [ 0 ];
    _ -> KTypeWire = term_to_wire_struct(KType),
         VTypeWire = term_to_wire_struct(VType),
         [ write_varint(Size), KTypeWire * 16 + VTypeWire ]
  end.

write_list_or_set_begin (EType, Size) ->
  ETypeWire = term_to_wire_struct(EType),
  if Size < 15 -> [ Size * 16 + ETypeWire ];
     true      -> [ 16#F0 + ETypeWire | write_varint(Size) ]
  end.


write (bool, true) ->
  2;

write (bool, false) ->
  0;

write (byte, Byte) ->
  <<Byte:8/signed>>;

write (i16, I16) ->
  write_varint(encode_zigzag(I16));

write (i32, I32) ->
  write_varint(encode_zigzag(I32));

write (i64, I64) ->
  write_varint(encode_zigzag(I64));

write (double, Double) ->
  <<Double:64/float-signed-little>>;

write (string, Str) when is_list(Str) ->
  Bin = list_to_binary(Str),
  BinLen = size(Bin),
  [ write_varint(BinLen), Bin ];

write (string, Bin) when is_binary(Bin) ->
  BinLen = size(Bin),
  [ write_varint(BinLen), Bin ].


write_varint (UnsignedInt) when UnsignedInt < 128 ->
  [ UnsignedInt ];
write_varint (UnsignedInt) ->
  [ 128 + UnsignedInt rem 128 | write_varint(UnsignedInt div 128) ].


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
  case Data0 of
    <<?COMPACT_BINARY_HEADER, Type:3, ?COMPACT_BINARY_VERSION:5, Data1/binary>> ->
      {Data2, SeqId} = read_varint(Data1),
      {Data3, Name} = read(Data2, string),
      {Data3, Name, Type, SeqId};
    _  ->
      %% Unexpected header or version number.
      error({bad_header, binary_part(Data0, {0, 2})})
  end.

-spec read_field_begin (IData::binary(), LastId::integer())
                       -> {OData::binary(), field_stop}
                        | {OData::binary(), bool, Id::integer(), Val::boolean()}
                        | {OData::binary(), byte, Id::integer(), Val::integer()}
                        | {OData::binary(), i16 | i32 | i64 | double | struct | map | set | list, Id::integer()}.
read_field_begin (Data0, LastId) ->
  %% This function decodes and returns the value when it's "easy" (bool,
  %% byte), and leaves it to the caller when it's hard.
  case Data0 of
    <<?TYPE_STRUCT_FIELD_STOP:8, Data1/binary>> ->
      {Data1, field_stop};
    %% long-form header fields (DeltaId is 0, Id is encoded after type field):
    <<0:4, ?TYPE_STRUCT_FIELD_TRUE:4, Data1/binary>> ->
      {Data2, IdZ} = read_varint(Data1),
      {Data2, bool, decode_zigzag(IdZ), true};
    <<0:4, ?TYPE_STRUCT_FIELD_FALSE:4, Data1/binary>> ->
      {Data2, IdZ} = read_varint(Data1),
      {Data2, bool, decode_zigzag(IdZ), true};
    <<0:4, ?TYPE_STRUCT_FIELD_BYTE:4, Data1/binary>> ->
      {Data2, IdZ} = read_varint(Data1),
      <<Val:8/signed, Data3/binary>> = Data2,
      {Data3, byte, decode_zigzag(IdZ), Val};
    <<0:4, Type:4, Data1/binary>> ->
      {Data2, IdZ} = read_varint(Data1),
      {Data2, wire_to_term_struct(Type), decode_zigzag(IdZ)};
    %% short-form header fields:
    <<DeltaId:4, ?TYPE_STRUCT_FIELD_TRUE:4, Data1/binary>> ->
      {Data1, bool, DeltaId + LastId, true};
    <<DeltaId:4, ?TYPE_STRUCT_FIELD_FALSE:4, Data1/binary>> ->
      {Data1, bool, DeltaId + LastId, false};
    <<DeltaId:4, ?TYPE_STRUCT_FIELD_BYTE:4, Val:8/signed, Data1/binary>> ->
      {Data1, byte, DeltaId + LastId, Val};
    <<DeltaId:4, Type:4, Data1/binary>> ->
      {Data1, wire_to_term_struct(Type), DeltaId + LastId}
  end.

%% This isn't necessary, since we never explicitly read a `field_stop', we
%% just find it when trying to read a `field_begin'.
%%
%% ?READ (field_stop, Data0) ->
%%   {?tType_STOP, Data1} = read(?tType_BYTE, Data0),
%%   {ok, Data1};

-spec read_map_begin(IData::binary())
                    -> {OData::binary(), 'undefined', 'undefined', 0}
                     | {OData::binary(), KType::proto_type(), VType::proto_type(), Size::pos_integer()}.
read_map_begin (Data0) ->
  {Data1, Size} = read_varint(Data0),
  if Size =:= 0 -> {Data1, undefined, undefined, Size};
     true       -> <<KType:4, VType:4, Data2/binary>> = Data1,
                   {Data2, wire_to_term_lsm(KType), wire_to_term_lsm(VType), Size}
  end.

-spec read_list_or_set_begin(IData::binary()) -> {OData::binary(), EType::proto_type(), Size::integer()}.
read_list_or_set_begin (Data0) ->
  <<CompactSize:4, EType:4, Data1/binary>> = Data0,
  case CompactSize of
    16#F -> {Data2, Size} = read_varint(Data1),
            {Data2, wire_to_term_lsm(EType), Size};
    _    -> {Data1, wire_to_term_lsm(EType), CompactSize}
  end.


?READ (Data0, bool) ->
  <<Bool:8, Data1/binary>> = Data0,
  {Data1, Bool =/= 0};

?READ (Data0, byte) ->
  <<Val:8/signed, Data1/binary>> = Data0,
  {Data1, Val};

?READ (Data0, i16) ->
  {Data1, Z} = read_varint(Data0),
  {Data1, decode_zigzag(Z)};

?READ (Data0, i32) ->
  {Data1, Z} = read_varint(Data0),
  {Data1, decode_zigzag(Z)};

?READ (Data0, i64) ->
  {Data1, Z} = read_varint(Data0),
  {Data1, decode_zigzag(Z)};

?READ (Data0, double) ->
  <<Val:64/float-signed-little, Data1/binary>> = Data0,
  {Data1, Val};

?READ (Data0, string) ->
  {Data1, Size} = read_varint(Data0),
  <<String:Size/binary, Data2/binary>> = Data1,
  {Data2, String}.


read_varint (Data) -> read_varint(Data, 0, 1).

read_varint (Data0, Acc, Multiplier) ->
  <<Val:8/integer-unsigned, Data1/binary>> = Data0,
  if Val < 128 -> {Data1, Val * Multiplier + Acc};
     true      -> read_varint(Data1, (Val - 128) * Multiplier + Acc, Multiplier * 128)
  end.


%% -spec read_data(DataIn::binary(), Size::non_neg_integer()) -> {DataOut::binary(), Value::binary()}.
%% read_data (Data0, Size) ->
%%   case Size of
%%     0 -> {Data0, <<>>};
%%     _ -> <<Val:Size/binary, Data1/binary>> = Data0,
%%          {Data1, Val}
%%   end.


encode_zigzag (SignedInt) ->
  if SignedInt >= 0 -> SignedInt * 2;
     true           -> -SignedInt * 2 - 1
    end.


decode_zigzag (ZigZagInt) ->
  if ZigZagInt rem 2 =:= 1 -> - (ZigZagInt + 1) div 2;
     true                  -> ZigZagInt div 2
  end.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-ifdef(EUNIT).

write_varint_test () ->
  ?assertEqual(<<0>>, iolist_to_binary(write_varint(0))),
  ?assertEqual(<<1>>, iolist_to_binary(write_varint(1))),
  ?assertEqual(<<127>>, iolist_to_binary(write_varint(127))),
  ?assertEqual(<<128, 1>>, iolist_to_binary(write_varint(128))),
  ?assertEqual(<<185, 96>>, iolist_to_binary(write_varint(12345))),
  ?assertEqual(<<135, 173, 75>>, iolist_to_binary(write_varint(1234567))),
  ?assertEqual(<<255, 127>>, iolist_to_binary(write_varint(16383))),
  ?assertEqual(<<128, 128, 1>>, iolist_to_binary(write_varint(16384))),
  ?assertEqual(<<255, 255, 127>>, iolist_to_binary(write_varint(2097151))),
  ?assertEqual(<<255, 255, 255, 127>>, iolist_to_binary(write_varint(268435455))).

read_varint_test () ->
  ?assertEqual({<<>>, 0}, read_varint(<<0>>)),
  ?assertEqual({<<>>, 1}, read_varint(<<1>>)),
  ?assertEqual({<<>>, 127}, read_varint(<<127>>)),
  ?assertEqual({<<>>, 128}, read_varint(<<128, 1>>)).


varint_roundtrip_test () ->
  lists:map(
    fun (I) -> ?assertMatch({{<<>>, I}, _, I},
                            begin
                              B = iolist_to_binary(write_varint(I)),
                              {read_varint(B), B, I}
                            end) end,
    [ 0, 1, 9, 10, 99, 100, 9999, 16#10001000, 16#FFFFFFFF, 16#FEDCBA9876543210 ]).


encode_zigzag_test () ->
  ?assertEqual(0, encode_zigzag(0)),
  ?assertEqual(2, encode_zigzag(1)),
  ?assertEqual(1, encode_zigzag(-1)),
  ?assertEqual(254, encode_zigzag(127)),
  ?assertEqual(255, encode_zigzag(-128)),
  ?assertEqual(4294967294, encode_zigzag(2147483647)),
  ?assertEqual(4294967295, encode_zigzag(-2147483648)).

zigzag_roundtrip_test () ->
  lists:map(
    fun (I) -> ?assertMatch({I, _, I},
                            begin
                              Z = encode_zigzag(I),
                              {decode_zigzag(Z), Z, I}
                            end) end,
    [ 0, 1, 2, 3, -1, -2, -3, 100, -100, 999, -999, 16#7FF0, 99999999, -99999999 ]).


message_test () ->
  Name = <<"test">>,
  NameLen = size(Name),
  Type = ?tMessageType_CALL,
  SeqId = 16#7FFFFFF0,
  ?assertEqual({<<>>, Name, Type, SeqId},
               read_message_begin(iolist_to_binary(write_message_begin(Name, Type, SeqId)))),

  SeqIdEnc = iolist_to_binary(write_varint(SeqId)),
  ?assertEqual({<<>>, Name, Type, SeqId},
               read_message_begin(<<?COMPACT_BINARY_HEADER, Type:3, ?COMPACT_BINARY_VERSION:5, SeqIdEnc/binary, NameLen, Name/binary>>)).

field_test () ->
  ?assertEqual(<<17>>, iolist_to_binary([ write_field_begin(bool, 1, 0, true) ])),
  ?assertEqual(<<18>>, iolist_to_binary([ write_field_begin(bool, 1, 0, false) ])),
  ?assertEqual(<<1, 2>>, iolist_to_binary([ write_field_begin(bool, 1, 99, true) ])),
  ?assertEqual(<<2, 2>>, iolist_to_binary([ write_field_begin(bool, 1, 99, false) ])),
  ?assertEqual(<<19, 123>>, iolist_to_binary(write_field_begin(byte, 1, 0, 123))),
  ?assertEqual(<<3, 2, 123>>, iolist_to_binary(write_field_begin(byte, 1, 99, 123))),
  ?assertEqual(<<16#25>>, iolist_to_binary([ element(1, write_field_begin(i32, 32, 30, not_used)) ])),
  ?assertEqual(<<16#05, 64>>, iolist_to_binary(element(1, write_field_begin(i32, 32, 0, not_used)))),

  %% Type = i32,
  %% Id = 16#7FF0,
  %% Name is not sent in binary protocol.
  %% ?assertEqual({<<>>, Type, Id}, read_field_begin(iolist_to_binary(write_field_begin(Type, Id, 0)), 0)),
  %% ?assertEqual({<<>>, Type, Id}, read_field_begin(iolist_to_binary(write_field_begin(Type, Id, Id - 5)), Id - 5)),
  ok.

map_test () ->
  KType = byte,
  VType = string,
  Size0 = 16#7FFFFFF1, %% Normal map header.
  ?assertEqual({<<>>, KType, VType, Size0}, read_map_begin(iolist_to_binary(write_map_begin(KType, VType, Size0)))),
  Size1 = 0, %% Empty map header.
  ?assertEqual({<<>>, undefined, undefined, Size1}, read_map_begin(iolist_to_binary(write_map_begin(KType, VType, Size1)))).

list_or_set_test () ->
  EType = byte, %% Large set header.
  Size0 = 16#7FFFFFF2,
  ?assertEqual({<<>>, EType, Size0}, read_list_or_set_begin(iolist_to_binary(write_list_or_set_begin(EType, Size0)))),
  Size1 = 14, %% Compact set header.
  ?assertEqual({<<>>, EType, Size1}, read_list_or_set_begin(iolist_to_binary(write_list_or_set_begin(EType, Size1)))).

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
