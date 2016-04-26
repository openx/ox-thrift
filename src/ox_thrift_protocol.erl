-module(ox_thrift_protocol).

-include("ox_thrift_internal.hrl").

-export([ encode_message/6, encode/3, decode_message/3, decode/3 ]).


typeid_to_atom(?tType_STOP)     -> field_stop;
typeid_to_atom(?tType_VOID)     -> void;
typeid_to_atom(?tType_BOOL)     -> bool;
typeid_to_atom(?tType_BYTE)     -> byte;
typeid_to_atom(?tType_DOUBLE)   -> double;
typeid_to_atom(?tType_I16)      -> i16;
typeid_to_atom(?tType_I32)      -> i32;
typeid_to_atom(?tType_I64)      -> i64;
typeid_to_atom(?tType_STRING)   -> string;
typeid_to_atom(?tType_STRUCT)   -> struct;
typeid_to_atom(?tType_MAP)      -> map;
typeid_to_atom(?tType_SET)      -> set;
typeid_to_atom(?tType_LIST)     -> list.

term_to_typeid(void)            -> ?tType_VOID;
term_to_typeid(bool)            -> ?tType_BOOL;
term_to_typeid(byte)            -> ?tType_BYTE;
term_to_typeid(double)          -> ?tType_DOUBLE;
term_to_typeid(i16)             -> ?tType_I16;
term_to_typeid(i32)             -> ?tType_I32;
term_to_typeid(i64)             -> ?tType_I64;
term_to_typeid(string)          -> ?tType_STRING;
term_to_typeid({struct, _})     -> ?tType_STRUCT;
term_to_typeid({map, _, _})     -> ?tType_MAP;
term_to_typeid({set, _})        -> ?tType_SET;
term_to_typeid({list, _})       -> ?tType_LIST.

-define(VALIDATE_TYPE(StructType, SuppliedType),
        case term_to_typeid(StructType) of
          SuppliedType -> ok;
          _ -> error(type_mismatch, [ {provided, SuppliedType}, {expected, StructType} ])
        end).


-spec encode_message(Codec::atom(), ServiceModule::atom(), Function::atom(), MessageType::integer(), SeqId::integer(), Args::list()) -> iolist().
%% `MessageType' is `?tMessageType_CALL', `?tMessageType_REPLY' or `?tMessageType_EXCEPTION'.  If a
%% normal reply, the `Args' argument is a variable of the expected return type
%% for `Function'.  If an exception reply, the `Args' argument is an exception record.
%%
%% `Args' is a list of function arguments for a `?tMessageType_CALL', and is
%% the reply for a `?tMessageType_REPLY' or exception record for
%% `?tMessageType_EXCEPTION'.
encode_message (Codec, ServiceModule, Function, MessageType, SeqId, Args) ->
  case MessageType of
    ?tMessageType_CALL ->
      MessageSpec = ServiceModule:function_info(Function, params_type),
      ArgsList = [ Function | Args ];
    ?tMessageType_REPLY ->
      %% Create a fake zero- or one-element structure for the result.
      ReplyName = atom_to_list(Function) ++ "_result",
      case ServiceModule:function_info(Function, reply_type) of
        oneway_void ->
          error(oneway_void), %% This shouldn't happen....
          MessageSpec = undefined,
          ArgsList = undefined;
        ReplySpec={struct, []} ->
          %% A void return
          MessageSpec = ReplySpec,
          ArgsList = [ ReplyName ];
        ReplySpec ->
          %% A non-void return.
          MessageSpec = {struct, [ {0, ReplySpec} ]},
          ArgsList = [ ReplyName | Args ]
          %% , io:format("encode\nspec ~p\nargs  ~p\n", [ MessageSpec, ArgsList ])
      end;
    ?tMessageType_EXCEPTION ->
      %% An exception is treated as a struct with a field for each possible
      %% exception.  Since any given call returns only one exception, all
      %% except one of the fields is `undefined' and so only the field for
      %% the actual exception is sent over the wire.
      [ Exception ] = Args,
      ExceptionName = element(1, Exception),
      MessageSpec = {struct, ExceptionsSpec} = ServiceModule:function_info(Function, exceptions),
      {ExceptionList, ExceptionFound} =
        lists:mapfoldl(
          fun ({_, {struct, {_, StructExceptionName}}}, {ArgsAcc, FoundAcc}) ->
              case StructExceptionName of
                ExceptionName -> {[ Exception | ArgsAcc ], true};
                _             -> {[ undefined | ArgsAcc], FoundAcc}
              end
          end, {[], false}, ExceptionsSpec),
      ExceptionFound orelse
        error({error_not_declared_as_thrown, Function, ExceptionName}),
      ArgsList = [ Function | ExceptionList ]
    end,

  [ Codec:write(#protocol_message_begin{name = atom_to_binary(Function, latin1), type = MessageType, seqid = SeqId})
    %% Thrift supports only lists of uniform types, and so it uses a
    %% function-specific struct for a function's argument list.
  , encode(Codec, MessageSpec, list_to_tuple(ArgsList))
  , Codec:write(message_end)
  ].


encode (Codec, {struct, StructDef}, Data)
  when is_list(StructDef), is_tuple(Data), length(StructDef) == size(Data) - 1 ->
  %% Encode a record from a struct definition.
  [ Codec:write(#protocol_struct_begin{name = element(1, Data)})
  , encode_struct(Codec, StructDef, Data, 2)
  , Codec:write(struct_end)
  ];

encode (Codec, {struct, {Schema, StructName}}, Data)
  when is_atom(Schema), is_atom(StructName), is_tuple(Data), element(1, Data) == StructName ->
  %% Encode a record from a schema module.
  encode(Codec, Schema:struct_info(StructName), Data);

encode (Codec, S={struct, {_Schema, _StructName}}, Data) ->
  error(struct_unmatched, [ Codec, S, Data ]);

encode (Codec, {list, Type}, Data)
  when is_list(Data) ->
  %% Encode a list.
  EltTId = term_to_typeid(Type),
  [ Codec:write(#protocol_list_begin{
                   etype = EltTId,
                   size = length(Data)})
  , lists:map(fun (Elt) -> encode(Codec, Type, Elt) end, Data)
  , Codec:write(list_end)
  ];

encode (Codec, {map, KeyType, ValType}, Data) ->
  %% Encode a map.
  KeyTId = term_to_typeid(KeyType),
  ValTId = term_to_typeid(ValType),
  [ Codec:write(#protocol_map_begin{
                   ktype = KeyTId,
                   vtype = ValTId,
                   size = dict:size(Data)})
  , dict:fold(fun (Key, Val, Acc) ->
                  [ encode(Codec, KeyType, Key)
                  , encode(Codec, ValType, Val)
                  | Acc
                  ]
              end, [], Data)
  , Codec:write(map_end)
  ];

encode (Codec, {set, Type}, Data) ->
  %% Encode a set.
  EltType = term_to_typeid(Type),
  [ Codec:write(#protocol_set_begin{
                   etype = EltType,
                   size = sets:size(Data)})
  , sets:fold(fun (Elt, Acc) -> [ encode(Codec, Type, Elt) | Acc ] end, [], Data)
  , Codec:write(set_end)
  ];

encode (Codec, Type, Data) when is_atom(Type) ->
  %% Encode the basic types.
  TypeId = term_to_typeid(Type),
  Codec:write({TypeId, Data});

encode (_Codec, Type, Data) ->
  error({invalid_type, {type, Type}, {data, Data}}).


-spec encode_struct(Codec::atom(), FieldData::list({integer(), atom()}), Record::tuple(), I::integer()) -> IOData::iodata().
encode_struct (Codec, [ {FieldId, Type} | FieldRest ], Record, I) ->
  %% We could use tail recursion to make this a little more efficient, because
  %% the field order should matter. @@
  case element(I, Record) of
    undefined ->
      %% null fields are skipped
      encode_struct(Codec, FieldRest, Record, I+1);
    Data ->
      FieldTypeId = term_to_typeid(Type),
      [ Codec:write(#protocol_field_begin{
                       type = FieldTypeId,
                       id = FieldId})
      , encode(Codec, Type, Data)
      , Codec:write(field_end)
      | encode_struct(Codec, FieldRest, Record, I+1)
      ]
  end;

encode_struct (Codec, [], _Record, _I) ->
  Codec:write(field_stop).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec decode_message(Codec::atom(), ServiceModule::atom(), Buffer::binary()) ->
                        {Function::atom(), MessageType::integer(), Seqid::integer(), Args::list()}.
%% `MessageType' is `?tMessageType_CALL' or `?tMessageType_ONEWAY'.
decode_message (Codec, ServiceModule, Buffer0) ->
  {#protocol_message_begin{name=FunctionBin, type=MessageType, seqid=SeqId}, Buffer1} =
    Codec:read(message_begin, Buffer0),
  Function = binary_to_atom(FunctionBin, latin1),
  {ArgsTuple, Buffer2} =
    case MessageType of
      Call when Call =:= ?tMessageType_CALL orelse Call =:= ?tMessageType_ONEWAY ->
        MessageSpec = ServiceModule:function_info(Function, params_type),
        decode_record(Codec, Function, MessageSpec, Buffer1);
      ?tMessageType_REPLY ->
        ReplySpec = ServiceModule:function_info(Function, reply_type),
        MessageSpec = {struct, [ {0, ReplySpec} ]},
        decode_record(Codec, Function, MessageSpec, Buffer1);
      ?tMessageType_EXCEPTION ->
        error({not_handled, MessageType}),
        MessageSpec = undefined
    end,
  {ok, <<>>} = Codec:read(message_end, Buffer2),
  %% io:format("decode\nspec ~p\nargs ~p\n", [ MessageSpec, ArgsTuple ]),
  [ _ | Args ] = tuple_to_list(ArgsTuple),
  {Function, MessageType, SeqId, Args}.


decode (Codec, {struct, {Schema, StructName}}, Buffer)
  when is_atom(Schema), is_atom(StructName) ->
  %% Decode a record from a schema module.
  decode_record(Codec, StructName, Schema:struct_info(StructName), Buffer);

decode (Codec, {list, Type}, Buffer0) ->
  {#protocol_list_begin{etype=EType, size=Size}, Buffer1} = Codec:read(list_begin, Buffer0),
  ?VALIDATE_TYPE(Type, EType),
  {List, Buffer2} = mapfoldn(fun (BufferL0) -> decode(Codec, Type, BufferL0) end, Buffer1, Size),
  {ok, Buffer3} = Codec:read(list_end, Buffer2),
  {List, Buffer3};

decode (Codec, {map, KeyType, ValType}, Buffer0) ->
  {#protocol_map_begin{ktype=KType, vtype=VType, size=Size}, Buffer1} = Codec:read(map_begin, Buffer0),
  ?VALIDATE_TYPE(KeyType, KType),
  ?VALIDATE_TYPE(ValType, VType),
  {List, Buffer2} = mapfoldn(fun (BufferL0) ->
                                 {K, BufferL1} = decode(Codec, KeyType, BufferL0),
                                 {V, BufferL2} = decode(Codec, ValType, BufferL1),
                                 {{K, V}, BufferL2}
                             end, Buffer1, Size),
  {ok, Buffer3} = Codec:read(map_end, Buffer2),
  {dict:from_list(List), Buffer3};

decode (Codec, {set, Type}, Buffer0) ->
  {#protocol_set_begin{etype=EType, size=Size}, Buffer1} = Codec:read(set_begin, Buffer0),
  ?VALIDATE_TYPE(Type, EType),
  {List, Buffer2} = mapfoldn(fun (BufferL0) -> decode(Codec, Type, BufferL0) end, Buffer1, Size),
  {ok, Buffer3} = Codec:read(set_end, Buffer2),
  {sets:from_list(List), Buffer3};

decode (Codec, Type, Buffer0) when is_atom(Type) ->
  %% Decode the basic types.
  TypeId = term_to_typeid(Type),
  Codec:read(TypeId, Buffer0).


-spec decode_record(Codec::atom(), Name::atom(), tuple(), BufferIn::binary()) -> {tuple(), binary()}.
decode_record (Codec, Name, {struct, StructDef}, Buffer0)
  when is_atom(Name), is_list(StructDef) ->
  %% Decode a record from a struct definition.
  {ok, Buffer1} = Codec:read(struct_begin, Buffer0),
  %% If we were going to handle field defaults we could create the initialize
  %% here.  It might be better to wait until after the struct is parsed,
  %% however, to avoid unnecessarily creating initializers for fields that
  %% don't need them. @@
  {Record, Buffer2} = decode_struct(Codec, StructDef, [ {1, Name} ], Buffer1),
  {ok, Buffer3} = Codec:read(struct_end, Buffer2),
  {Record, Buffer3}.


-spec decode_struct(Codec::atom(), FieldList::list(), Acc::list(), BufferIn::binary()) -> {tuple(), binary()}.

decode_struct (Codec, FieldList, Acc, Buffer0) ->
  {#protocol_field_begin{type=FieldTId, id=FieldId}, Buffer1} = Codec:read(field_begin, Buffer0),
  case FieldTId of
    ?tType_STOP ->
      Record = erlang:make_tuple(length(FieldList)+1, undefined, Acc),
      {Record, Buffer1};
    _ ->
      case keyfind(FieldList, FieldId, 2) of %% inefficient @@
        {FieldTypeAtom, N} ->
          ?VALIDATE_TYPE(FieldTypeAtom, FieldTId),
          {Val, Buffer2} = decode(Codec, FieldTypeAtom, Buffer1),
          {ok, Buffer3} = Codec:read(field_end, Buffer2),
          decode_struct(Codec, FieldList, [ {N, Val} | Acc ], Buffer3);
        false ->
          %% io:format("field ~p not found in ~p\n", [ FieldId, FieldList ]),
          {_, Buffer2} = skip(Codec, typeid_to_atom(FieldTId), Buffer1),
          {ok, Buffer3} = Codec:read(field_end, Buffer2),
          decode_struct(Codec, FieldList, Acc, Buffer3)
      end
  end.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

skip (Codec, struct, Buffer0) ->
  {_, Buffer1} = Codec:read(struct_begin, Buffer0),
  Buffer2 = skip_struct(Codec, Buffer1),
  Codec:read(struct_end, Buffer2);

skip (Codec, list, Buffer0) ->
  {#protocol_list_begin{etype=EType, size=Size}, Buffer1} = Codec:read(list_begin, Buffer0),
  Buffer2 = foldn(fun (BufferL0) ->
                      {_, BufferL1} = decode(Codec, typeid_to_atom(EType), BufferL0),
                      BufferL1
                  end, Buffer1, Size),
  Codec:read(list_end, Buffer2);

skip (Codec, map, Buffer0) ->
  {#protocol_map_begin{ktype=KType, vtype=VType, size=Size}, Buffer1} = Codec:read(map_begin, Buffer0),
  Buffer2 = foldn(fun (BufferL0) ->
                      {_, BufferL1} = decode(Codec, typeid_to_atom(KType), BufferL0),
                      {_, BufferL2} = decode(Codec, typeid_to_atom(VType), BufferL1),
                      BufferL2
                  end, Buffer1, Size),
  Codec:read(map_end, Buffer2);

skip (Codec, set, Buffer0) ->
  {#protocol_set_begin{etype=EType, size=Size}, Buffer1} = Codec:read(set_begin, Buffer0),
  Buffer2 = foldn(fun (BufferL0) ->
                      {_, BufferL1} = decode(Codec, typeid_to_atom(EType), BufferL0),
                      BufferL1
                  end, Buffer1, Size),
  Codec:read(set_end, Buffer2).


skip_struct (Codec, Buffer0) ->
  {#protocol_field_begin{type=Type}, Buffer1} = Codec:read(field_begin, Buffer0),
  case Type of
    ?tType_STOP ->
      Buffer1;
    _ ->
      {_, Buffer2} = skip(Codec, typeid_to_atom(Type), Buffer1),
      {ok, Buffer3} = Codec:read(field_end, Buffer2),
      Buffer3
  end.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

mapfoldn (F, Acc0, N) when N > 0 ->
  {First, Acc1} = F(Acc0),
  {Rest, Acc2} = mapfoldn(F, Acc1, N-1),
  {[ First | Rest ], Acc2};
mapfoldn (F, Acc, 0) when is_function(F, 1) ->
  {[], Acc}.


foldn (F, Acc, N) when N > 0 ->
  foldn(F, F(Acc), N-1);
foldn (F, Acc, 0) when is_function(F, 1) ->
  Acc.

%% Similar to `lists:keyfind', but also returns index of the found element.
keyfind ([ {FieldId, FieldTypeAtom} | Rest ], SearchFieldId, I) ->
  if FieldId =:= SearchFieldId -> {FieldTypeAtom, I};
     true                      -> keyfind(Rest, SearchFieldId, I+1)
  end;
keyfind ([], _, _) -> false.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-ifdef(EUNIT).

mapfoldn_test () ->
  ?assertEqual({"", "abcdef"}, mapfoldn(fun ([ F | R ]) -> {F + $A - $a, R} end, "abcdef", 0)),
  ?assertEqual({"A", "bcdef"}, mapfoldn(fun ([ F | R ]) -> {F + $A - $a, R} end, "abcdef", 1)),
  ?assertEqual({"ABC", "def"}, mapfoldn(fun ([ F | R ]) -> {F + $A - $a, R} end, "abcdef", 3)),
  ?assertEqual({"ABCDEF", ""}, mapfoldn(fun ([ F | R ]) -> {F + $A - $a, R} end, "abcdef", 6)),
  ?assertError(function_clause, mapfoldn(fun ([ F | R ]) -> {F + $A - $a, R} end, "abcdef", 7)).

foldn_test () ->
  ?assertEqual(1, foldn(fun (E) -> E * 2 end, 1, 0)),
  ?assertEqual(2, foldn(fun (E) -> E * 2 end, 1, 1)),
  ?assertEqual(8, foldn(fun (E) -> E * 2 end, 1, 3)).

keyfind_test () ->
  List = [ {a, apple}, {b, banana}, {c, carrot} ],
  ?assertEqual({apple, 1}, keyfind(List, a, 1)),
  ?assertEqual({carrot, 3}, keyfind(List, c, 1)),
  ?assertEqual(false, keyfind(List, d, 1)).

-endif. %% EUNIT
