%% Copyright 2016-2018, OpenX.  All rights reserved.
%% Licensed under the conditions specified in the accompanying LICENSE file.

-include("ox_thrift_internal.hrl").
-include("ox_thrift.hrl").

-ifdef(OXTHRIFT_NO_MAPS).
-define(IS_MAP(Term), false).
-define(MAP_SIZE(Term), 0).
-else.
-define(IS_MAP(Term), is_map(Term)).
-define(MAP_SIZE(Term), map_size(Term)).
-endif.

-compile({inline, [ decode_record/4 ]}).

-dialyer({no_match, [ encode_struct/4, decode/3, decode_struct/5, skip_struct/2 ]}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

encode_record ({Schema, StructName}, Record) when StructName =:= element(1, Record) ->
  iolist_to_binary(encode(Schema:struct_info(StructName), Record, [])).

decode_record ({Schema, StructName}, Buffer, CodecConfig) ->
  {<<>>, Record} = decode_record(Buffer, StructName, Schema:struct_info(StructName), CodecConfig),
  Record.

decode_record (SchemaAndStructName, Buffer) ->
  decode_record(SchemaAndStructName, Buffer, #codec_config{}).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec term_to_typeid(Term::struct_type()) -> TypeId::proto_type().
term_to_typeid (A) when is_atom(A)       -> A;
term_to_typeid ({A, _}) when is_atom(A)  -> A;
term_to_typeid ({map, _, _})             -> map.

-define(SUCCESS_FIELD_ID, 0).

-define(VALIDATE(Expr), (fun () -> Expr end)()).

-define(VALIDATE_TYPE(StructType, SuppliedType, Args),
        case term_to_typeid(StructType) of
          SuppliedType -> ok;
          _ -> error({type_mismatch, {provided, SuppliedType}, {expected, StructType}}, Args)
        end).


-spec encode_call(ServiceModule::atom(), Function::atom(), SeqId::integer(), Args::term()) ->
                     {CallType::message_type(), Data::iolist()}.
encode_call (ServiceModule, Function, SeqId, Args) ->
  CallType = try ServiceModule:function_info(Function, reply_type) of
               oneway_void -> call_oneway;
               _           -> call
             catch error:function_clause ->
                 error({unknown_function, ServiceModule, Function})
             end,
  Data = encode_message(ServiceModule, Function, call, SeqId, Args),
  {CallType, Data}.


-spec encode_message(ServiceModule::atom(), Function::atom(), MessageType::message_type(), SeqId::integer(), Args::term()) -> iolist().
%% `MessageType' is `call', `call_oneway', `reply_normal', 'reply_exception',
%% or `exception'.  If a normal reply, the `Args' argument is a variable of
%% the expected return type for `Function'.  If an exception reply, the `Args'
%% argument is an record of one of the declared exception types.
%%
%% `Args' is a list of function arguments for a `?tMessageType_CALL', and is
%% the reply for a `?tMessageType_REPLY' or exception record for
%% `?tMessageType_EXCEPTION'.
encode_message (ServiceModule, Function, MessageType, SeqId, Args) ->
  case MessageType of
    call ->
      ThriftMessageType = ?tMessageType_CALL,
      MessageSpec = ServiceModule:function_info(Function, params_type),
      ArgsList = list_to_tuple([ Function | Args ]);
    reply_normal ->
      ThriftMessageType = ?tMessageType_REPLY,
      %% Create a fake zero- or one-element structure for the result.
      ReplyName = atom_to_list(Function) ++ "_result",
      case ServiceModule:function_info(Function, reply_type) of
        oneway_void ->
          error(oneway_void), %% This shouldn't happen....
          MessageSpec = undefined,
          ArgsList = undefined;
        ?tVoidReply_Structure ->
          %% A void return
          MessageSpec = ?tVoidReply_Structure,
          ArgsList = {ReplyName};
        ReplySpec ->
          %% A non-void return.
          MessageSpec = {struct, [ {?SUCCESS_FIELD_ID, ReplySpec} ]},
          ArgsList = {ReplyName, Args}
      end;
    reply_exception ->
      %% An exception is treated as a struct with a field for each possible
      %% exception.  Since any given call returns only one exception, all
      %% except one of the fields is `undefined' and so only the field for
      %% the exception actually being thrown is sent over the wire.
      ExceptionName = element(1, Args),
      MessageSpec0 = {struct, ExceptionsSpec} = ServiceModule:function_info(Function, exceptions),
      {ExceptionList, ExceptionFound} =
        lists:mapfoldl(
          fun ({_, {struct, {_, StructExceptionName}}}, FoundAcc) ->
              case StructExceptionName of
                ExceptionName -> {Args, true};
                _             -> {undefined, FoundAcc}
              end
          end, false, ExceptionsSpec),
      ?LOG("exception ~p\n", [ {ExceptionList, ExceptionFound} ]),
      %% If `Exception' is not one of the declared exceptions, turn it into an
      %% application_exception.
      if ExceptionFound ->
          ThriftMessageType = ?tMessageType_REPLY,
          MessageSpec = MessageSpec0,
          ArgsList = list_to_tuple([ Function | ExceptionList ]);
         true ->
          ThriftMessageType = ?tMessageType_EXCEPTION,
          MessageSpec = ?tApplicationException_Structure,
          Message = ox_thrift_util:format_error_message({error_not_declared_as_thrown, Function, ExceptionName}),
          ArgsList = #application_exception{message = Message, type = ?tApplicationException_UNKNOWN}
      end;
    exception ->
      ThriftMessageType = ?tMessageType_EXCEPTION,
      ?VALIDATE(true = is_record(Args, application_exception)),
      MessageSpec = ?tApplicationException_Structure,
      ArgsList = Args
  end,

  ?VALIDATE(begin
              {struct, StructDef} = MessageSpec,
              StructDefLength = length(StructDef),
              ArgsListLength = size(ArgsList) - 1,
              if StructDefLength =/= ArgsListLength ->
                  %% io:format(standard_error, "arg_length_mismatch\ndef ~p\narg ~p\n", [ StructDef, ArgsList ]),
                  error({arg_length_mismatch, {provided, ArgsListLength}, {expected, StructDefLength}});
                 true -> ok
              end
            end),

  [ write_message_begin(atom_to_binary(Function, latin1), ThriftMessageType, SeqId)
    %% Thrift supports only lists of uniform types, and so it uses a
    %% function-specific struct for a function's argument list.
  , encode(MessageSpec, ArgsList, [])
  ].


encode ({struct, StructDef}, Data, Acc)
  when is_list(StructDef), is_tuple(Data), length(StructDef) == size(Data) - 1 ->
  %% Encode a record from a struct definition.
  encode_struct(StructDef, Data, 2, 0, Acc);

encode ({struct, {Schema, StructName}}, Data, Acc)
  when is_atom(Schema), is_atom(StructName), is_tuple(Data), element(1, Data) == StructName ->
  %% Encode a record from a schema module.
  encode(Schema:struct_info(StructName), Data, Acc);

encode (S={struct, {_Schema, _StructName}}, Data, _Acc) ->
  error(struct_unmatched, [ S, Data ]);

encode ({list, Type}, Data, Acc)
  when is_list(Data) ->
  %% Encode a list.
  write_list_or_set_begin(Type, length(Data),
                          lists:foldr(fun (Elt, InnerAcc) -> encode(Type, Elt, InnerAcc) end, Acc, Data));

encode ({map, KeyType, ValType}, Data, Acc) ->
  %% Encode a map.
  if
    is_list(Data) ->
      %% Encode a proplist as a map.
      write_map_begin(KeyType, ValType, length(Data),
                      lists:foldl(fun ({Key, Val}, InnerAcc) ->
                                      encode(KeyType, Key,
                                             encode(ValType, Val, InnerAcc))
                                  end, Acc, Data));
    ?IS_MAP(Data) ->
      write_map_begin(KeyType, ValType, ?MAP_SIZE(Data),
                      maps:fold(fun (Key, Val, InnerAcc) ->
                                    encode(KeyType, Key,
                                           encode(ValType, Val, InnerAcc))
                                end, Acc, Data));
    true ->
      %% Encode an Erlang dict as a map.
      write_map_begin(KeyType, ValType, dict:size(Data),
                      dict:fold(fun (Key, Val, InnerAcc) ->
                                    encode(KeyType, Key,
                                          encode(ValType, Val, InnerAcc))
                                end, Acc, Data))
  end;

encode ({set, Type}, Data, Acc) ->
  %% Encode a set.
  if
    is_list(Data) ->
      %% Encode a list as a set.
      write_list_or_set_begin(Type, length(Data),
                              lists:foldl(fun (Elt, InnerAcc) ->
                                              encode(Type, Elt, InnerAcc)
                                          end, Acc, Data));
    true ->
      %% Encode an Erlang set as a set.
      write_list_or_set_begin(Type, sets:size(Data),
                              sets:fold(fun (Elt, InnerAcc) ->
                                            encode(Type, Elt, InnerAcc)
                                        end, Acc, Data))
  end;

encode (Type, Data, Acc) when is_atom(Type) ->
  %% Encode the basic types.
  write(Type, Data, Acc);

encode (Type, Data, _Acc) ->
  error({invalid_type, {type, Type}, {data, Data}}).


-spec encode_struct(FieldData::list({integer(), atom()}), Record::tuple(), I::integer(), LastId::integer(), Acc::iolist()) -> IOData::iolist().
encode_struct ([ {FieldId, Type} | FieldRest ], Record, I, LastId, Acc) ->
  case element(I, Record) of
    undefined ->
      %% null fields are skipped
      encode_struct(FieldRest, Record, I+1, LastId, Acc);
    Data ->
      write_field(Type, FieldId, LastId, Data,
                  encode_struct(FieldRest, Record, I+1, FieldId, Acc))
  end;

encode_struct ([], _Record, _I, _LastId, Acc) ->
  write_field_stop(Acc).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec decode_message(ServiceModule::atom(), codec_config(), Buffer::binary()) ->
                        {Function::atom(), MessageType::message_type(), Seqid::integer(), Args::term()}.
%% `MessageType' is `?tMessageType_CALL', `?tMessageType_ONEWAY', `?tMessageReply', or `?tMessageException'.
decode_message (ServiceModule, CodecConfig, Buffer0) ->
  {Buffer1, FunctionBin, ThriftMessageType, SeqId} =
    read_message_begin(Buffer0),
  Function = binary_to_atom(FunctionBin, latin1),
  case ThriftMessageType of
    ?tMessageType_CALL ->
      MessageType = case ServiceModule:function_info(Function, reply_type) of
                      oneway_void -> call_oneway;
                      _           -> call
                    end,
      MessageSpec = ServiceModule:function_info(Function, params_type),
      {Buffer2, ArgsTuple} = decode_record(Buffer1, Function, MessageSpec, CodecConfig),
      [ _ | Args ] = tuple_to_list(ArgsTuple);
    ?tMessageType_ONEWAY ->
      MessageType = call_oneway,
      MessageSpec = ServiceModule:function_info(Function, params_type),
      {Buffer2, ArgsTuple} = decode_record(Buffer1, Function, MessageSpec, CodecConfig),
      [ _ | Args ] = tuple_to_list(ArgsTuple);
    ?tMessageType_REPLY ->
      MessageSpec = ServiceModule:function_info(Function, reply_type),
      {Buffer2, {_, Args}, MessageType} = decode_reply(Buffer1, ServiceModule, Function, MessageSpec, CodecConfig);
    ?tMessageType_EXCEPTION ->
      MessageType = exception,
      MessageSpec = ?tApplicationException_Structure,
      {Buffer2, Args} = decode_record(Buffer1, application_exception, MessageSpec, CodecConfig)
  end,
  <<>> = Buffer2,
  %% io:format(standard_error, "decode\nspec ~p\nargs ~p\n", [ MessageSpec, Args ]),
  {Function, MessageType, SeqId, Args}.


decode_reply (Buffer0, ServiceModule, Function, ReplySpec, CodecConfig) ->
  {struct, ExceptionDef} = ServiceModule:function_info(Function, exceptions),
  MessageSpec = {struct, [ {?SUCCESS_FIELD_ID, ReplySpec} | ExceptionDef ]},
  {Buffer1, ArgsTuple} = decode_record(Buffer0, Function, MessageSpec, CodecConfig),
  [ F, Reply | Exceptions ] = tuple_to_list(ArgsTuple),
  %% Check for an exception.
  case first_defined(Exceptions)of
    undefined ->
      case ReplySpec of
        ?tVoidReply_Structure -> {Buffer1, {F, ok}, reply_normal};
        _                     -> {Buffer1, {F, Reply}, reply_normal}
      end;
    Exception                 -> {Buffer1, {F, Exception}, reply_exception}
  end.


-spec decode(BufferIn::binary(), Spec::term(), codec_config()) -> {BufferOut::binary(), Decoded::term()}.
decode (Buffer, {struct, {Schema, StructName}}, CodecConfig)
  when is_atom(Schema), is_atom(StructName) ->
  %% Decode a record from a schema module.
  decode_record(Buffer, StructName, Schema:struct_info(StructName), CodecConfig);

decode (Buffer0, _T={list, Type}, CodecConfig) ->
  {Buffer1, EType, Size} = read_list_or_set_begin(Buffer0),
  ?VALIDATE_TYPE(Type, EType, [ Buffer0, _T ]),
  decode_list(Buffer1, Type, CodecConfig, [], Size);

decode (Buffer0, _T={map, KeyType, ValType}, CodecConfig) ->
  {Buffer1, KType, VType, Size} = read_map_begin(Buffer0),
  if ?THRIFT_PROTOCOL =/= compact orelse Size =/= 0 ->
      %% Types are not sent on wire for compact if size is 0.
      ?VALIDATE_TYPE(KeyType, KType, [ Buffer0, _T ]),
      ?VALIDATE_TYPE(ValType, VType, [ Buffer0, _T ]);
     true -> ok
  end,
  decode_map(Buffer1, {KeyType, ValType}, CodecConfig, [], Size);

decode (Buffer0, _T={set, Type}, CodecConfig) ->
  {Buffer1, EType, Size} = read_list_or_set_begin(Buffer0),
  ?VALIDATE_TYPE(Type, EType, [ Buffer0, _T ]),
  decode_set(Buffer1, Type, CodecConfig, [], Size);

decode (Buffer0, Type, _) when is_atom(Type) ->
  %% Decode the basic types.
  read(Buffer0, Type).


-spec decode_record(BufferIn::binary(), Name::atom(), tuple(), codec_config()) -> {binary(), tuple()}.
decode_record (Buffer0, Name, {struct, StructDef}, CodecConfig)
  when is_atom(Name), is_list(StructDef) ->
  %% Decode a record from a struct definition.
  %% If we were going to handle field defaults we could create the initialize
  %% here.  It might be better to wait until after the struct is parsed,
  %% however, to avoid unnecessarily creating initializers for fields that
  %% don't need them. @@
  decode_struct(Buffer0, StructDef, CodecConfig, [ {1, Name} ], 0).


-spec decode_struct(BufferIn::binary(), FieldList::list(), codec_config(), Acc::list(), LastId::integer()) -> {binary(), tuple()}.
decode_struct (Buffer0, FieldList, CodecConfig, Acc, LastId) ->
  case read_field_begin(Buffer0, LastId) of
    {Buffer1, field_stop} ->
      Record = erlang:make_tuple(length(FieldList)+1, undefined, Acc),
      {Buffer1, Record};
    {Buffer1, ReadType, FieldId, Val} ->        % base type with value (boolean, integer, double, binary)
      case keyfind(FieldList, FieldId, 2) of %% inefficient @@
        {ExpectedType, N} ->                    %   known field
          ?VALIDATE_TYPE(ExpectedType, ReadType, [ Buffer0, FieldList, Acc, LastId ]),
          decode_struct(Buffer1, FieldList, CodecConfig, [ {N, Val} | Acc ], FieldId);
        false ->                                %   unknown field; skip it
          decode_struct(Buffer1, FieldList, CodecConfig, Acc, FieldId)
      end;
    {Buffer1, ReadType, FieldId} ->             % type without value (struct, set, map, list)
      case keyfind(FieldList, FieldId, 2) of %% inefficient @@
        {ExpectedType, N} ->                    %   known field
          ?VALIDATE_TYPE(ExpectedType, ReadType, [ Buffer0, FieldList, Acc, LastId ]),
          {Buffer2, Val} = decode(Buffer1, ExpectedType, CodecConfig),
          decode_struct(Buffer2, FieldList, CodecConfig, [ {N, Val} | Acc ], FieldId);
        false ->                                %   unknown field; skip it
          %% io:format("field ~p not found in ~p\n", [ FieldId, FieldList ]),
          Buffer2 = skip(Buffer1, ReadType),
          decode_struct(Buffer2, FieldList, CodecConfig, Acc, FieldId)
      end
  end.


-spec decode_list(IBuffer::binary(), EType::struct_type(), codec_config(), Acc::list(), N::non_neg_integer()) -> {OBuffer::binary(), Result::list()}.
decode_list (Buffer, _, _, Acc, 0) -> {Buffer, lists:reverse(Acc)};
decode_list (Buffer0, EType, CodecConfig, Acc, N) ->
  {Buffer1, Elt} = decode(Buffer0, EType, CodecConfig),
  decode_list(Buffer1, EType, CodecConfig, [ Elt | Acc ], N - 1).


-spec decode_map(IBuffer::binary(), {KType::struct_type(), VType::struct_type()}, codec_config(), Acc::list(), N::non_neg_integer()) -> {OBuffer::binary(), Result::term()}.
decode_map (Buffer, _, CodecConfig, Acc, 0) ->
  {Buffer, (CodecConfig#codec_config.map_module):from_list(Acc)};
decode_map (Buffer0, Types={KType, VType}, CodecConfig, Acc, N) ->
  {Buffer1, K} = decode(Buffer0, KType, CodecConfig),
  {Buffer2, V} = decode(Buffer1, VType, CodecConfig),
  decode_map(Buffer2, Types, CodecConfig, [ {K, V} | Acc ], N - 1).


-spec decode_set(IBuffer::binary(), EType::struct_type(), codec_config(), Acc::list(), N::non_neg_integer()) -> {OBuffer::binary(), Result::term()}.
decode_set (Buffer, _, _, Acc, 0) ->
  {Buffer, sets:from_list(Acc)};
decode_set (Buffer0, EType, CodecConfig, Acc, N) ->
  {Buffer1, Elt} = decode(Buffer0, EType, CodecConfig),
  decode_set(Buffer1, EType, CodecConfig, [ Elt | Acc ], N - 1).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec skip(Buffer0::binary(), Type::atom()) -> Buffer1::binary().
skip (Buffer0, struct) ->
  skip_struct(Buffer0, 0);

skip (Buffer0, list) ->
  {Buffer1, EType, Size} = read_list_or_set_begin(Buffer0),
  foldn(fun (BufferL0) ->
            BufferL1 = skip(BufferL0, EType),
            BufferL1
        end, Buffer1, Size);

skip (Buffer0, map) ->
  {Buffer1, KType, VType, Size} = read_map_begin(Buffer0),
  foldn(fun (BufferL0) ->
            BufferL1 = skip(BufferL0, KType),
            BufferL2 = skip(BufferL1, VType),
            BufferL2
        end, Buffer1, Size);

skip (Buffer0, set) ->
  {Buffer1, EType, Size} = read_list_or_set_begin(Buffer0),
  foldn(fun (BufferL0) ->
            BufferL1 = skip(BufferL0, EType),
            BufferL1
        end, Buffer1, Size);

skip (Buffer0, Type) when is_atom(Type) ->
  %% Skip the basic types.
  {Buffer, _Value} = read(Buffer0, Type),
  Buffer.


-spec skip_struct (Buffer0::binary(), LastId::integer()) -> Buffer1::binary().
skip_struct (Buffer0, LastId) ->
  case read_field_begin(Buffer0, LastId) of
    {Buffer1, field_stop} ->
      Buffer1;
    {Buffer1, _ReadType, FieldId, _Val} ->
      skip_struct(Buffer1, FieldId);
    {Buffer1, ReadType, FieldId} ->
      Buffer2 = skip(Buffer1, ReadType),
      skip_struct(Buffer2, FieldId)
  end.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

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

%% Returns the first element of a list that is not `undefined', or `undefined'
%% if all of the elements are `undefined'.
first_defined ([ undefined | Rest ]) ->
  first_defined(Rest);
first_defined ([ First | _ ]) -> First;
first_defined ([]) -> undefined.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-ifdef(EUNIT).

foldn_test () ->
  ?assertEqual(1, foldn(fun (E) -> E * 2 end, 1, 0)),
  ?assertEqual(2, foldn(fun (E) -> E * 2 end, 1, 1)),
  ?assertEqual(8, foldn(fun (E) -> E * 2 end, 1, 3)).

keyfind_test () ->
  List = [ {a, apple}, {b, banana}, {c, carrot} ],
  ?assertEqual({apple, 1}, keyfind(List, a, 1)),
  ?assertEqual({carrot, 3}, keyfind(List, c, 1)),
  ?assertEqual(false, keyfind(List, d, 1)).

first_defined_test () ->
  ?assertEqual(undefined, first_defined([])),
  ?assertEqual(1, first_defined([ 1, undefined, 3 ])),
  ?assertEqual(2, first_defined([ undefined, 2, 3 ])).

-endif. %% EUNIT
