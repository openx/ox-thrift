-ifndef(_ox_thrift_internal_included).
-define(_ox_thrift_internal_included, true).


-record(ts_config, {
          service_module :: atom(),
          codec_module :: atom(),
          handler_module :: atom(),
          stats_module :: atom() }).


%% TType
-define(tType_STOP, 0).
-define(tType_VOID, 1).
-define(tType_BOOL, 2).
-define(tType_BYTE, 3).
-define(tType_DOUBLE, 4).
-define(tType_I16, 6).
-define(tType_I32, 8).
-define(tType_I64, 10).
-define(tType_STRING, 11).
-define(tType_STRUCT, 12).
-define(tType_MAP, 13).
-define(tType_SET, 14).
-define(tType_LIST, 15).

% TMessageType
-define(tMessageType_CALL, 1).
-define(tMessageType_REPLY, 2).
-define(tMessageType_EXCEPTION, 3).
-define(tMessageType_ONEWAY, 4).

-type base_type () :: 'void' | 'bool' | 'byte' | 'double' | 'i16' | 'i32' | 'i64' | 'string'.

-type proto_type() :: base_type() | 'struct' | 'set' | 'map' | 'list'.

-type struct_type() :: base_type() | {struct, term()} | {map, struct_type(), struct_type()} | {set, struct_type()} | {list, struct_type()}.

-define(tVoidReply_Structure,
        {struct, []}).

-define(tApplicationException_Structure,
        {struct, [{1, string},
                  {2, i32}]}).

%% -define(LOG(Format, Args), io:format(standard_error, "~s:~p: " ++ Format, [ ?MODULE, ?LINE | Args ])).
-define(LOG(Format, Args), ok).

-endif. %% _ox_thrift_internal_included.
