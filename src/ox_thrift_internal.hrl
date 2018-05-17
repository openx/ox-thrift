-ifndef(OX_THRIFT_INTERNAL_INCLUDED).
-define(OX_THRIFT_INTERNAL_INCLUDED, true).

-record(codec_config, {
          map_module = 'dict' :: 'dict' | 'maps' }).
-type codec_config() :: #codec_config{}.

-record(ts_config, {
          service_module :: atom(),
          protocol_module :: atom(),
          handler_module :: atom(),
          codec_config = #codec_config{} :: codec_config(),
          recv_timeout = 'infinity' :: non_neg_integer() | 'infinity',
          max_message_size = 'infinity' :: pos_integer() | 'infinity',
          stats_module :: atom() }).

-type message_type() :: 'call' | 'call_oneway' | 'reply_normal' | 'reply_exception' | 'exception'.

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

-endif. %% ! OX_THRIFT_INTERNAL_INCLUDED.
