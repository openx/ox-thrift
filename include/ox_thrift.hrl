-ifndef(OX_THRIFT_HRL_INCLUDED).
-define(OX_THRIFT_HRL_INCLUDED, true).

-record(ox_thrift_config, {
          service_module :: atom(),
          codec_module :: atom(),
          handler_module :: atom(),
          stats_module = undefined :: atom() }).

-define(tApplicationException_UNKNOWN, 0).
-define(tApplicationException_UNKNOWN_METHOD, 1).
-define(tApplicationException_INVALID_MESSAGE_TYPE, 2).
-define(tApplicationException_WRONG_METHOD_NAME, 3).
-define(tApplicationException_BAD_SEQUENCE_ID, 4).
-define(tApplicationException_MISSING_RESULT, 5).
-define(tApplicationException_INTERNAL_ERROR, 6).
-define(tApplicationException_PROTOCOL_ERROR, 7).
-define(tApplicationException_INVALID_TRANSFORM, 8).
-define(tApplicationException_INVALID_PROTOCOL, 9).
-define(tApplicationException_UNSUPPORTED_CLIENT_TYPE, 10).

-record(application_exception, {
          message::binary(),
          type::integer() }).

-endif. %% OX_THRIFT_HRL_INCLUDED
