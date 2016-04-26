-ifndef(OX_THRIFT_HRL_INCLUDED).
-define(OX_THRIFT_HRL_INCLUDED, true).

-record(ox_thrift_config, {
          service_module :: atom(),
          codec_module :: atom(),
          handler_module :: atom() }).

-endif. %% OX_THRIFT_HRL_INCLUDED
