-ifndef(OX_THRIFT_INCLUDED).
-define(OX_THRIFT_INCLUDED, true).

-type ox_thrift_option() ::
        { recv_timeout, RecvTimeout :: non_neg_integer() | 'infinity' } |
        { map_module, 'dict' | 'maps' } |
        { max_message_size, MaxMessageSize :: pos_integer() | 'infinity' } |
        { stats_module, StatsModule :: atom() } |
        { spawn_options, list(term()) }.

-record(ox_thrift_config, {
          %% The service module, produced from your Thrift definition file by
          %% the Thrift compiler, that defines the service's structures and
          %% interfaces.
          service_module :: atom(),
          %% The protocol module that is used to decode and encode messages.
          %%
          %% If unset (i.e. set to undefined) ox_thrift_server will decide between
          %% the two defaulting (ox_thrift_protocol_binary or
          %% ox_thrift_protocol_compact) protocols to decode the request and encode
          %% the reply automatically based on incoming request.
          protocol_module :: atom(),
          %% The handler module that implements the ox_thrift_server behaviour
          %% to implement the service.
          handler_module :: atom(),
          options = [] :: list(ox_thrift_option()) }).

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

-endif. %% ! OX_THRIFT_INCLUDED
