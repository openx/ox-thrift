%% Copyright 2019, OpenX.  All rights reserved.
%% Licensed under the conditions specified in the accompanying LICENSE file.

-module(ox_thrift_shackle_client).

-behaviour(shackle_client).

-include("ox_thrift_internal.hrl").

%% shackle_client callbacks.
-export([ init/1, setup/2, handle_request/2, handle_data/2, terminate/1 ]).

-record(state, {
          service_module :: atom(),
          protocol_module :: atom(),
          codec_config = #codec_config{} :: codec_config(),
          seqid = 0 :: integer(),
          buffer = <<>> :: binary() }).

init (Options) ->
  State = parse_options(Options, #state{}),
  State#state.service_module =/= undefined orelse error(service_module),
  State#state.protocol_module =/= undefined orelse error(protocol_module),
  {ok, State}.

%% Prepare a socket for use.
setup (_Socket, State) ->
  {ok, State}.

handle_request ({Function, Args}, State=#state{service_module=Service, protocol_module=Protocol, seqid=SeqId}) ->
  %% ox_thrift_shackle_client does not currently handle oneway_void thrift
  %% functions because the thrift server does not return any response.
  {call, RequestData} = Protocol:encode_call(Service, Function, SeqId, Args),
  Length = iolist_size(RequestData),
  {ok, SeqId, [ <<Length:32/big-signed>>, RequestData ], State#state{seqid = SeqId + 1}}.

handle_data (Data, State=#state{service_module=Service, protocol_module=Protocol, codec_config=CodecConfig, buffer=Buffer}) ->
  Data2 = case Buffer of
            <<>> -> Data;
            _    -> <<Data/binary, Buffer/binary>>
          end,
  {Replies, Buffer2} = parse_replies(Service, Protocol, CodecConfig, Data2, []),
  {ok, Replies, State#state{buffer = Buffer2}}.

terminate (_State) ->
  ok.

%% Internal functions.

parse_options ([ {service_module, ServiceModule} | Options ], State)
  when is_atom(ServiceModule) ->
  parse_options(Options, State#state{service_module = ServiceModule});
parse_options ([ {protocol_module, ProtocolModule} | Options ], State)
  when is_atom(ProtocolModule) ->
  parse_options(Options, State#state{protocol_module = ProtocolModule});
parse_options ([ {map_module, MapModule} | Options ], State=#state{codec_config=CodecConfig})
  when MapModule =:= 'dict'; MapModule =:= 'maps' ->
  parse_options(Options, State#state{codec_config = CodecConfig#codec_config{map_module = MapModule}});
parse_options ([], State) ->
  State.

parse_replies (Service, Protocol, CodecConfig, Buffer, Acc) ->
  case Buffer of
    <<Length:32/big-signed, ReplyData:Length/binary, Buffer2/binary>> ->
      {_Function, MessageType, SeqId, Result} = Protocol:decode_message(Service, CodecConfig, ReplyData),
      parse_replies(Service, Protocol, CodecConfig, Buffer2, [ {SeqId, {reply_status(MessageType), Result}} | Acc ]);
    _ ->
      {Acc, Buffer}
  end.

reply_status (reply_normal)    -> ok;
reply_status (reply_exception) -> throw;
reply_status (exception)       -> error.
