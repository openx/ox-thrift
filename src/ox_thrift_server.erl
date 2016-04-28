-module(ox_thrift_server).

-include("ox_thrift.hrl").
-include("ox_thrift_internal.hrl").

-behaviour(ranch_protocol).

-export([ start_link/4, handle_request/2 ]).
-export([ init/4 ]).

-record(ts_state, {
          socket,
          transport,
          config :: #ox_thrift_config{} }).

start_link (Ref, Socket, Transport, Opts) ->
  Pid = spawn_link(?MODULE, init, [ Ref, Socket, Transport, Opts ]),
  {ok, Pid}.


init (Ref, Socket, Transport, Config) ->
  ok = ranch:accept_ack(Ref),
  loop(#ts_state{socket = Socket, transport = Transport, config = Config}).


loop (State=#ts_state{socket=Socket, transport=Transport, config=Config=#ox_thrift_config{handler_module=HandlerModule}}) ->
  %% Implement thrift_framed_transport.

  {OK, Closed, Error} = Transport:messages(),

  Result0 =
    case Transport:recv(Socket, 4) of
      {OK, LengthBin} ->
        <<Length:32/integer-signed-big>> = LengthBin,
        Transport:recv(Socket, Length);
      RecvClosedOrError0 ->
        RecvClosedOrError0
    end,

  Result1 =
    case Result0 of
      {OK, RequestData} ->
        case handle_request(Config, RequestData) of
          noreply ->
            %% Oneway void function.
            {OK, 0};
          ReplyData ->
            %% Normal function.
            ReplyLen = iolist_size(ReplyData),
            Transport:send(Socket, [ <<ReplyLen:32/big-signed>>, ReplyData ])
        end;
      RecvClosedOrError1 ->
        RecvClosedOrError1
    end,

  case Result1 of
    {OK, _BytesSent} ->
      loop(State);
    {Closed, _} ->
      HandlerModule:handle_error(undefined, closed),
      Transport:close(Socket);
    {Error, _, Reason} ->
      HandlerModule:handle_error(undefined, Reason),
      Transport:close(Socket)
  end.


-spec handle_request(Config::#ox_thrift_config{}, RequestData::binary()) -> Reply::iolist()|'noreply'.
handle_request (#ox_thrift_config{service_module=ServiceModule, codec_module=CodecModule, handler_module=HandlerModule}, RequestData) ->
  {Function, CallType, SeqId, Args} =
    CodecModule:decode_message(ServiceModule, RequestData),
%%  try
  Result = HandlerModule:handle_function(Function, Args),
  case {CallType, Result} of
    {?tMessageType_CALL, {reply, Reply}} ->
      CodecModule:encode_message(ServiceModule, Function, ?tMessageType_REPLY, SeqId, [ Reply ]);
    {?tMessageType_CALL, ok} ->
      CodecModule:encode_message(ServiceModule, Function, ?tMessageType_REPLY, SeqId, []);
    {?tMessageType_ONEWAY, ok} ->
      noreply
  end.

%%  catch
%%    Error:Reason when Error =:= error; Error =:= throw ->
%%      ReplyData = ox_thrift_protocol:encode_message(Codec, ServiceModule, Function, ?tMessageType_EXCEPTION, SeqId,  )
%%  end
