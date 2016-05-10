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

-define(RECV_TIMEOUT, infinity).

start_link (Ref, Socket, Transport, Opts) ->
  Pid = spawn_link(?MODULE, init, [ Ref, Socket, Transport, Opts ]),
  {ok, Pid}.


init (Ref, Socket, Transport, Config) ->
  ?LOG("ox_thrift_server:init ~p ~p ~p ~p\n", [ Ref, Socket, Transport, Config ]),
  ok = ranch:accept_ack(Ref),
  loop(#ts_state{socket = Socket, transport = Transport, config = Config}).


loop (State=#ts_state{socket=Socket, transport=Transport, config=Config=#ox_thrift_config{handler_module=HandlerModule}}) ->
  %% Implement thrift_framed_transport.

  Result0 =
    %% Result0 will be `{ok, Packet}' or `{error, Reason}'.
    case Transport:recv(Socket, 4, ?RECV_TIMEOUT) of
      {ok, LengthBin} ->
        <<Length:32/integer-signed-big>> = LengthBin,
        Transport:recv(Socket, Length, ?RECV_TIMEOUT);
      Error0 ->
        Error0
    end,

  ?LOG("recv -> ~p\n", [ Result0 ]),

  {Result1, Function} =
    %% Result1 will be `ok' or `{error, Reason}'.
    case Result0 of
      {ok, RequestData} ->
        case handle_request(Config, RequestData) of
          {noreply, Function1} ->
            %% Oneway void function.
            {ok, Function1};
          {ReplyData, Function1} ->
            %% Normal function.
            ReplyLen = iolist_size(ReplyData),
            Reply = [ <<ReplyLen:32/big-signed>>, ReplyData ],
            X = Transport:send(Socket, Reply),
            ?LOG("server send(~p, ~p) -> ~p\n", [ Socket, Reply, X ]),
            {X, Function1}
        end;
      Error1 ->
        {Error1, undefined}
    end,

  case Result1 of
    ok ->
      loop(State);
    {error, Reason} ->
      HandlerModule:handle_error(Function, Reason),
      Transport:close(Socket)
      %% Return from loop on error.
  end.


-spec handle_request(Config::#ox_thrift_config{}, RequestData::binary()) -> {Reply::iolist()|'noreply', Function::atom()}.
handle_request (#ox_thrift_config{service_module=ServiceModule, codec_module=CodecModule, handler_module=HandlerModule}, RequestData) ->
  %% Should do a try block around decode_message. @@
  {DecodeMilliSeconds, {Function, CallType, SeqId, Args}} =
    timer:tc(CodecModule, decode_message, [ ServiceModule, RequestData ]),
  ox_thrift_stats:record(decode, DecodeMilliSeconds),

  {EncodeMilliSeconds, ResultMsg} =
    try
      ?LOG("call ~p:handle_function(~p, ~p)\n", [ HandlerModule, Function, Args ]),
      Result = HandlerModule:handle_function(Function, Args),
      ?LOG("result ~p ~p\n", [ CallType, Result ]),
      case {CallType, Result} of
        {call, {reply, Reply}} ->
          timer:tc(CodecModule, encode_message, [ ServiceModule, Function, reply_normal, SeqId, Reply ]);
        {call, ok} ->
          timer:tc(CodecModule, encode_message, [ ServiceModule, Function, reply_normal, SeqId, undefined ]);
        {call_oneway, ok} ->
          {0, noreply}
      end
    catch
      throw:Reason ->
        timer:tc(CodecModule, encode_message, [ ServiceModule, Function , reply_exception, SeqId, Reason ]);
      error:Reason ->
        Message = ox_thrift_util:format_error_message(Reason),
        ExceptionReply = #application_exception{message = Message, type = ?tApplicationException_UNKNOWN},
        timer:tc(CodecModule, encode_message, [ ServiceModule, Function, exception, SeqId, ExceptionReply ])
    end,
  ResultMsg =/= noreply andalso
    ox_thrift_stats:record(encode, EncodeMilliSeconds),

  %% ?LOG("handle_request -> ~p\n", [ {ResultMsg, Function } ]),
  {ResultMsg, Function}.
