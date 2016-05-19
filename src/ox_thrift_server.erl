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


-callback handle_function(Function::atom(), Args::list()) -> 'ok' | {'reply', Reply::term()}.

-callback handle_error(Function::atom(), Reason::term()) -> Ignored::term().


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

  %% Read the length, and then the request data.
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

  %% Call the handler for the function.
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
handle_request (Config=#ox_thrift_config{handler_module=HandlerModule}, RequestData) ->
  %% Should do a try block around decode_message. @@
  {Function, CallType, SeqId, Args} = decode(Config, RequestData),

  ResultMsg =
    try
      ?LOG("call ~p:handle_function(~p, ~p)\n", [ HandlerModule, Function, Args ]),
      Result = HandlerModule:handle_function(Function, Args),
      ?LOG("result ~p ~p\n", [ CallType, Result ]),
      case {CallType, Result} of
        {call, {reply, Reply}} ->
          encode(Config, Function, reply_normal, SeqId, Reply);
        {call, ok} ->
          encode(Config, Function, reply_normal, SeqId, undefined);
        {call_oneway, ok} ->
          noreply
      end
    catch
      throw:Reason ->
        encode(Config, Function , reply_exception, SeqId, Reason);
      error:Reason ->
        Message = ox_thrift_util:format_error_message(Reason),
        ExceptionReply = #application_exception{message = Message, type = ?tApplicationException_UNKNOWN},
        encode(Config, Function, exception, SeqId, ExceptionReply)
    end,

  %% ?LOG("handle_request -> ~p\n", [ {ResultMsg, Function } ]),
  {ResultMsg, Function}.


decode(#ox_thrift_config{service_module=ServiceModule, codec_module=CodecModule, stats_module=undefined}, RequestData) ->
  %% Decode, not collecting stats.
  CodecModule:decode_message(ServiceModule, RequestData);
decode(#ox_thrift_config{service_module=ServiceModule, codec_module=CodecModule, stats_module=StatsModule}, RequestData) ->
  %% Decode, collecting stats.
  {Elapsed, Result} = timer:tc(CodecModule, decode_message, [ ServiceModule, RequestData ]),
  Function = element(1, Result),
  StatsModule:handle_stat(Function, decode_time, Elapsed),
  Result.


encode(#ox_thrift_config{service_module=ServiceModule, codec_module=CodecModule, stats_module=undefined}, Function, MessageType, SeqId, Args) ->
  %% Encode, not collecting stats.
  CodecModule:encode_message(ServiceModule, Function, MessageType, SeqId, Args);
encode(#ox_thrift_config{service_module=ServiceModule, codec_module=CodecModule, stats_module=StatsModule}, Function, MessageType, SeqId, Args) ->
  %% Encode, collecting stats.
  {Elapsed, Result} = timer:tc(CodecModule, encode_message, [ ServiceModule, Function, MessageType, SeqId, Args ]),
  StatsModule:handle_stat(Function, encode_time, Elapsed),
  Result.
