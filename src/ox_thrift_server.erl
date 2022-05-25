%% Copyright 2016-2018 OpenX.  All rights reserved.
%% Licensed under the conditions specified in the accompanying LICENSE file.

-module(ox_thrift_server).

-include("ox_thrift.hrl").
-include("ox_thrift_internal.hrl").

-behaviour(ranch_protocol).

-export([ start_link/4 ]).                      % ranch_protocol behaviour.
-export([ init/3 ]).
-export([ parse_config/1, handle_request/2 ]).  % Exported for unit tests.

-record(ts_state, {
          socket,
          transport,
          config :: #ts_config{},
          call_count = 0 :: non_neg_integer(),
          connect_time = os:timestamp() :: erlang:timestamp() }).


-type reply_options() :: 'undefined' | 'close'.

-callback handle_function(Function::atom(), Args::list()) ->
  'ok' | {'reply', Reply::term()} | {ok, reply_options()} | {'reply', Reply::term(), reply_options()}.

-callback handle_error(Function::atom(), Reason::term()) -> Ignored::term().


start_link (Ref, _Socket, Transport, Opts) ->
  Pid = spawn_link(?MODULE, init, [ Ref, Transport, Opts ]),
  {ok, Pid}.


init (Ref, Transport, Config) ->
  ?LOG("ox_thrift_server:init ~p ~p ~p\n", [ Ref, Transport, Config ]),
  {ok, Socket} = ranch:handshake(Ref),
  Transport:setopts(Socket, [ {nodelay, true} ]),

  TSConfig = parse_config(Config),

  loop(#ts_state{socket = Socket, transport = Transport, config = TSConfig}).


-spec parse_config(#ox_thrift_config{}) -> #ts_config{}.
parse_config (#ox_thrift_config{service_module=ServiceModule, protocol_module=ProtocolModule, handler_module=HandlerModule, options=Options}) ->
  Config0 = #ts_config{
               service_module = ServiceModule,
               protocol_module = ProtocolModule,
               handler_module = HandlerModule},
  parse_options(Options, Config0).


parse_options ([ {recv_timeout, RecvTimeout} | Options ], Config)
  when (is_integer(RecvTimeout) andalso RecvTimeout >= 0) orelse (RecvTimeout =:= infinity) ->
  parse_options(Options, Config#ts_config{recv_timeout = RecvTimeout});
parse_options ([ {map_module, MapModule} | Options ], Config=#ts_config{codec_config=CodecConfig})
  when MapModule =:= 'dict'; MapModule =:= 'maps' ->
  parse_options(Options, Config#ts_config{codec_config = CodecConfig#codec_config{map_module = MapModule}});
parse_options ([ {max_message_size, MaxMessageSize} | Options ], Config)
  when (is_integer(MaxMessageSize) andalso MaxMessageSize > 0) orelse (MaxMessageSize =:= infinity) ->
  parse_options(Options, Config#ts_config{max_message_size = MaxMessageSize});
parse_options ([ {stats_module, StatsModule} | Options ], Config) when is_atom(StatsModule) ->
  parse_options(Options, Config#ts_config{stats_module = StatsModule});
parse_options ([ {spawn_options, SpawnOptions} | Options ], Config)
  when is_list(SpawnOptions) orelse SpawnOptions =:= undefined ->
  SO = if is_list(SpawnOptions) -> [ link | SpawnOptions ];
          true                  -> SpawnOptions
       end,
  parse_options(Options, Config#ts_config{spawn_options = SO});
parse_options ([], Config) ->
  Config.


loop (State=#ts_state{socket=Socket, transport=Transport,
                      config=#ts_config{recv_timeout=RecvTimeout, max_message_size=MaxMessageSize}}) ->
  %% Implement thrift_framed_transport.

  %% Read the length, and then the request data.
  %% Result0 will be `{ok, Packet}' or `{error, Reason}'.
  case Transport:recv(Socket, 4, RecvTimeout) of
    {ok, <<Length:32/integer-unsigned-big>>} when MaxMessageSize =:= infinity; Length =< MaxMessageSize ->
      loop1(State#ts_state{call_count = State#ts_state.call_count + 1}, Length);
    {ok, LengthBin} ->
      handle_large_message(State, LengthBin);
    {error, Reason} ->
      handle_error(State, undefined, Reason)
      %% Return from loop on error.
  end.

loop1 (State=#ts_state{socket=Socket, transport=Transport, config=#ts_config{recv_timeout=RecvTimeout}}, Length) ->
  RecvResult = Transport:recv(Socket, Length, RecvTimeout),
  ?LOG("server recv(~p, ~p) -> ~p\n", [ Socket, Length, RecvResult ]),
  case RecvResult of
    {ok, RequestData} ->
      loop2(State, RequestData);
    {error, Reason} ->
      handle_error(State, undefined, Reason)
      %% Return from loop on error.
  end.

loop2 (State=#ts_state{socket=Socket, transport=Transport, config=Config}, RequestData) ->
  {ReplyData, Function, ReplyOptions} = handle_request(Config, RequestData),
  case ReplyData of
    noreply ->
      %% Oneway void function.
      loop3(State, Function, ReplyOptions);
    _ ->
      %% Normal function.
      ReplyLen = iolist_size(ReplyData),
      Reply = [ <<ReplyLen:32/integer-unsigned-big>>, ReplyData ],
      SendResult = Transport:send(Socket, Reply),
      ?LOG("server send(~p, ~p) -> ~p\n", [ Socket, Reply, SendResult ]),
      case SendResult of
        ok ->
          loop3(State, Function, ReplyOptions);
        {error, Reason} ->
          handle_error(State, Function, Reason)
          %% Return from loop on error.
      end
  end.

loop3 (State, Function, ReplyOptions) ->
  case ReplyOptions of
    undefined ->
      loop(State);
    close ->
      handle_error(State, Function, closed)
      %% Return from loop when server requests close.
  end.


-spec handle_request(Config::#ts_config{}, RequestData::binary()) -> {ReplyData::iodata()|'noreply', Function::atom(), ReplyOptions::reply_options()}.
handle_request (Config=#ts_config{spawn_options=SpawnOptions}, RequestData) when is_list(SpawnOptions) ->
  Parent = self(),
  Child = spawn_opt(
            fun () ->
                RespMsg = case handle_request2(Config, RequestData) of
                            R={ReplyData, _, _} when is_list(ReplyData) -> setelement(1, R, list_to_binary(ReplyData));
                            R                                           -> R
                          end,
                Parent ! {self(), RespMsg}
            end, SpawnOptions),
  receive {Child, Response} -> Response end;
handle_request (Config, RequestData) ->
  handle_request2(Config, RequestData).


handle_request2 (Config=#ts_config{protocol_module=ProtocolModule, handler_module=HandlerModule}, RequestData) ->
  Protocol = detect_protocol(RequestData, ProtocolModule),

  %% Should do a try block around decode_message. @@
  {Function, CallType, SeqId, Args} = decode(Config, Protocol, RequestData),

  {ResultMsg, ReplyOptions} =
    try
      ?LOG("call ~p:handle_function(~p, ~p)\n", [ HandlerModule, Function, Args ]),
      Result = HandlerModule:handle_function(Function, Args),
      ?LOG("result ~p ~p\n", [ CallType, Result ]),
      {Reply, ReplyOptions0} = reply_options(Result),
      R = case {CallType, Reply} of
            {call, Reply}            -> encode(Config, Protocol, Function, reply_normal, SeqId, Reply);
            {call_oneway, undefined} -> noreply
          end,
      {R, ReplyOptions0}
    catch
      throw:Reason when is_tuple(Reason) ->
        case CallType of
          call        -> {encode(Config, Protocol, Function, reply_exception, SeqId, Reason), undefined};
          call_oneway -> %% If a oneway function returns an exception perhaps we should close the connection. @@
                         {noreply, undefined}
        end;
      ErrorOrThrow:Reason:Stacktrace when ErrorOrThrow =:= error; ErrorOrThrow =:= throw ->
        case CallType of
          call        -> Message = ox_thrift_util:format_error_message(ErrorOrThrow, Reason, Stacktrace),
                         ExceptionReply = #application_exception{message = Message, type = ?tApplicationException_UNKNOWN},
                         {encode(Config, Protocol, Function, exception, SeqId, ExceptionReply), undefined};
          call_oneway -> %% If a oneway function returns an exception perhaps we should close the connection. @@
                         {noreply, undefined}
        end
    end,

  %% ?LOG("handle_request -> ~p\n", [ {ResultMsg, Function } ]),
  {ResultMsg, Function, ReplyOptions}.


reply_options ({reply, Reply})          -> {Reply, undefined};
reply_options (ok)                      -> {undefined, undefined};
reply_options ({reply, Reply, Options}) -> {Reply, Options};
reply_options ({ok, Options})           -> {undefined, Options}.


handle_large_message (State=#ts_state{socket=Socket, transport=Transport}, Buffer0) ->
  case inet:peername(Socket) of
    {ok, RemoteHostPort} -> ok;
    {error, _}           -> RemoteHostPort = undefined
  end,
  %% Erlang doesn't provide a way to request just "at most N" bytes, so we
  %% just receive whatever is currently available.
  Data = case Transport:recv(Socket, 0, 0) of
           {ok, Buffer1} -> <<Buffer0/binary, Buffer1/binary>>;
           {error, _}    -> Buffer0
         end,
  handle_error(State, undefined, {large_message, {remote, RemoteHostPort}, {data, Data}}).


handle_error (State=#ts_state{socket=Socket, transport=Transport, config=#ts_config{handler_module=HandlerModule, stats_module=StatsModule}}, Function, Reason) ->
  StatsModule =/= undefined andalso
    StatsModule:handle_stats(Function, [ {connect_time, timer:now_diff(os:timestamp(), State#ts_state.connect_time)}
                                       , {call_count, State#ts_state.call_count} ]),
  HandlerModule:handle_error(Function, Reason),
  Transport:close(Socket).


-spec decode(Config::#ts_config{}, ProtocolModule::atom(), RequestData::binary()) -> Result::tuple().
decode (#ts_config{service_module=ServiceModule, codec_config=CodecConfig, stats_module=StatsModule}, ProtocolModule, RequestData) ->
  Stats0 = collect_stats(StatsModule),
  Result = ProtocolModule:decode_message(ServiceModule, CodecConfig, RequestData),
  case Stats0 of
    undefined -> ok;
    {TS0, Reductions0} ->
      {TS1, Reductions1} = collect_stats(StatsModule),
      Function = element(1, Result),
      StatsModule:handle_stats(Function, [ {decode_time, timer:now_diff(TS1, TS0)}
                                         , {decode_size, size(RequestData)}
                                         , {decode_reductions, Reductions1 - Reductions0} ])
  end,
  Result.


-spec encode(Config::#ts_config{}, ProtocolModule::atom(), Function::atom(), MessageType::message_type(), Seqid::integer(), Args::term()) -> ReplyData::iolist().
encode (#ts_config{service_module=ServiceModule, stats_module=StatsModule}, ProtocolModule, Function, MessageType, SeqId, Args) ->
  Stats0 = collect_stats(StatsModule),
  Result = ProtocolModule:encode_message(ServiceModule, Function, MessageType, SeqId, Args),
  case Stats0 of
    undefined -> ok;
    {TS0, Reductions0} ->
      {TS1, Reductions1} = collect_stats(StatsModule),
      StatsModule:handle_stats(Function, [ {encode_time, timer:now_diff(TS1, TS0)}
                                         , {encode_size, iolist_size(Result)}
                                         , {encode_reductions, Reductions1 - Reductions0} ])
  end,
  Result.


-spec detect_protocol(Buffer::binary(), Default::atom()) -> atom().
detect_protocol (<<16#82, _/binary>>, _Default)       -> ox_thrift_protocol_compact;
detect_protocol (<<16#80, 16#1, _/binary>>, _Default) -> ox_thrift_protocol_binary; %% binary version 1
detect_protocol (<<0, 0, _/binary>>, _Default)        -> ox_thrift_protocol_binary; %% binary version 0
detect_protocol (_, Default)                          -> Default.


collect_stats (undefined) -> undefined;
collect_stats (_)         -> TS = os:timestamp(),
                             {reductions, Reductions} = process_info(self(), reductions),
                             {TS, Reductions}.
