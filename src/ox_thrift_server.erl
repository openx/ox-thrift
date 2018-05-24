%% Copyright 2016-2018 OpenX.  All rights reserved.
%% Licensed under the conditions specified in the accompanying LICENSE file.

-module(ox_thrift_server).

-include("ox_thrift.hrl").
-include("ox_thrift_internal.hrl").

-behaviour(ranch_protocol).

-export([ start_link/4, handle_request/2 ]).
-export([ init/4 ]).
-export([ parse_config/1 ]). %% Exported for unit tests.

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


start_link (Ref, Socket, Transport, Opts) ->
  Pid = spawn_link(?MODULE, init, [ Ref, Socket, Transport, Opts ]),
  {ok, Pid}.


init (Ref, Socket, Transport, Config) ->
  ?LOG("ox_thrift_server:init ~p ~p ~p ~p\n", [ Ref, Socket, Transport, Config ]),
  ok = ranch:accept_ack(Ref),
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
parse_options ([ {stats_module, StatsModule} | Options ], Config) when is_atom(StatsModule) ->
  parse_options(Options, Config#ts_config{stats_module = StatsModule});
parse_options ([], Config) ->
  Config.


loop (State=#ts_state{socket=Socket, transport=Transport, config=#ts_config{recv_timeout=RecvTimeout}}) ->
  %% Implement thrift_framed_transport.

  %% Read the length, and then the request data.
  %% Result0 will be `{ok, Packet}' or `{error, Reason}'.
  case Transport:recv(Socket, 4, RecvTimeout) of
    {ok, LengthBin} ->
      <<Length:32/integer-signed-big>> = LengthBin,
      loop1(State#ts_state{call_count = State#ts_state.call_count + 1}, Length);
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
      Reply = [ <<ReplyLen:32/big-signed>>, ReplyData ],
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


-spec handle_request(Config::#ts_config{}, RequestData::binary()) -> {Reply::iolist()|'noreply', Function::atom(), ReplyOptions::reply_options()}.
handle_request (Config=#ts_config{protocol_module=ProtocolModule, handler_module=HandlerModule}, RequestData) ->
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
        {encode(Config, Protocol, Function, reply_exception, SeqId, Reason), undefined};
      ErrorOrThrow:Reason when ErrorOrThrow =:= error; ErrorOrThrow =:= throw ->
        Message = ox_thrift_util:format_error_message(ErrorOrThrow, Reason),
        ExceptionReply = #application_exception{message = Message, type = ?tApplicationException_UNKNOWN},
        {encode(Config, Protocol, Function, exception, SeqId, ExceptionReply), undefined}
    end,

  %% ?LOG("handle_request -> ~p\n", [ {ResultMsg, Function } ]),
  {ResultMsg, Function, ReplyOptions}.


reply_options ({reply, Reply})          -> {Reply, undefined};
reply_options (ok)                      -> {undefined, undefined};
reply_options ({reply, Reply, Options}) -> {Reply, Options};
reply_options ({ok, Options})           -> {undefined, Options}.


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
