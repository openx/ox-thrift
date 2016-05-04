-module(socket_transport).

-include("ox_thrift.hrl").
-include("../src/ox_thrift_internal.hrl").

-export([ send/2, recv/2, close/1, make_get_socket/1, start_server/4, stop_server/0 ]).

-define(LOCALHOST, "127.0.0.1").

send (Socket, Data) ->
  DataBin = list_to_binary(Data),
  X = gen_tcp:send(Socket, DataBin),
  ?LOG("send(~p,~p) -> ~p\n", [ Socket, DataBin, X ]),
  X.

recv (Socket, Length) ->
  X = gen_tcp:recv(Socket, Length),
  ?LOG("recv(~p,~p) -> ~p\n", [ Socket, Length, X ]),
  X.

close (Socket) ->
  gen_tcp:close(Socket).

make_get_socket (Port) ->
  fun () ->
      case gen_tcp:connect(?LOCALHOST, Port, [ binary, {active, false}, {packet, raw} ]) of
        {ok, Socket} -> Socket;
        {error, Reason} -> error({connect, ?LOCALHOST, Port, Reason})
      end
  end.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-define(SERVICE_REF, test_service).

start_server (Port, ServiceModule, CodecModule, HandlerModule) ->
  case application:start(ranch) of
    ok                              -> ok;
    {error,{already_started,ranch}} -> ok;
    Error                           -> error(Error)
  end,

  Config = #ox_thrift_config{
              service_module = ServiceModule,
              codec_module = CodecModule,
              handler_module = HandlerModule},
  case ranch:start_listener(?SERVICE_REF, 2, ranch_tcp, [ {port, Port} ], ox_thrift_server, Config) of
    {ok, _} -> ok;
    {error,{already_started,_}} -> ok
    end.

stop_server () ->
  ranch:stop_listener(?SERVICE_REF).
