%% Copyright 2016, OpenX.  All rights reserved.
%% Licensed under the conditions specified in the accompanying LICENSE file.

-module(socket_transport).

-include("ox_thrift.hrl").
-include("../src/ox_thrift_internal.hrl").

-export([ send/2, recv/3, close/1, make_get_socket/1, start_server/5, stop_server/0 ]).

-define(LOCALHOST, "127.0.0.1").

send (Socket, Data) ->
  DataBin = list_to_binary(Data),
  X = gen_tcp:send(Socket, DataBin),
  ?LOG("send(~p,~p) -> ~p\n", [ Socket, DataBin, X ]),
  X.

recv (Socket, Length, Timeout) ->
  X = gen_tcp:recv(Socket, Length, Timeout),
  ?LOG("recv(~p,~p) -> ~p\n", [ Socket, Length, X ]),
  X.

close (Socket) ->
  gen_tcp:close(Socket).

make_get_socket (Port) ->
  fun () ->
      case gen_tcp:connect(?LOCALHOST, Port, [ binary, {active, false}, {packet, raw}, {nodelay, true} ]) of
        {ok, Socket} -> Socket;
        {error, Reason} -> error({connect, ?LOCALHOST, Port, Reason})
      end
  end.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-define(SERVICE_REF, test_service).

start_application (Application) ->
  case application:start(Application) of
    ok                                    -> ok;
    {error,{already_started,Application}} -> ok;
    Error                                 -> error(Error)
  end.

start_server (Port, ServiceModule, ProtocolModule, HandlerModule, StatsModule) ->
  lists:foreach(fun start_application/1, [ asn1, public_key, ssl, ranch ]),

  Config = #ox_thrift_config{
              service_module = ServiceModule,
              protocol_module = ProtocolModule,
              handler_module = HandlerModule,
              options = [ {recv_timeout, 100}
                        , {stats_module, StatsModule} ] },
  case ranch:start_listener(?SERVICE_REF, 2, ranch_tcp, [ {port, Port} ], ox_thrift_server, Config) of
    {ok, _} -> ok;
    {error,{already_started,_}} -> ok
    end.

stop_server () ->
  ranch:stop_listener(?SERVICE_REF).
