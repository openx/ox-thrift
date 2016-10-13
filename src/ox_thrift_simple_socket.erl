%% Copyright 2016, 2017, OpenX.  All rights reserved.
%% Licensed under the conditions specified in the accompanying LICENSE file.

-module(ox_thrift_simple_socket).

%% @doc A simple non-reconnecting TCP connection for ox-thrift.
%% This module is intended more as an example that implements the {@link
%% ox_thrift_connection} behaviour than as something that is truly useful.
%% See {@link ox_thrift_reconnecting_socket} for a ox_thrift_connection
%% implementation that attempts to reconnect the connection to the Thrift
%% server is lost.

-behaviour(ox_thrift_connection).

-export([ new/1, destroy/1, checkout/1, checkin/3 ]).


-spec new({Host::inet:hostname()|inet:ip_address(), Port::inet:port_number()}) -> State::gen_tcp:socket().
new ({Host, Port}) ->
  case gen_tcp:connect(Host, Port, [ binary, {active, false}, {packet, raw}, {nodelay, true} ]) of
    {ok, Socket}    -> Socket;
    {error, Reason} -> error({connect, Host, Port, Reason})
  end.


-spec destroy(State::gen_tcp:socket()) -> 'ok'.
destroy (Socket) ->
  gen_tcp:close(Socket).


-spec checkout(State::gen_tcp:socket()) -> {ok, Connection::gen_tcp:socket()}.
checkout (Socket) ->
  {ok, Socket}.


-spec checkin(State::gen_tcp:socket(), Connection::gen_tcp:socket(), Status::atom()) -> NewState::gen_tcp:socket().
checkin (_State, Socket, _Status) ->
  Socket.
