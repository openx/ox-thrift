%% Copyright 2016, OpenX.  All rights reserved.
%% Licensed under the conditions specified in the accompanying LICENSE file.

-module(ox_thrift_connection).

%%% Defines an interface to a connection manager for use with the OX Thrift
%%% client library.
%%%
%%% The code that sets up the Thrift client should do something like
%%%
%%% <code>
%%% {ok, ConnectionState} = ConnectionModule:new(Args),
%%% {ok, Client0} = ox_thrift_client:new(ConnectionModule, ConnectionState, Transport, Protocol, Service, []),
%%%
%%% {Client1, Result} = ox_thrift_client:call(Client0, Function, FunctionArgs),
%%%
%%% ConnectionModule:destroy(get_connection_state(Client1)),
%%% </code>
%%%
%%% The following code gives a sketch of how the OX Thrift client uses the
%%% connection.
%%%
%%% <code>
%%% {ok, Socket} = ConnectionModule:checkout(ConnectionState0),
%%% Transport:send(Socket, ThriftEncodedRequest),
%%% {ok, ThriftEncodedReply} = Transport:recv(Socket),
%%% ConnectionState1 = ConnectionModule:checkin(ConnectionState0, Socket, ok),
%%% </code>

-type connection_state() :: term().
-type connection() :: term().
-type status() :: 'ok' | 'close'.

-export_type([ connection_state/0, connection/0, status/0 ]).

%% Creates a new connection manager.
-callback new(Args::term()) ->
  State::connection_state().

%% Destroys a connection manager.
-callback destroy(State::connection_state()) ->
  ok.

%% Gets a connection, suitable for use with the OX Thrift transport, from
%% the connection manager.
-callback checkout(State::connection_state()) ->
  {ok, Connection::connection()} | {error, Reason::term()}.

%% Returns a connection to the connection manager.
-callback checkin(State::connection_state(),
                  Connection::connection(),
                  Status::status()) ->
  NewState::connection_state().
