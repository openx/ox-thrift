%% Copyright 2016-2018, OpenX.  All rights reserved.
%% Licensed under the conditions specified in the accompanying LICENSE file.

-module(ox_thrift_reconnecting_socket).

%% @doc A reconnecting TCP connection for ox-thrift.

-behaviour(ox_thrift_connection).

-export([ new/1, destroy/1, checkout/1, checkin/3 ]).   % ox_thrift_connection callbacks.
-export([ is_open/1 ]).                                 % Debugging.

-record(state, {
          host = error({required, host}) :: inet:hostname() | inet:ip_address(),
          port = error({required, port}) :: inet:port_number(),
          socket :: gen_tcp:socket() | 'undefined'}).


-spec new({Host::inet:hostname()|inet:ip_address(), Port::inet:port_number()}) -> State::#state{}.
new ({Host, Port}) ->
  #state{host = Host, port = Port}.


-spec destroy(State::#state{}) -> 'ok'.
destroy (State) ->
  case State#state.socket of
    undefined -> ok;
    Socket    -> gen_tcp:close(Socket)
  end.


-spec checkout(State::#state{}) -> {'ok', Connection::gen_tcp:socket()} | {'error', Reason::term()}.
checkout (State=#state{host=Host, port=Port}) ->
  case State#state.socket of
    undefined -> gen_tcp:connect(Host, Port, [ binary, {active, false}, {packet, raw}, {nodelay, true} ]);
    Socket    -> {ok, Socket}
  end.


-spec checkin(State::#state{}, Connection::gen_tcp:socket(), Status::ox_thrift_connection:status()) -> NewState::#state{}.
checkin (State, Socket, Status) ->
  case Status of
    ok    -> State#state{socket = Socket};
    close -> is_port(Socket) andalso gen_tcp:close(Socket),
             State#state{socket = undefined}
  end.

%% For unit test.
is_open (#state{socket = Socket}) ->
  Socket =/= undefined.
