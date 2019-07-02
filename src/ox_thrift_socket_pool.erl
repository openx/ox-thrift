%% Copyright 2016-2018, OpenX.  All rights reserved.
%% Licensed under the conditions specified in the accompanying LICENSE file.

%% @doc A connection pool for ox-thrift.
%% You are intended to link this gen_server into your supervisor tree by calling {@link start_link/4}.
%%
%% It should be OK for `ox_thrift_socket_pool''s supervisor to use a
%% `one_for_one' restart strategy, at least with respect to
%% `ox_thrift_socket_pool'.  If another process that has a socket checked out
%% crashes, `ox_thrift_socket_pool' will notice and drop those sockets from
%% the pool.  if `ox_thrift_socket_pool' crashes, another process will crash
%% when it tries to check a socket out of or in to the pool.

-module(ox_thrift_socket_pool).

-behaviour(gen_server).
-behaviour(ox_thrift_connection).

-export([ start_link/4, transfer/3, stats/1             % API.
        , print_stats/3, print_stats/2, print_stats/1
        ]).
-export([ new/1, destroy/1, checkout/1, checkin/3 ]).   % ox_thrift_connection callbacks.
-export([ init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3 ]). % gen_server callbacks.
-export([ get_state/1, get_connection/2 ]).             % Debugging.

-define(INFINITY, 1000000).
-define(M, 1000000).
-define(K, 1000).

-define(MAX_ASYNC_OPENS_DEFAULT, 10).

%% Macros for logging messages in a standard format.
-define(THRIFT_LOG_MSG(LogFunc, Format),
        LogFunc("~s:~B - " ++ Format, [ ?MODULE, ?LINE ])).
-define(THRIFT_LOG_MSG(LogFunc, Format, Args),
        LogFunc("~s:~B - " ++ Format, [ ?MODULE, ?LINE | Args ])).
-define(THRIFT_ERROR_MSG(Format),
        ?THRIFT_LOG_MSG(error_logger:error_msg, Format)).
-define(THRIFT_ERROR_MSG(Format, Args),
        ?THRIFT_LOG_MSG(error_logger:error_msg, Format, Args)).

-ifdef(DEBUG_CONNECTIONS).
-define(MONITOR_QUEUE_SIZE, 200).
monitor_queue_new () -> queue:from_list(lists:duplicate(?MONITOR_QUEUE_SIZE, undefined)).
-define(MONITOR_QUEUE_ADD(Item, Queue), queue:in(Item, queue:drop(Queue))).
-else. %% ! DEBUG_CONNECTIONS
monitor_queue_new () -> undefined.
-define(MONITOR_QUEUE_ADD(Item, Queue), Queue).
-endif. %% ! DEBUG_CONNECTIONS
monitor_queue_to_list (undefined) -> [];
monitor_queue_to_list (Queue)     -> queue:to_list(Queue).


-ifdef(MONDEMAND_PROGID).
-define(MONDEMAND_INCREMENT(Stat), mondemand:increment(?MONDEMAND_PROGID, Stat, 1)).
-define(MONDEMAND_ENABLED, true).
-else. %% ! MONDEMAND_PROGID
-define(MONDEMAND_INCREMENT(Stat), ok).
-define(MONDEMAND_ENABLED, false).
-endif. %% ! MONDEMAND_PROGID

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% Macros which provide an identical interface to maps and dicts.

-ifdef(OXTHRIFT_NO_MAPS).

-type map_type() :: dict:dict().
-define(MAPS_NEW(), dict:new()).
-define(MAPS_GET(Key, Dict), dict:fetch(Key, Dict)).
-define(MAPS_GET(Key, Dict, Default), dict_get(Key, Dict, Default)).
-define(MAPS_PUT(Key, Value, Dict1), dict:store(Key, Value, Dict1)).
-define(MAPS_UPDATE(Key, Value, Dict1), dict:store(Key, Value, Dict1)). %% Does not fail if Key does not exist.
-define(MAPS_REMOVE(Key, Dict1), dict:erase(Key, Dict1)).
-define(MAPS_FOLD(Fun, Init, Dict), dict:fold(Fun, Init, Dict)).
dict_get(Key, Dict, Default) -> case dict:find(Key, Dict) of {ok, Value} -> Value; error -> Default end.

-else. %% ! OXTHRIFT_NO_MAPS

-type map_type() :: map().
-define(MAPS_NEW(), #{}).
-define(MAPS_GET(Key, Map), maps:get(Key, Map)).
-define(MAPS_GET(Key, Map, Default), maps:get(Key, Map, Default)).
-define(MAPS_PUT(Key, Value, Map1), Map1#{Key => Value}).
-define(MAPS_UPDATE(Key, Value, Map1), Map1#{Key := Value}).
-define(MAPS_REMOVE(Key, Map1), maps:remove(Key, Map1)).
-define(MAPS_FOLD(Fun, Init, Map), maps:fold(Fun, Init, Map)).

-endif. %% ! OXTHRIFT_NO_MAPS

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-record(state, {
          id = error({required, id}) :: atom(),
          host = error({required, host}) :: inet:hostname() | inet:ip_address(),
          port = error({required, port}) :: inet:port_number(),
          connect_timeout_ms = infinity :: pos_integer() | 'infinity',
          max_age_ms = infinity :: pos_integer() | 'infinity',
          max_age_jitter_ms :: non_neg_integer() | 'undefined',
          max_connections = infinity :: pos_integer() | 'infinity',
          max_async_opens = ?MAX_ASYNC_OPENS_DEFAULT :: non_neg_integer(),
          remaining_connections = ?INFINITY :: non_neg_integer(),
          remaining_async_opens = ?MAX_ASYNC_OPENS_DEFAULT :: non_neg_integer(),
          idle = 0 :: non_neg_integer(),
          busy = 0 :: non_neg_integer(),
          checkout = 0 :: non_neg_integer(),
          close_local = 0 :: non_neg_integer(),
          close_remote = 0 :: non_neg_integer(),
          close_die = 0 :: non_neg_integer(),
          error_pool_full = 0 :: non_neg_integer(),
          error_connect = 0 :: non_neg_integer(),
          error_socket_not_found = 0 :: non_neg_integer(),
          idle_queue = undefined :: queue:queue() | 'undefined',
          monitor_queue = undefined :: queue:queue() | 'undefined', %% DEBUG_CONNECTIONS
          connections = undefined :: map_type() | 'undefined'}).


-spec start_link(Id::atom(), Host::inet:hostname(), Port::inet:port_number(), Options::list()) ->
                    {'ok', Pid::pid()} | 'ignore' | {'error', Error::term()}.
%% @doc Creates a gen_server to manage a socket pool.
%%
%% `Id' is an ID used to identify this socket pool in subsequent calls to
%% `checkout', `checkin', `destroy' and `transfer'.
%%
%% `Host' and `Port' identify the server that the socket pool connects to.
%%
%% `Options' is a list of options.
%%
%% `{connect_timeout_ms, Milliseconds}' specifies how long a client can spend
%% trying to establish a connection to the server.
%%
%% `{max_age_seconds, Seconds}' specifies how old a connection may be before
%% it is closed.
%%
%% `{max_age_jitter_ms, Milliseconds}' specifies a random extra amount of time
%% that is added to `max_age_seconds' so that when a burst of traffic causes a
%% lot of connections to be opened in a short period of time those connections
%% are not all also closed at the same time..  It defaults to 20% of
%% `max_age_seconds' if not specified.  You can specify a value of 0 if you
%% don't want any jitter added.
%%
%% `{max_connections, Count}' specifies the maximum size of the connection
%% pool.  If Count sockets are already checked out, calls to `checkout' will
%% fail until until a socket is checked in.
%%
%% `{max_async_opens, Count}' limits the number of simultaneous opens the pool
%% will attempt.  When the limit is reached, the pool will fall back to
%% opening new connections synchronously.  This is useful to allow a few new
%% connections to be opened quickly when there is a small burst of requests
%% but to avoid overloading the remote server by a large burst.  The default
%% `max_async_opens' is 10, and asynchronous opens may be disabled by setting
%% `max_async_opens' to 0.
start_link (Id, Host, Port, Options) ->
  State0 = #state{id = Id, host = Host, port = Port},
  State1 = parse_options(Options, State0),
  %% We're using ets:update_counter to maintain max_connections, so translate
  %% 'infinity' to a large number.
  RemainingConnections =
    case State1#state.max_connections of
      infinity       -> ?INFINITY;
      MaxConnections -> MaxConnections
    end,
  MaxAgeJitterMS =
    case State1 of
      #state{max_age_ms=infinity}                        -> undefined;
      #state{max_age_ms=MA, max_age_jitter_ms=undefined} -> round(MA * 0.10);
      #state{max_age_jitter_ms=MAJ}                      -> MAJ
    end,
  State = State1#state{remaining_connections = RemainingConnections,
                       remaining_async_opens = State1#state.max_async_opens,
                       max_age_jitter_ms = MaxAgeJitterMS},

  gen_server:start_link({local, Id}, ?MODULE, State, []).

%% @doc Transfers control of a checked-out socket to a new process.
%% This socket will no longer be managed by the pool, and will not count
%% towards the pool's `max_connections'.
-spec transfer(Id::atom(), Socket::gen_tcp:socket(), NewOwner::pid()) -> 'ok' | {'error', Reason::term()}.
transfer (Id, Socket, NewOwner) ->
  gen_server:call(Id, {transfer, Socket, NewOwner}).

%% @doc Returns stats as a proplist
%% `idle': the number of idle connections.
%% `busy': the number of busy connections.
%% `checkout': the number of checkout calls.
%% `open': the number of new connections opened.
%% `close_local': the number of times a connection was closed locally
%%    because the maximum age was reached.
%% `close_remote': the number of times a connection was closed by the remote
%%   end or a communication error occurred.
%% `close_die': the number of times a connection was closed because the
%%    process that had it checked out died
%% `error_pool_full': the number of times the pool failed to provide a
%%    connection because the `max_connections' limit has been reached.
%% `error_connect': the number of times the pool failed to provide a
%%    connection because the socket open failed.
-spec stats(Id::atom()) -> list({atom(), integer()}).
stats (Id) ->
  gen_server:call(Id, stats).

print_stats (Id, DelaySec, Count)
  when is_integer(DelaySec) andalso DelaySec >= 1 andalso
       ((is_integer(Count) andalso Count >= 1) orelse Count =:= infinity) ->
  print_stats_internal(Id, erlang:monotonic_time(),
                       erlang:convert_time_unit(DelaySec, second, native),
                       subtract_one(Count)).

print_stats (Id, DelaySec) -> print_stats(Id, DelaySec, infinity).

print_stats (Id) -> print_stats(Id, 1, infinity).


-record(connection, {
          socket = error({required, socket}) :: inet:socket() | reference(),
          lifetime_epoch_ms = error({required, lifetime_epoch_ms}) :: non_neg_integer() | 'infinity',
          monitor_ref :: reference() | 'undefined',
          owner :: pid() | 'undefined',
          use_count = 0 :: non_neg_integer()}).

-record(open_start, {
          reference = error({required, reference}) :: reference(),
          host = error({required, host}) :: inet:hostname() | inet:ip_address(),
          port = error({required, port}) :: inet:port_number(),
          connect_timeout_ms = infinity :: pos_integer() | 'infinity',
          pool_pid = error({required, pool_pid}) :: pid()}).

-record(open_complete, {
          reference = error({required, reference}) :: reference(),
          socket :: inet:socket() | 'error'}).


-spec new({Id::atom(), Host::inet:hostname(), Port::inet:port_number(), Options::list()}) -> Id::atom().
%% @doc Creates a new socket pool.
%% This function is a thin wrapper around {@link start_link/4}.
new ({Id, Host, Port, Options}) ->
  {ok, _} = start_link(Id, Host, Port, Options),
  Id.


parse_options ([], State) ->
  State;
parse_options ([ {connect_timeout_ms, ConnectTimeout} | Rest ], State)
  when is_integer(ConnectTimeout), ConnectTimeout > 0; ConnectTimeout =:= infinity ->
  parse_options(Rest, State#state{connect_timeout_ms = ConnectTimeout});
parse_options ([ {max_age_seconds, MaxAge} | Rest ], State)
  when is_integer(MaxAge), MaxAge > 0; MaxAge =:= infinity ->
  parse_options(Rest, State#state{max_age_ms = MaxAge * ?K});
parse_options ([ {max_age_jitter_ms, JitterMS} | Rest ], State)
  when is_integer(JitterMS), JitterMS > 0 ->
  parse_options(Rest, State#state{max_age_jitter_ms = JitterMS});
parse_options ([ {max_connections, MaxConnections} | Rest ], State)
  when is_integer(MaxConnections), MaxConnections > 0; MaxConnections =:= infinity ->
  parse_options(Rest, State#state{max_connections = MaxConnections});
parse_options ([ {max_async_opens, MaxAsyncOpens} | Rest ], State)
  when is_integer(MaxAsyncOpens), MaxAsyncOpens >= 0 ->
  parse_options(Rest, State#state{max_async_opens = MaxAsyncOpens}).


-spec destroy(Id::atom()) -> 'ok'.
%% @doc Destroys the socket pool.
%% All open connections will be closed.
destroy (Id) ->
  gen_server:call(Id, stop).


-spec checkout(Id::atom()) -> {'ok', Connection::gen_tcp:socket()} | {'error', Reason::term()}.
%% @doc Borrows a socket from the socket pool.
%%
%% Returns either `{ok, Socket}' or `{error, Reason}'.  Reason may be any of
%% the error codes returned by `gen_tcp:open', or `busy' if the
%% `max_connections' limit has been reached.
%%
%% Implementation notes.  The maximum age is only enforced on checkin.  When a
%% socket is checked out of the connection pool, its age is not checked; it is
%% used even if its age exceeds the pool's maximum age.  The pool works this
%% way to avoid the delay that could happen if there were a large number of
%% old connections in the pool.  The pool is implemented as a FIFO queue so
%% that old connections will be quickly removed the pool if is actively being
%% used.
checkout (Id) ->
  case gen_server:call(Id, checkout) of
    #open_start{reference=Ref, host=Host, port=Port, connect_timeout_ms=ConnectTimeoutMS, pool_pid = PoolPid} ->
      case gen_tcp:connect(Host, Port, [ binary, {active, false}, {packet, raw}, {nodelay, true} ], ConnectTimeoutMS) of
        Success={ok, Socket} ->
          ok = gen_tcp:controlling_process(Socket, PoolPid),
          gen_server:cast(Id, #open_complete{reference = Ref, socket = Socket}),
          Success;
        Error ->
          gen_server:cast(Id, #open_complete{reference = Ref, socket = error}),
          Error
      end;
    OKOrErrorReply={OKOrError, _} when OKOrError =:= ok; OKOrError =:= error ->
      OKOrErrorReply
  end.


-spec checkin(Id::atom(), Socket::gen_tcp:socket(), Status::ox_thrift_connection:status()) -> Id::atom().
%% @doc Returns a socket to the socket pool.
%%
%% The Status is either `ok' to indicate that it is OK to return the socket to
%% the pool to be reused by a subsequence `checkout' call, or `close' if the
%% socket should be closed.
checkin (Id, Socket, Status) when is_port(Socket) ->
  gen_server:cast(Id, {checkin, Socket, Status, self()}),
  Id.


-spec get_state(Id::atom()) -> #state{}.
%% @hidden Returns the state record.
get_state (Id) ->
  gen_server:call(Id, get_state).


-spec get_connection(Id::atom(), Socket::inet:socket()) -> #connection{} | 'undefined'.
%% @hidden Returns the connection record for a Socket.
get_connection (Id, Socket) ->
  gen_server:call(Id, {get_connection, Socket}).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% gen_server callbacks.

%% @hidden
init (State=#state{}) ->
  {ok, State#state{idle_queue = queue:new(), monitor_queue = monitor_queue_new(), connections = ?MAPS_NEW()}}.

%% @hidden
handle_call (checkout, {CallerPid, _Tag}, State=#state{idle_queue=IdleQueue}) ->
  {Reply, StateOut} =
    case queue:is_empty(IdleQueue) of
      false -> get_idle(CallerPid, State);
      true  -> open_start(CallerPid, State)
    end,
  {reply, Reply, StateOut};
handle_call (get_state, _From, State) ->
  {reply, State#state{idle_queue = undefined, monitor_queue = undefined, connections = undefined}, State};
handle_call ({get_connection, Socket}, _From, State=#state{connections=Connections}) ->
  Connection = ?MAPS_GET(Socket, Connections, undefined),
  {reply, Connection, State};
handle_call (stop, _From, State) ->
  {stop, normal, ok, State};
%% returns idle/busy/unavailable stats
handle_call (stats, _From, State=#state{error_pool_full=ErrorPoolFull, error_connect=ErrorConnect}) ->
    {reply, [ {epoch_us, erlang:convert_time_unit(erlang:system_time(), native, microsecond)},
              {idle, State#state.idle},
              {busy, State#state.busy},
              {checkout, State#state.checkout},
              {open, State#state.idle + State#state.busy +
                 State#state.close_local + State#state.close_remote + State#state.close_die +
                 State#state.error_connect},
              {close_local, State#state.close_local},
              {close_remote, State#state.close_remote},
              {close_die, State#state.close_die},
              {error_pool_full, ErrorPoolFull},
              {error_connect_error, ErrorConnect},
              {error_socket_not_found, State#state.error_socket_not_found},
              {unavailable, ErrorPoolFull + ErrorConnect}
            ],
            State};
%% Transfers control of a checked-out socket to a new owner.
handle_call ({transfer, Socket, NewOwner}, _From, State=#state{monitor_queue=MonitorQueue, connections=Connections}) ->
  %% Stop monitoring the process that checked the socket out, so we won't
  %% close the socket if that process dies.
  #connection{monitor_ref=MonitorRef} = ?MAPS_GET(Socket, Connections),
  demonitor(MonitorRef, [ flush ]),
  MonitorQueueOut = ?MONITOR_QUEUE_ADD({'demonitor_transfer', MonitorRef, _From, Socket, erlang:monotonic_time()}, MonitorQueue),
  %% Stop considering the socket in the pool's accounting.
  StateOut = close(Socket, do_not_close, State#state{monitor_queue = MonitorQueueOut}),
  %% Make NewOwner be the socket's controlling process.
  Reply = gen_tcp:controlling_process(Socket, NewOwner),
  {reply, Reply, StateOut};
handle_call (Request, _From, State) ->
  {reply, {unknown_call, Request}, State}.

%% @hidden
handle_cast ({checkin, Socket, Status, Pid}, State) ->
  StateOut = maybe_put_idle(Socket, Status, Pid, State),
  {noreply, StateOut};
%% @hidden
handle_cast(OpenComplete=#open_complete{}, State) ->
  StateOut = open_complete(OpenComplete, State),
  {noreply, StateOut};
handle_cast (_, State) ->
  {noreply, State}.

%% @hidden
handle_info (_DownMsg={'DOWN', MonitorRef, process, _Pid, _Info}, State=#state{monitor_queue=MonitorQueue, connections=Connections}) ->
  %% A process that had checked out a socket died.  Update the connection
  %% accounting so that we do not leak connections.

  %% The monitor_ref is not the key, so search the connections map to find the
  %% matching connection record and then close the associated socket.
  Socket = ?MAPS_FOLD(
              fun (S, #connection{monitor_ref=M}, _Acc) when M =:= MonitorRef -> S;
                  (_S, _C, Acc)                                               -> Acc
              end, not_found, Connections),
  StateOut =
    case Socket of
      not_found ->
        %% This should never happen.
        MonitorQueueOut = ?MONITOR_QUEUE_ADD({'down_missing', MonitorRef, _Pid, Socket, erlang:monotonic_time()}, MonitorQueue),
        ?THRIFT_ERROR_MSG("Socket not found\n  ~p\n  ~p\n  ~p\n",
                          [ _DownMsg, State#state{monitor_queue = undefined}, monitor_queue_to_list(MonitorQueueOut) ]),
        ?MONDEMAND_INCREMENT(socket_not_found),
        State#state{error_socket_not_found = State#state.error_socket_not_found + 1,
                    monitor_queue = MonitorQueueOut};
      _ ->
        %% Close and forget the socket.
        MonitorQueueOut = ?MONITOR_QUEUE_ADD({'down_close', MonitorRef, _Pid, Socket, erlang:monotonic_time()}, MonitorQueue),
        close(Socket, close_die, State#state{monitor_queue = MonitorQueueOut})
    end,
  {noreply, StateOut};
handle_info (_, State) ->
  {noreply, State}.

%% @hidden
terminate (_Reason, _State) ->
  ok.

%% @hidden
code_change (_OldVsn, State, _Extra) ->
  {ok, State}.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

get_idle (CallerPid, State=#state{idle=Idle, busy=Busy, idle_queue=IdleQueue, monitor_queue=MonitorQueue, connections=Connections}) ->
  {{value, Socket}, IdleQueueOut} = queue:out(IdleQueue),
  MonitorRef = monitor(process, CallerPid),
  MonitorQueueOut = ?MONITOR_QUEUE_ADD({'monitor_idle', MonitorRef, CallerPid, Socket, erlang:monotonic_time()}, MonitorQueue),
  Connection=#connection{use_count=UseCount} = ?MAPS_GET(Socket, Connections),
  ConnectionOut = Connection#connection{monitor_ref = MonitorRef, owner = CallerPid, use_count = UseCount + 1},
  ConnectionsOut = ?MAPS_UPDATE(Socket, ConnectionOut, Connections),
  {{ok, Socket}, State#state{idle = Idle - 1, busy = Busy + 1, checkout = State#state.checkout + 1,
                             idle_queue = IdleQueueOut, monitor_queue = MonitorQueueOut, connections = ConnectionsOut}}.


maybe_put_idle (Socket, Status, CallerPid, State=#state{monitor_queue=MonitorQueue, connections=Connections}) ->
  case ?MAPS_GET(Socket, Connections, undefined) of
    Connection=#connection{lifetime_epoch_ms=LifetimeEpochMS, monitor_ref=MonitorRef, owner=CallerPid} ->
      demonitor(MonitorRef, [ flush ]),
      MonitorQueueOut = ?MONITOR_QUEUE_ADD({'demonitor_checkin', MonitorRef, CallerPid, Socket, erlang:monotonic_time()}, MonitorQueue),
      ConnectionsOut = ?MAPS_UPDATE(Socket, Connection#connection{monitor_ref = undefined, owner = undefined}, Connections),
      StateOut = State#state{monitor_queue = MonitorQueueOut, connections = ConnectionsOut},

      if Status =/= ok                  -> close(Socket, close_remote, StateOut);
         is_integer(LifetimeEpochMS) ->
          NowMS = now_ms(os:timestamp()),
          if NowMS >= LifetimeEpochMS   -> close(Socket, close_local, StateOut);
             true                       -> put_idle(Socket, StateOut)
          end;
         true                           -> put_idle(Socket, StateOut)
      end;
    undefined ->
      %% This should never happen.
      MonitorQueueOut = ?MONITOR_QUEUE_ADD({'demonitor_checkin_missing', undefined, CallerPid, Socket, erlang:monotonic_time()}, MonitorQueue),
      ?THRIFT_ERROR_MSG("Socket not found\n  ~p\n  ~p\n",
                        [ State#state{monitor_queue = undefined}, monitor_queue_to_list(MonitorQueueOut) ]),
      ?MONDEMAND_INCREMENT(socket_not_found),
      State#state{error_socket_not_found = State#state.error_socket_not_found + 1,
                  monitor_queue = MonitorQueueOut}
  end.


put_idle (Socket, State=#state{idle=Idle, busy=Busy, idle_queue=IdleQueue}) ->
  IdleQueueOut = queue:in(Socket, IdleQueue),
  State#state{idle = Idle + 1, busy = Busy - 1, idle_queue = IdleQueueOut}.


-spec open_start(CallerPid::pid(), State::#state{}) -> {({'ok', Socket::inet:socket()} | {'error', Reason::term()}), #state{}}.
open_start (CallerPid, State) ->
  case State of
    #state{remaining_connections=0} ->
      {{error, busy}, State#state{checkout = State#state.checkout + 1, error_pool_full=State#state.error_pool_full + 1}}; % ERROR RETURN
    #state{remaining_connections=RemainingConnections, remaining_async_opens=RemainingAsyncOpensIn, busy=Busy,
           host=Host, port=Port, connect_timeout_ms=ConnectTimeoutMS, max_age_ms=MaxAgeMS, max_age_jitter_ms=MaxJitterMS,
           monitor_queue=MonitorQueue, connections=Connections}
      when RemainingConnections > 0 ->

      case
        RemainingAsyncOpensIn =:= 0 andalso
        gen_tcp:connect(Host, Port, [ binary, {active, false}, {packet, raw}, {nodelay, true} ], ConnectTimeoutMS)
      of
        Error={error, _} ->
          %% We attempted to open the connection locally but it failed.
          {Error, State#state{checkout = State#state.checkout + 1, error_connect = State#state.error_connect + 1}}; % ERROR RETURN
        SuccessOrFalse ->
          %% Either the socket has been successfully opened locally or it will
          %% be opened in the client.
          MonitorRef = monitor(process, CallerPid),
          case SuccessOrFalse of
            Success={ok, Socket} ->
              %% The socket was successfuly opened locally.
              Reply = Success,
              SocketOrMonitorRef = Socket,
              MonitorQueueOut = ?MONITOR_QUEUE_ADD({'monitor_open_server', MonitorRef, CallerPid, Socket, erlang:monotonic_time()}, MonitorQueue),
              RemainingAsyncOpensOut = RemainingAsyncOpensIn;
            false ->
              %% Update our state as if the connection was opened, but leave
              %% the actual opening of the connection to the client so that it
              %% doesn't block other requests.  Once the client has opened the
              %% socket open_complete will be called to finalize the state.
              Reply = #open_start{reference = MonitorRef, host = Host, port = Port, connect_timeout_ms = ConnectTimeoutMS, pool_pid = self()},
              SocketOrMonitorRef = MonitorRef,
              MonitorQueueOut = ?MONITOR_QUEUE_ADD({'monitor_open_client', MonitorRef, CallerPid, client_open, erlang:monotonic_time()}, MonitorQueue),
              RemainingAsyncOpensOut = RemainingAsyncOpensIn - 1
            end,
          LifetimeEpochMS = case MaxAgeMS of
                              infinity -> infinity;
                              _        -> JitterMS = case MaxJitterMS of 0 -> 0; _ -> rand:uniform(MaxJitterMS + 1) - 1 end,
                                          now_ms(os:timestamp()) + MaxAgeMS + JitterMS
                            end,
          ConnectionOut = #connection{socket = SocketOrMonitorRef, lifetime_epoch_ms = LifetimeEpochMS,
                                      monitor_ref = MonitorRef, owner = CallerPid, use_count = 1},
          ConnectionsOut = ?MAPS_PUT(SocketOrMonitorRef, ConnectionOut, Connections),
          {Reply, State#state{checkout = State#state.checkout + 1, % SUCCESS RETURN
                              remaining_connections = RemainingConnections - 1,
                              remaining_async_opens = RemainingAsyncOpensOut,
                              busy = Busy + 1, monitor_queue = MonitorQueueOut, connections = ConnectionsOut}}
      end
  end.


-spec open_complete(#open_complete{}, #state{}) -> #state{}.
open_complete (#open_complete{reference=Ref, socket=Socket}, State=#state{connections=Connections}) ->
  Connection=#connection{monitor_ref=MonitorRef} = ?MAPS_GET(Ref, Connections),
  ConnectionsOut0 = ?MAPS_REMOVE(Ref, Connections),
  case Socket of
    error ->
      %% The open failed; remove the connection.
      demonitor(MonitorRef, [ flush ]),
      State#state{remaining_connections = State#state.remaining_connections + 1, busy = State#state.busy - 1,
                  error_connect = State#state.error_connect + 1, connections = ConnectionsOut0};
    _ ->
      %% The open succeeded; update the socket in the connection.
      ConnectionOut = Connection#connection{socket = Socket},
      ConnectionsOut1 = ?MAPS_PUT(Socket, ConnectionOut, ConnectionsOut0),
      State#state{connections = ConnectionsOut1}
  end.


-spec close(Socket::inet:socket() | reference(), CloseSocket, State::#state{}) -> #state{} when
    CloseSocket :: 'close_local' | 'close_remote' | 'close_die' | 'do_not_close'.
close (Socket, CloseSocket, State=#state{remaining_connections=Remaining, busy=Busy, connections=Connections}) ->
  %% The "Socket" may be a reference if it was being opened in the client.
  CloseSocket =/= do_not_close andalso is_port(Socket) andalso gen_tcp:close(Socket),
  ConnectionsOut = ?MAPS_REMOVE(Socket, Connections),
  State#state{remaining_connections = Remaining + 1, busy = Busy - 1,
              close_local = increment_if_match(State#state.close_local, CloseSocket, close_local),
              close_remote = increment_if_match(State#state.close_remote, CloseSocket, close_remote),
              close_die = increment_if_match(State#state.close_die, CloseSocket, close_die),
              connections = ConnectionsOut}.


increment_if_match (Value, A, A) -> Value + 1;
increment_if_match (Value, _, _) -> Value.


now_ms ({MegaSec, Sec, MicroSec}) ->
  (MegaSec * ?M + Sec) * ?K + MicroSec div ?K.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-define(EPOCH_BASE, 62167219200). %% calendar:datetime_to_gregorian_seconds({{1970, 1, 1}, {0, 0, 0}})

format_epoch (Epoch) ->
  {{Year, Month, Day}, {Hour, Minute, Second}} = calendar:gregorian_seconds_to_datetime(Epoch + ?EPOCH_BASE),
  io_lib:format("~4B-~2.10.0B-~2.10.0B ~2.10.0B:~2.10.0B:~2.10.0B", [ Year, Month, Day, Hour, Minute, Second ]).

subtract_one (I) when is_integer(I) -> I - 1;
subtract_one (infinity)             -> infinity.

print_stats_internal (Id, LastTS, DelayTime, Repeat) ->
  Stats = stats(Id),
  {_, EpochUS} = lists:keyfind(epoch_us, 1, Stats),
  io:format("~s\n  ~p\n", [ format_epoch(EpochUS div 1000000), Stats ]),
  if Repeat =:= 0 -> ok;
     true ->
      NextTS = LastTS + DelayTime,
      SleepMS = erlang:convert_time_unit(NextTS - erlang:monotonic_time(), native, millisecond),
      timer:sleep(SleepMS),
      print_stats_internal(Id, NextTS, DelayTime, subtract_one(Repeat))
  end.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

-define(TEST_PORT, 9999).
-define(TEST_ID, ox_thrift_test).

parse_options_test () ->
  BaseState = #state{id = test, host = "localhost", port = 12345},
  ?assertMatch(#state{connect_timeout_ms = infinity, max_age_ms = infinity, max_connections = infinity},
               parse_options([], BaseState)),
  ?assertMatch(#state{connect_timeout_ms = 42}, parse_options([ {connect_timeout_ms, 42} ], BaseState)),
  ?assertMatch(#state{max_age_ms = 117000}, parse_options([ {max_age_seconds, 117} ], BaseState)),
  ?assertMatch(#state{max_connections = 10}, parse_options([ {max_connections, 10} ], BaseState)),
  ?assertMatch(#state{max_connections = infinity}, parse_options([ {max_connections, infinity} ], BaseState)),
  ?assertMatch(#state{max_async_opens = 10}, parse_options([ {max_async_opens, 10} ], BaseState)),
  ok.

now_ms_test () ->
  ?assertEqual(1002003004, now_ms({1, 002003, 004005})).

start_stop_test () ->
  ?TEST_ID = new({?TEST_ID, "localhost", ?TEST_PORT, []}),
  ok = destroy(?TEST_ID).


error_test () ->
  ?TEST_ID = new({?TEST_ID, "localhost", ?TEST_PORT, []}),
  ?assertMatch({error, econnrefused}, checkout(?TEST_ID)),
  ok = destroy(?TEST_ID).


echo (Port, EchoCounts) ->
  {ok, LSock} = gen_tcp:listen(Port, [ binary, {active, false}, {reuseaddr, true} ]),
  echo_accepter(EchoCounts, LSock),
  ok = gen_tcp:close(LSock).

echo_accepter ([], LSock) ->
  ok = gen_tcp:close(LSock);
echo_accepter ([ FirstCount | RestCounts ], LSock) ->
  {ok, Sock} = gen_tcp:accept(LSock),
  ChildPid = spawn(fun () -> echo_echoer(FirstCount, Sock) end),
  %% Give ownership to the child so the socket won't be closed if we exit before the child does.
  ok = gen_tcp:controlling_process(Sock, ChildPid),
  echo_accepter(RestCounts, LSock).

echo_echoer (0, Sock) ->
  ok = gen_tcp:close(Sock);
echo_echoer (-1, Sock) ->
  %% We use a count of -1 in the transfer test to indicate that we want to
  %% attempt a receive, but we expect the connection to be closed on us.
  {error, closed} = gen_tcp:recv(Sock, 0),
  ok;
echo_echoer (Count, Sock) ->
  {ok, Data} = gen_tcp:recv(Sock, 0),
  ok = gen_tcp:send(Sock, Data),
  echo_echoer(Count - 1, Sock).


echo_test () ->
  EchoPid = spawn(fun () -> echo(?TEST_PORT, [ 2 ]) end),
  ?TEST_ID = new({?TEST_ID, "localhost", ?TEST_PORT, []}),

  {ok, Socket0} = checkout(?TEST_ID),
  DataOut = <<"hello">>,
  ok = gen_tcp:send(Socket0, DataOut),
  {ok, DataIn} = gen_tcp:recv(Socket0, 0),
  ?assertEqual(DataOut, DataIn),

  %% This is an 'ok' checkin, so the socket goes back into the pool.
  checkin(?TEST_ID, Socket0, ok),
  Conn0 = get_connection(?TEST_ID, Socket0),
  ?assertMatch(#connection{}, Conn0),
  ?assertEqual(1, Conn0#connection.use_count),

  %% This checkout should return the socket from the pool.
  {ok, Socket1} = checkout(?TEST_ID),
  ?assertEqual(Socket0, Socket1),
  DataOut1 = <<"world">>,
  ok = gen_tcp:send(Socket1, DataOut1),
  {ok, DataIn1} = gen_tcp:recv(Socket1, 0),
  ?assertEqual(DataOut1, DataIn1),
  Conn1 = get_connection(?TEST_ID, Socket1),
  ?assertEqual(2, Conn1#connection.use_count),

  %% This is an 'error' checkin, so the socket is closed.
  ?assertMatch(#state{close_local = 0, close_remote = 0}, get_state(?TEST_ID)),
  checkin(?TEST_ID, Socket1, close),
  ?assertEqual(undefined, get_connection(?TEST_ID, Socket1)),
  ?assertMatch(#state{close_local = 0, close_remote = 1}, get_state(?TEST_ID)),

  %% Check that checkin validates its arguments.
  ?assertError(function_clause, checkin(?TEST_ID, undefined, ok)),

  Stats = stats(?TEST_ID),
  ?assertMatch({_, 0}, lists:keyfind(idle, 1, Stats)),
  ?assertMatch({_, 0}, lists:keyfind(busy, 1, Stats)),
  ?assertMatch({_, 2}, lists:keyfind(checkout, 1, Stats)),
  ?assertMatch({_, 1}, lists:keyfind(open, 1, Stats)),
  ?assertMatch({_, 0}, lists:keyfind(close_local, 1, Stats)),
  ?assertMatch({_, 1}, lists:keyfind(close_remote, 1, Stats)),
  ?assertMatch({_, 0}, lists:keyfind(close_die, 1, Stats)),

  print_stats(?TEST_ID, 1, 2),

  ok = destroy(?TEST_ID),
  exit(EchoPid, kill).


timeout_test () ->
  EchoPid = spawn(fun () -> echo(?TEST_PORT, [ 0, 0 ]) end),
  ?TEST_ID = new({?TEST_ID, "localhost", ?TEST_PORT, [ {max_age_seconds, 1}, {max_connections, 2} ]}),

  ?assertMatch(#state{idle = 0, busy = 0, remaining_connections = 2}, get_state(?TEST_ID)),

  {ok, Socket0} = checkout(?TEST_ID),
  ?assertMatch(#state{idle = 0, busy = 1, remaining_connections = 1}, get_state(?TEST_ID)),
  ?assertMatch(#connection{use_count = 1}, get_connection(?TEST_ID, Socket0)),

  %% This checkin happened quickly, so the socket was returned to the pool.
  checkin(?TEST_ID, Socket0, ok),
  ?assertMatch(#state{idle = 1, busy = 0, remaining_connections = 1, close_local = 0}, get_state(?TEST_ID)),

  {ok, Socket1} = checkout(?TEST_ID),
  ?assertEqual(Socket0, Socket1),
  ?assertMatch(#state{idle = 0, busy = 1, remaining_connections = 1}, get_state(?TEST_ID)),
  Conn1 = get_connection(?TEST_ID, Socket1),
  ?assertEqual(2, Conn1#connection.use_count),
  timer:sleep(1200),

  %% This checkin happens after the max age, so the socket is closed.
  checkin(?TEST_ID, Socket1, ok),
  ?assertEqual(undefined, get_connection(?TEST_ID, Socket1)),
  ?assertMatch(#state{idle = 0, busy = 0, remaining_connections = 2, close_local = 1}, get_state(?TEST_ID)),

  %% This checkout gets a new socket.
  {ok, Socket2} = checkout(?TEST_ID),
  ?assertMatch(#state{idle = 0, busy = 1, remaining_connections = 1, close_local = 1}, get_state(?TEST_ID)),
  Conn2 = get_connection(?TEST_ID, Socket2),
  ?assertEqual(1, Conn2#connection.use_count),

  %% This checkin returns the socket to the pool.
  checkin(?TEST_ID, Socket2, ok),
  ?assertMatch(#state{idle = 1, busy = 0, remaining_connections = 1, close_local = 1}, get_state(?TEST_ID)),

  ok = destroy(?TEST_ID),
  exit(EchoPid, kill).


monitor_test () ->
  EchoPid = spawn(fun () -> echo(?TEST_PORT, [ 0 ]) end),
  ?TEST_ID = new({?TEST_ID, "localhost", ?TEST_PORT, [ {max_age_seconds, 1} ]}),

  %% Start a process that checks out a socket and then exits without checking
  %% it back in to ensure that the gen_server cleans up the connection.
  ?assertMatch(#state{idle = 0, busy = 0}, get_state(?TEST_ID)),
  spawn(fun () -> checkout(?TEST_ID), timer:sleep(50) end),
  timer:sleep(10),
  ?assertMatch(#state{idle = 0, busy = 1, close_die = 0}, get_state(?TEST_ID)),
  timer:sleep(100),
  ?assertMatch(#state{idle = 0, busy = 0, close_die = 1}, get_state(?TEST_ID)),

  ok = destroy(?TEST_ID),
  exit(EchoPid, kill).


-ifdef(MONDEMAND_PROGID).
mock_mondemand () ->
  %% Mock out mondemand:increment, using an ets table to track the values.
  TId = ets:new(md_debug, [ public ]),
  meck:new(mondemand),
  meck:expect(mondemand, increment, fun (?MONDEMAND_PROGID, Key, Inc) -> ets:update_counter(TId, Key, Inc, {Key, 0}) end),
  TId.
unmock_mondemand ()  -> meck:unload(mondemand).
-else. %% ! MONDEMAND_PROGID
mock_mondemand () -> undefined.
unmock_mondemand () -> ok.
-endif. %% ! MONDEMAND_PROGID


monitor_bad_down_test () ->
  TId = mock_mondemand(),

  EchoPid = spawn(fun () -> echo(?TEST_PORT, [ 0 ]) end),
  ?TEST_ID = new({?TEST_ID, "localhost", ?TEST_PORT, [ {max_age_seconds, 1} ]}),

  %% Send the pool a spurious DOWN message and see what happens.
  ?TEST_ID ! {'DOWN', make_ref(), process, self(), shutdown},
  timer:sleep(10),

  Stats = stats(?TEST_ID),
  ?assertMatch({_, 1}, lists:keyfind(error_socket_not_found, 1, Stats)),

  ok = destroy(?TEST_ID),
  exit(EchoPid, kill),

  ?MONDEMAND_ENABLED andalso
    ?assertMatch([ {_, 1} ], ets:lookup(TId, socket_not_found)),
  unmock_mondemand().


monitor_bad_checkin_test () ->
  TId = mock_mondemand(),

  EchoPid = spawn(fun () -> echo(?TEST_PORT, [ 0 ]) end),
  ?TEST_ID = new({?TEST_ID, "localhost", ?TEST_PORT, [ {max_age_seconds, 1} ]}),

  %% Check in a socket that was not checked out and see what happens.
  Port = open_port({spawn, <<"cat">>}, []),
  checkin(?TEST_ID, Port, ok),
  timer:sleep(10),
  port_close(Port),

  Stats = stats(?TEST_ID),
  ?assertMatch({_, 1}, lists:keyfind(error_socket_not_found, 1, Stats)),

  ok = destroy(?TEST_ID),
  exit(EchoPid, kill),

  ?MONDEMAND_ENABLED andalso
    ?assertMatch([ {_, 1} ], ets:lookup(TId, socket_not_found)),
  unmock_mondemand().


limit_test () ->
  EchoPid = spawn(fun () -> echo(?TEST_PORT, [ 0, 0 ]) end),
  ?TEST_ID = new({?TEST_ID, "localhost", ?TEST_PORT, [ {max_connections, 2} ]}),

  ?assertMatch(#state{idle = 0, busy = 0, remaining_connections = 2, error_pool_full = 0}, get_state(?TEST_ID)),
  {ok, Socket0} = checkout(?TEST_ID),
  ?assert(is_port(Socket0)),
  {ok, Socket1} = checkout(?TEST_ID),
  ?assert(is_port(Socket1)),
  ?assertMatch(#state{idle = 0, busy = 2, remaining_connections = 0, error_pool_full = 0}, get_state(?TEST_ID)),
  ?assertMatch({error, busy}, checkout(?TEST_ID)),
  ?assertMatch(#state{idle = 0, busy = 2, remaining_connections = 0, error_pool_full = 1}, get_state(?TEST_ID)),
  checkin(?TEST_ID, Socket0, ok),
  {ok, Socket2} = checkout(?TEST_ID),
  ?assert(is_port(Socket2)),
  ?assertMatch(#state{idle = 0, busy = 2, remaining_connections = 0, error_pool_full = 1}, get_state(?TEST_ID)),
  checkin(?TEST_ID, Socket0, ok),
  checkin(?TEST_ID, Socket1, ok),
  ?assertMatch(#state{idle = 2, busy = 0, remaining_connections = 0, error_pool_full = 1}, get_state(?TEST_ID)),

  ok = destroy(?TEST_ID),
  exit(EchoPid, kill).


transfer_test () ->
  EchoPid = spawn(fun () -> echo(?TEST_PORT, [ -1, 1 ]) end),
  ?TEST_ID = new({?TEST_ID, "localhost", ?TEST_PORT, [ {max_connections, 2} ]}),

  {ok, Socket0} = checkout(?TEST_ID),
  ?assert(is_port(Socket0)),
  {ok, Socket1} = checkout(?TEST_ID),
  ?assert(is_port(Socket1)),
  ?assertMatch(#state{idle = 0, busy = 2, remaining_connections = 0}, get_state(?TEST_ID)),
  ok = transfer(?TEST_ID, Socket1, self()),
  ?assertMatch(#state{idle = 0, busy = 1, remaining_connections = 1}, get_state(?TEST_ID)),

  ok = destroy(?TEST_ID),

  %% Check that the non-transfered socket is closed.
  Status =
    case gen_tcp:send(Socket0, <<"closed">>) of
      {error, closed}     -> send_closed;
      ok ->
        case gen_tcp:recv(Socket0, 0) of
          {error, closed} -> recv_closed;
          {ok, _}         -> open
        end
    end,
  ?assertNotEqual(open, Status),

  %% Check that the transfered socket is still open.
  DataOut1 = <<"transfer">>,
  ok = gen_tcp:send(Socket1, DataOut1),
  {ok, DataIn1} = gen_tcp:recv(Socket1, 0),
  ?assertEqual(DataOut1, DataIn1),

  exit(EchoPid, kill).

-endif. %% TEST
