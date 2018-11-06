%% Copyright 2016-2018 OpenX.  All rights reserved.
%% Licensed under the conditions specified in the accompanying LICENSE file.

-module(ox_thrift_client).

-include("ox_thrift.hrl").
-include("ox_thrift_internal.hrl").

-define(MAX_TRIES, 2).
-define(RECV_LIMIT, 60 * 1024 * 1024).          % gen_tcp:recv max length

-export([ new/5, new/6, get_connection_module/1, get_connection_state/1, get_seqid/1, call/3 ]).
-export_type([ ox_thrift_client/0 ]).


-record(ox_thrift_client, {
          connection_module = error({required, connection_module}) :: atom(),
          connection_state = error({required, connection_state}) :: ox_thrift_connection:connection_state(),
          transport_module = error({required, transport_module}) :: atom(),
          protocol_module = error({required, protocol_module}) :: atom(),
          service_module = error({required, service_module}) :: atom(),
          codec_config = #codec_config{} :: codec_config(),
          seqid = 0 :: integer(),
          recv_timeout = 'infinity' :: non_neg_integer() | 'infinity' }).
-type ox_thrift_client() :: #ox_thrift_client{}.

-define(REASON_IS_CLOSED(Error), (Error =:= closed orelse Error =:= enotconn)).


new (Connection, ConnectionState, Transport, Protocol, Service) ->
  new(Connection, ConnectionState, Transport, Protocol, Service, []).

-spec new(Connection::atom(), ConnectionState::term(),
          Transport::atom(), Protocol::atom(), Service::atom(),
          Options::list()) ->
             {ok, Client::ox_thrift_client()}.
%% @doc Returns a new client, to be used in subsequent calls to {@link call/3}.
%%
%% * Connection: A module that manages a connection to the Thrift server.
%%   This module must support the interface defined by {@link
%%   ox_thrift_connection}.
%% * ConnectionState: The initial state for the Connection module, returned by
%%   its `new' function.
%% * Transport: A module that provides the transport layer, such as
%%   {@link gen_tcp}.  This module is expected to export `send/2`, `recv/3`,
%%   and `close/1` functions.
%% * Protocol: A module that provides the Thrift protocol layer, e.g.,
%%   `ox_thrift_protocol_binary' or `ox_thrift_protocol_compact'.
%% * Service: A module, produced by the Thrift IDL compiler from the
%%   service's Thrift definition, that provides the Service layer.
%% * Options: A list of options.
%%     * `{map_module, MapModule}' where `MapModule' is `dict' (the default)
%%        or `maps'.
%%     * `{recv_timeout, Milliseconds}' or `{recv_timeout, infinity}' The
%%        receive timeout.  The default is `infinity'.

new (Connection, ConnectionState, Transport, Protocol, Service, Options)
  when is_atom(Connection), is_atom(Transport), is_atom(Protocol), is_atom(Service) ->
  Client0 = #ox_thrift_client{
               connection_module = Connection,
               connection_state = ConnectionState,
               transport_module = Transport,
               protocol_module = Protocol,
               service_module = Service},
  Client1 = parse_options(Options, Client0),
  {ok, Client1}.


parse_options ([ {map_module, MapModule} | Options ], Client=#ox_thrift_client{codec_config=CodecConfig})
  when MapModule =:= 'dict'; MapModule =:= 'maps' ->
  parse_options(Options, Client#ox_thrift_client{codec_config = CodecConfig#codec_config{map_module = MapModule}});
parse_options([ {recv_timeout, RecvTimeout} | Options ], Client)
  when (is_integer(RecvTimeout) andalso RecvTimeout >= 0) orelse (RecvTimeout =:= infinity) ->
  parse_options(Options, Client#ox_thrift_client{recv_timeout = RecvTimeout});
parse_options ([], Client) ->
  Client.


get_connection_module (#ox_thrift_client{connection_module=ConnectionModule}) ->
  ConnectionModule.


get_connection_state (#ox_thrift_client{connection_state=ConnectionState}) ->
  ConnectionState.


get_seqid (#ox_thrift_client{seqid=SeqId}) ->
  SeqId.


-spec call(Client::#ox_thrift_client{}, Function::atom(), Args::list()) ->
              {ok, OClient::#ox_thrift_client{}, Reply::term()} |
              {throw, OClient::#ox_thrift_client{}, Exception::term()} |
              {error, OClient::#ox_thrift_client{}, Reason::term()}.
%% @doc Calls a Thrift server and returns the result.
%%
%% * IClient: An `ox_thrift_client' record, as returned by the
%%   {@link ox_thrift_client:new/6} call or a previous {@link ox_thrift_client:call/3}
%%   call.
%% * Function: An atom representing the function to call.
%% * Args: A list containing the arguments to Function.
%%
%% The return value is
%% * `{ok, OClient, Reply}' for a normal return.  Reply is the Function's
%%    return value, or `ok' for a cast.
%% * `{throw, OClient, Exception}' if the call returns a declared exception.
%% * `{error, OClient, #application_exception{}}' if the function throws an
%%    undeclared exception.
%% * `{error, OClient, busy}' if you're using {@link ox_thrift_socket_pool}
%%    and you've reached the `max_connections' limit.
%% * `{error, OClient, timeout}' if the call times out.
%% * `{error, OClient, Reason}' if some other transport error happens.
call (Client=#ox_thrift_client{protocol_module=Protocol, service_module=Service, seqid=SeqId}, Function, Args)
  when is_atom(Function), is_list(Args) ->
  %% Formats the message and chains to call1.
  {CallType, RequestMsg} = Protocol:encode_call(Service, Function, SeqId, Args),
  call1(Client, CallType, RequestMsg, ?MAX_TRIES).

%% Gets the socket and chains to call2.
call1 (Client, _CallType, _RequestMsg, 0) ->
  {error, Client, max_retries};
call1 (Client=#ox_thrift_client{connection_module=Connection, connection_state=ConnectionState}, CallType, RequestMsg, TriesLeft) ->
  case Connection:checkout(ConnectionState) of
    {ok, Socket}    -> call2(Client, CallType, RequestMsg, Socket, TriesLeft);
    {error, Reason} -> {error, Client, Reason}
  end.

%% Sends the request and chains to call3.  Retry once if we discover that the
%% socket is closed.
call2 (Client=#ox_thrift_client{transport_module=Transport}, CallType, RequestMsg, Socket, TriesLeft) ->
  %% Implement thrift_framed_transport.
  Length = iolist_size(RequestMsg),
  case Transport:send(Socket, [ <<Length:32/big-signed>>, RequestMsg ]) of
    ok              -> call3(Client, CallType, RequestMsg, Socket, TriesLeft);
    {error, Closed} when ?REASON_IS_CLOSED(Closed)
                    -> call1(checkin(Client, Socket, closed), CallType, RequestMsg, TriesLeft - 1);
    {error, Reason} -> {error, checkin(Client, Socket, error), Reason}
  end.

%% Receives the response data and chains to call4.  Retry once if we discover
%% that the socket is closed.
call3 (Client, call_oneway, _RequestMsg, Socket, _TriesLeft) ->
  {ok, checkin(Client, Socket, ok), ok};
call3 (Client=#ox_thrift_client{transport_module=Transport, recv_timeout=RecvTimeout}, call, RequestMsg, Socket, TriesLeft) ->
  %% Implement thrift_framed_transport.
  %% When recv is called with a non-zero length, it will return `{error,
  %% timeout}' or `{error, closed}' rather than a short read.
  case Transport:recv(Socket, 4, RecvTimeout) of
    {ok, LengthBin} ->
      <<Length:32/integer-signed-big>> = LengthBin,
      case transport_recv(Transport, Socket, Length, RecvTimeout) of
        {ok, ReplyData}         -> call4(Client, Socket, ReplyData);
        {error, ReasonRecvData} -> {error, checkin(Client, Socket, error), ReasonRecvData}
      end;
    %% If the remote end has closed its end of the socket even before we send,
    %% we may discover that only when we try to read data.
    {error, Closed} when ?REASON_IS_CLOSED(Closed)
                                -> call1(checkin(Client, Socket, closed), call, RequestMsg, TriesLeft - 1);
    {error, ReasonRecvLength}   -> {error, checkin(Client, Socket, error), ReasonRecvLength}
  end.

%% Parses the response data and returns the reply.
call4 (Client=#ox_thrift_client{protocol_module=Protocol, service_module=Service, codec_config=CodecConfig, seqid=InSeqId}, Socket, ReplyData) ->
  {_Function, MessageType, SeqId, Reply} = Protocol:decode_message(Service, CodecConfig, ReplyData),
  if SeqId =/= InSeqId               -> {error, checkin(Client, Socket, error), application_exception_seqid(InSeqId, SeqId)};
     MessageType =:= reply_normal    -> {ok, checkin(Client, Socket, ok), Reply};
     MessageType =:= reply_exception -> {throw, checkin(Client, Socket, ok), Reply};
     MessageType =:= exception       -> {error, checkin(Client, Socket, error), Reply}
  end.

application_exception_seqid (ExpectedSeqId, ActualSeqId) ->
  #application_exception{
     message = list_to_binary(io_lib:format("exp:~p act:~p\n", [ ExpectedSeqId, ActualSeqId ])),
     type = ?tApplicationException_BAD_SEQUENCE_ID}.

checkin (Client=#ox_thrift_client{connection_module=Connection, connection_state=ConnectionState, seqid=SeqId}, Socket, Status) ->
  case Status of
    ok     -> Client#ox_thrift_client{connection_state = Connection:checkin(ConnectionState, Socket, ok), seqid = SeqId + 1};
    closed -> Client#ox_thrift_client{connection_state = Connection:checkin(ConnectionState, Socket, close)};
    error  -> Client#ox_thrift_client{connection_state = Connection:checkin(ConnectionState, Socket, close), seqid = 0}
  end.

transport_recv (Transport, Socket, Length, RecvTimeout) when Length < ?RECV_LIMIT ->
  Transport:recv(Socket, Length, RecvTimeout);
transport_recv (Transport, Socket, Length, RecvTimeout) ->
  transport_recv_large(Transport, Socket, Length, RecvTimeout, []).

transport_recv_large (_Transport, _Socket, 0, _RecvTimeout, Acc) ->
  {ok, list_to_binary(lists:reverse(Acc))};
transport_recv_large (Transport, Socket, Length, RecvTimeout, Acc) ->
  RecvLength = min(Length, ?RECV_LIMIT),
  case Transport:recv(Socket, RecvLength, RecvTimeout) of
    {ok, ReplyData} -> transport_recv_large(Transport, Socket, Length - RecvLength, RecvTimeout, [ ReplyData | Acc ]);
    Error={error, _} -> Error
  end.
