%% Copyright 2016, OpenX.  All rights reserved.
%% Licensed under the conditions specified in the accompanying LICENSE file.

-module(ox_thrift_client).

-include("ox_thrift.hrl").
-include("ox_thrift_internal.hrl").

-define(MAX_TRIES, 2).

-export([ new/5, new/6, get_connection_module/1, get_connection_state/1, get_seqid/1, call/3 ]).
-export_type([ ox_thrift_client/0 ]).


-record(ox_thrift_client, {
          connection_module = error({required, connection_module}) :: atom(),
          connection_state = error({required, connection_state}) :: ox_thrift_connection:connection_state(),
          transport_module = error({required, transport_module}) :: atom(),
          protocol_module = error({required, protocol_module}) :: atom(),
          service_module = error({required, service_module}) :: atom(),
          seqid = 0 :: integer(),
          recv_timeout = 'infinity' :: non_neg_integer() | 'infinity' }).
-type ox_thrift_client() :: #ox_thrift_client{}.


-define(RETURN_ERROR(Client, Socket, Error),
        begin
          ?LOG("return_error ~p ~p\n", [ Client, Error ]),
          error({checkin(Client, Socket, error), Error})
        end).


new (Connection, ConnectionState, Transport, Protocol, Service) ->
  new(Connection, ConnectionState, Transport, Protocol, Service, []).

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


%% Formats the message and chains to call1.
-spec call(Client::#ox_thrift_client{}, Function::atom(), Args::list()) ->
              {OClient::#ox_thrift_client{}, Reply::term()}.
call (Client=#ox_thrift_client{protocol_module=Protocol, service_module=Service, seqid=SeqId}, Function, Args)
  when is_atom(Function), is_list(Args) ->
  {CallType, RequestMsg} = Protocol:encode_call(Service, Function, SeqId, Args),
  call1(Client, CallType, RequestMsg, ?MAX_TRIES).

%% Gets the socket and chains to call2.
call1 (Client, _CallType, _RequestMsg, 0) ->
  error({Client, {error, max_retries}});
call1 (Client=#ox_thrift_client{connection_module=Connection, connection_state=ConnectionState}, CallType, RequestMsg, TriesLeft) ->
  case Connection:checkout(ConnectionState) of
    {ok, Socket} -> call2(Client, CallType, RequestMsg, Socket, TriesLeft);
    Error        -> error({Client, Error})
  end.

%% Sends the request and chains to call3.  Retry once if we discover that the
%% socket is closed.
call2 (Client=#ox_thrift_client{transport_module=Transport}, CallType, RequestMsg, Socket, TriesLeft) ->
  %% Implement thrift_framed_transport.
  Length = iolist_size(RequestMsg),
  case Transport:send(Socket, [ <<Length:32/big-signed>>, RequestMsg ]) of
    ok              -> call3(Client, CallType, RequestMsg, Socket, TriesLeft);
    {error, closed} -> call1(checkin(Client, Socket, closed), CallType, RequestMsg, TriesLeft - 1);
    Error           -> ?RETURN_ERROR(Client, Socket, Error)
  end.

%% Receives the response data and chains to call4.  Retry once if we discover
%% that the socket is closed.
call3 (Client, call_oneway, _RequestMsg, Socket, _TriesLeft) ->
  {checkin(Client, Socket, ok), ok};
call3 (Client=#ox_thrift_client{transport_module=Transport, recv_timeout=RecvTimeout}, call, RequestMsg, Socket, TriesLeft) ->
  %% Implement thrift_framed_transport.
  %% When recv is called with a non-zero length, it will return `{error,
  %% timeout}' or `{error, closed}' rather than a short read.
  case Transport:recv(Socket, 4, RecvTimeout) of
    {ok, LengthBin} ->
      <<Length:32/integer-signed-big>> = LengthBin,
      case Transport:recv(Socket, Length, RecvTimeout) of
        {ok, ReplyData} -> call4(Client, Socket, ReplyData);
        ErrorRecvData   -> ?RETURN_ERROR(Client, Socket, ErrorRecvData)
      end;
    %% If the remote end has closed its end of the socket even before we send,
    %% we may discover that only when we try to read data.
    {error, closed}     -> call1(checkin(Client, Socket, closed), call, RequestMsg, TriesLeft - 1);
    ErrorRecvLength     -> ?RETURN_ERROR(Client, Socket, ErrorRecvLength)
  end.

%% Parses the response data and returns the reply.
call4 (Client=#ox_thrift_client{protocol_module=Protocol, service_module=Service, seqid=InSeqId}, Socket, ReplyData) ->
  {_Function, MessageType, SeqId, Reply} = Protocol:decode_message(Service, ReplyData),
  if SeqId =/= InSeqId               -> ?RETURN_ERROR(Client, Socket, application_exception_seqid(InSeqId, SeqId));
     MessageType =:= reply_normal    -> {checkin(Client, Socket, ok), Reply};
     MessageType =:= reply_exception -> throw({checkin(Client, Socket, ok), Reply});
     MessageType =:= exception       -> ?RETURN_ERROR(Client, Socket, Reply)
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
