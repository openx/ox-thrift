-module(ox_thrift_client).

-include("ox_thrift.hrl").
-include("ox_thrift_internal.hrl").

-define(MAX_TRIES, 2).

-export([ new/4, new/5, close/1, get_socket/1, get_seqid/1, call/3 ]).

-record(ox_thrift_client, {
          socket :: term(),
          socket_fun = error({required, socket_fun}) :: fun(() -> term()),
          transport_module = error({required, transport_module}) :: atom(),
          protocol_module = error({required, protocol_module}) :: atom(),
          service_module = error({required, service_module}) :: atom(),
          seqid = 0 :: integer(),
          recv_timeout = 'infinity' :: non_neg_integer() | 'infinity' }).

-define(RETURN_ERROR(Client, Error),
        begin
          ?LOG("return_error ~p ~p\n", [ Client, Error ]),
          error({close(Client), Error})
        end).

new (SocketFun, Transport, Protocol, Service) ->
  new(SocketFun, Transport, Protocol, Service, []).

new (SocketFun, Transport, Protocol, Service, Options)
  when is_function(SocketFun, 0), is_atom(Transport), is_atom(Protocol), is_atom(Service) ->
  Client0 = #ox_thrift_client{
               socket_fun = SocketFun,
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


close (Client=#ox_thrift_client{socket=Socket, transport_module=Transport}) ->
  Socket =:= undefined orelse
    Transport:close(Socket),
  Client#ox_thrift_client{socket = undefined}.


get_socket (#ox_thrift_client{socket=Socket}) ->
  Socket.


get_seqid (#ox_thrift_client{seqid=SeqId}) ->
  SeqId.


%% Formats the message and chains to call1.
call (Client=#ox_thrift_client{protocol_module=Protocol, service_module=Service, seqid=SeqId}, Function, Args)
  when is_atom(Function), is_list(Args) ->
  {CallType, RequestMsg} = Protocol:encode_call(Service, Function, SeqId, Args),
  call1(Client, CallType, RequestMsg, ?MAX_TRIES).

%% Ensures that socket is open and chains to call2.
call1 (Client, _CallType, _RequestMsg, 0) ->
  ?RETURN_ERROR(Client, {error, max_retries});
call1 (Client=#ox_thrift_client{socket=Socket0, socket_fun=SocketFun}, CallType, RequestMsg, TriesLeft) ->
  case Socket0 of
    undefined -> Socket1 = SocketFun(),
                 call2(Client#ox_thrift_client{socket = Socket1}, CallType, RequestMsg, TriesLeft);
    _         -> call2(Client, CallType, RequestMsg, TriesLeft)
  end.

%% Sends the request and chains to call3.  Retry once if we discover that the
%% socket is closed.
call2 (Client=#ox_thrift_client{socket=Socket, transport_module=Transport}, CallType, RequestMsg, TriesLeft) ->
  %% Implement thrift_framed_transport.
  Length = iolist_size(RequestMsg),
  case Transport:send(Socket, [ <<Length:32/big-signed>>, RequestMsg ]) of
    ok              -> call3(Client, CallType, RequestMsg, TriesLeft);
    {error, closed} -> call1(close(Client), CallType, RequestMsg, TriesLeft - 1);
    Error           -> ?RETURN_ERROR(Client, Error)
  end.

%% Receives the response data and chains to call4.  Retry once if we discover
%% that the socket is closed.
call3 (Client=#ox_thrift_client{seqid=InSeqId}, call_oneway, _RequestMsg, _TriesLeft) ->
  {Client#ox_thrift_client{seqid = InSeqId + 1}, ok};
call3 (Client=#ox_thrift_client{socket=Socket, transport_module=Transport, recv_timeout=RecvTimeout}, call, RequestMsg, TriesLeft) ->
  %% Implement thrift_framed_transport.
  case Transport:recv(Socket, 4, RecvTimeout) of
    {ok, LengthBin} ->
      <<Length:32/integer-signed-big>> = LengthBin,
      case Transport:recv(Socket, Length, RecvTimeout) of
        {ok, ReplyData} -> call4(Client, ReplyData);
        ErrorRecvData -> ?RETURN_ERROR(Client, ErrorRecvData)
      end;
    %% If the remote end has closed its end of the socket even before we send,
    %% we may discover that only when we try to read data.
    {error, closed} -> call1(close(Client), call, RequestMsg, TriesLeft - 1);
    ErrorRecvLength -> ?RETURN_ERROR(Client, ErrorRecvLength)
  end.

%% Parses the response data and returns the reply.
-spec call4(Client::#ox_thrift_client{}, ReplyData::binary()) -> {OClient::#ox_thrift_client{}, Reply::term()}.
call4 (Client=#ox_thrift_client{protocol_module=Protocol, service_module=Service, seqid=InSeqId}, ReplyData) ->
  {_Function, MessageType, SeqId, Reply} = Protocol:decode_message(Service, ReplyData),
  if SeqId =/= InSeqId               -> ?RETURN_ERROR(Client, application_exception_seqid(InSeqId, SeqId));
     MessageType =:= reply_normal    -> {Client#ox_thrift_client{seqid = SeqId + 1}, Reply};
     MessageType =:= reply_exception -> throw({Client#ox_thrift_client{seqid = SeqId + 1}, Reply});
     MessageType =:= exception       -> ?RETURN_ERROR(Client, Reply)
  end.

application_exception_seqid (ExpectedSeqId, ActualSeqId) ->
  #application_exception{
     message = list_to_binary(io_lib:format("exp:~p act:~p\n", [ ExpectedSeqId, ActualSeqId ])),
     type = ?tApplicationException_BAD_SEQUENCE_ID}.
