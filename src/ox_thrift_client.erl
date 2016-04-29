-module(ox_thrift_client).

-include("ox_thrift.hrl").
-include("ox_thrift_internal.hrl").

-define(MAX_TRIES, 2).

-export([ new/4, close/1, get_socket/1, get_seqid/1, call/3 ]).

-record(ox_thrift_client, {
          socket :: term(),
          socket_fun = error({required, socket_fun}) :: fun(() -> term()),
          transport_module = error({required, transport_module}) :: atom(),
          protocol_module = error({required, protocol_module}) :: atom(),
          service_module = error({required, service_module}) :: atom(),
          seqid = 0 :: integer()}).

-define(RETURN_ERROR(Client, Error),
        begin
          ?LOG("return_error ~p ~p\n", [ Client, Error ]),
          error({close(Client), Error})
        end).

new (SocketFun, Transport, Protocol, Service)
  when is_function(SocketFun, 0), is_atom(Transport), is_atom(Protocol), is_atom(Service) ->
  {ok, #ox_thrift_client{
          socket_fun = SocketFun,
          transport_module = Transport,
          protocol_module = Protocol,
          service_module = Service}}.


close (Client=#ox_thrift_client{socket=Socket, transport_module=Transport}) ->
  Socket =:= undefined orelse
    Transport:close(Socket),
  Client#ox_thrift_client{socket = undefined, seqid = 0}.


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

%% Sends the request and chains to call3.  Retry once if socket we discover
%% that the socket is actually closed.
call2 (Client=#ox_thrift_client{socket=Socket, transport_module=Transport}, CallType, RequestMsg, TriesLeft) ->
  %% Implement thrift_framed_transport.
  Length = iolist_size(RequestMsg),
  case Transport:send(Socket, [ <<Length:32/big-signed>>, RequestMsg ]) of
    ok              -> call3(Client, CallType);
    %% I believe that if the remote end has closed their end of the socket, we
    %% will only discover that when we attempt to write to it.  Open a new
    %% server connection and retry the call in this case.
    {error, closed} -> call1(close(Client), CallType, RequestMsg, TriesLeft - 1);
    Error           -> ?RETURN_ERROR(Client, Error)
  end.

%% Receives the response data and chains to call4.  Don't retry errors.
call3 (Client=#ox_thrift_client{seqid=InSeqId}, call_oneway) ->
  {Client#ox_thrift_client{seqid = InSeqId + 1}, ok};
call3 (Client=#ox_thrift_client{socket=Socket, transport_module=Transport}, call) ->
  %% Implement thrift_framed_transport.
  case Transport:recv(Socket, 4) of
    {ok, LengthBin} ->
      <<Length:32/integer-signed-big>> = LengthBin,
      case Transport:recv(Socket, Length) of
        {ok, ReplyData} -> call4(Client, ReplyData);
        ErrorRecvData -> ?RETURN_ERROR(Client, ErrorRecvData)
      end;
    ErrorRecvLength -> ?RETURN_ERROR(Client, ErrorRecvLength)
  end.

%% Parses the response data and returns the reply.
-spec call4(Client::#ox_thrift_client{}, ReplyData::binary()) -> {OClient::#ox_thrift_client{}, Reply::term()}.
call4 (Client=#ox_thrift_client{protocol_module=Protocol, service_module=Service, seqid=InSeqId}, ReplyData) ->
  {_Function, MessageType, SeqId, Reply} = Protocol:decode_message(Service, ReplyData),
  if SeqId =/= InSeqId               -> ?RETURN_ERROR(Client, #application_exception{type = ?tApplicationException_BAD_SEQUENCE_ID});
     MessageType =:= reply_normal    -> {Client#ox_thrift_client{seqid = SeqId + 1}, Reply};
     MessageType =:= reply_exception -> throw({Client#ox_thrift_client{seqid = SeqId + 1}, Reply});
     MessageType =:= exception       -> ?RETURN_ERROR(Client, Reply)
  end.
