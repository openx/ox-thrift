-module(direct_transport).

-include("ox_thrift.hrl").
-include("../src/ox_thrift_internal.hrl").

%%% A dummy transport that provides the interface required by
%%% `ox_thrift_client'.  This transport calls the thrift server directly
%%% instead of through a socket, and uses the process dictionary to remember
%%% the thrift function's return value between calls to `send' and `recv'.

-export([ send/2, recv/2, close/1, make_get_socket/4 ]).

send (Config=#ox_thrift_config{}, Request) ->
  <<RequestLength:32/big-signed, RequestBin/binary>> = iolist_to_binary(Request),
  io:format(standard_io, "request: ~p, ~p\n", [ RequestLength, RequestBin ]),
  TSConfig = ox_thrift_server:parse_config(Config),
  {Reply, _Function, _ReplyOptions} = ox_thrift_server:handle_request(TSConfig, RequestBin),
  case Reply of
    noreply -> ok;
    _       -> ReplyBin = iolist_to_binary(Reply),
               ReplyLength = size(ReplyBin),
               io:format(standard_io, "reply: ~p ~p\n", [ ReplyLength, ReplyBin ]),
               put(?MODULE, <<ReplyLength:32/big-signed, ReplyBin/binary>>),
               ok
  end.

recv (_Socket, Length) ->
  <<Return:Length/binary, Rest/binary>> = get(?MODULE),
  put(?MODULE, Rest),
  {ok, Return}.

close (_Socket) ->
  ok.

-spec make_get_socket(Service::atom(), Codec::atom(), Handler::atom(), StatsModule::atom()) ->
                         GetSocketFun::fun(() -> Socket::term()).
make_get_socket (Service, Codec, Handler, StatsModule) ->
  Config = #ox_thrift_config{
              service_module = Service,
              codec_module = Codec,
              handler_module = Handler,
              options = [ {stats_module, StatsModule} ]},
  fun () ->
      put(?MODULE, <<>>),                       % Reset the buffer.
      Config
  end.
