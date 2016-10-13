%% Copyright 2016, OpenX.  All rights reserved.
%% Licensed under the conditions specified in the accompanying LICENSE file.

-module(direct_transport).

-behaviour(ox_thrift_connection).

-include("ox_thrift.hrl").
-include("../src/ox_thrift_internal.hrl").

%%% A dummy transport that provides the interface required by
%%% `ox_thrift_client'.  This transport calls the thrift server directly
%%% instead of through a socket, and uses the process dictionary to remember
%%% the thrift function's return value between calls to `send' and `recv'.

-export([ send/2, recv/3 ]).                            % transport functions
-export([ new/1, destroy/1, checkout/1, checkin/3 ]).   % connection functions
-export([ is_open/1 ]).                                 % for testing

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

recv (_Config, Length, _Timeout) ->
  <<Return:Length/binary, Rest/binary>> = get(?MODULE),
  put(?MODULE, Rest),
  {ok, Return}.

new ({Service, Protocol, Handler, StatsModule}) ->
  #ox_thrift_config{
     service_module = Service,
     protocol_module = Protocol,
     handler_module = Handler,
     options = [ {stats_module, StatsModule} ]}.

destroy (_State) ->
  ok.

checkout (State) ->
  put(?MODULE, <<>>),                           % Reset the buffer.
  put(is_open, true),
  {ok, State}.

checkin (State, _, Status) ->
  put(is_open, if Status =:= ok -> true; true -> false end),
  State.

is_open (_State) ->
  get(is_open).
