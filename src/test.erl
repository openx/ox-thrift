-module(test).

%% -behaviour(ox_thrift_handler).

-export([ main/1, handle_function/2, handle_error/2 ]).

-include_lib("ssrtb_thrift_erl/include/ssRtbService_thrift.hrl").
-include_lib("ssrtb_thrift_erl/include/ssrtb_service_types.hrl").
-include("ox_thrift.hrl").
-include("ox_thrift_internal.hrl").

%% main ([]) ->
%%   try doit()
%%   catch Error:Reason ->
%%       io:format("~p:~p\n~p\n", [ Error, Reason, erlang:get_stacktrace() ])
%%   end.

%% doit () ->

main ([]) ->
  Args = get_args(),

  SeqId = 88,

  Function = solicit_bidders,
  RequestMsgIOList = ox_thrift_protocol_binary:encode_message(ssRtbService_thrift,
                                                       Function, ?tMessageType_CALL, SeqId, Args),
  RequestMsg = list_to_binary(RequestMsgIOList),
  %% io:format("request ~p\n", [ RequestMsg ]),

  ThriftConfig = #ox_thrift_config{
                    service_module = ssRtbService_thrift,
                    codec_module = ox_thrift_protocol_binary,
                    handler_module = ?MODULE},
  ReplyMsgIOList = ox_thrift_server:handle_request(ThriftConfig, RequestMsg),
  {OFunction, OMessageType, OSeqId, [ Reply ]} =
    ox_thrift_protocol_binary:decode_message(ssRtbService_thrift, list_to_binary(ReplyMsgIOList)),

  %% io:format("reply ~p ~p ~p\ngot ~p\nexp ~p\n", [ OFunction, OMessageType, OSeqId, Reply, get_reply() ]),
  OFunction = Function,
  OMessageType = ?tMessageType_REPLY,
  OSeqId = SeqId,
  Reply == get_reply() orelse error(reply_mismatch),

  ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

handle_function (_Function=solicit_bidders, Args) ->
  Args == get_args() orelse error(request_mismatch),
  %% io:format("~p ~p\n", [ _Function, Args ]),

  {reply, get_reply()}.

handle_error (Function, Reason) ->
  io:format("error ~p ~p\n", [ Function, Reason ]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

get_args () ->
  RequestContext = dict:from_list([ {<<"key1">>, <<"value1">>}
                                  , {<<"key2">>, <<"value2">>}
                                  ]),
  Bidders = [ #ssRtbBidder{id = 10110,
                           matching_ad_ids = [ #ssRtbAdId{
                                                 campaign_id = 101,
                                                 placement_id = 102,
                                                 creative_id = 103,
                                                 ad_height = 250,
                                                 ad_width = 300,
                                                 ad_position = ?ssrtb_service_SsRtbAdPosition_Above_the_fold}
                                             , #ssRtbAdId{
                                                 campaign_id = 111,
                                                 placement_id = 112,
                                                 creative_id = 113,
                                                 ad_height = 600,
                                                 ad_width = 300,
                                                 ad_position = ?ssrtb_service_SsRtbAdPosition_Below_the_fold}
                                             ],
                           user_cookie_id = <<"bidder1_cookie">>,
                           endpoint = <<"http://bidder1.example.com/solicit">>,
                           rtb_data = <<"bidder1_rtbdata">>,
                           ox3_platform_hash = <<"bidder1_platform_hash">>,
                           ox3_id = 10110,
                           protocol = <<"openrtb_json">>,
                           currency = <<"USD">>,
                           bid_floor_cpm_micros = 123456}
            ],
  %% Bidders = [],

  [ <<"auction_id">> , 0, RequestContext, Bidders ].

get_reply () ->
  ResultContext = dict:from_list([ {<<"version">>, <<"1.2.3">>} ]),
  SolicitResults = [ #ssRtbSolicitResult{
                        id = 10110,
                        code = ?ssrtb_service_SsRtbReturnCode_OK,
                        response_time_mS = 120,
                        bids = [ #ssRtbBid{} ],
                        bids_discarded = [],
                        flags = dict:from_list([ {<<"flag1">>, 1001}, {<<"flag2">>, 1002} ])}
                   ],
  #ssRtbSolicitResponse{context = ResultContext, ssRtbSolicitResults = SolicitResults}.
