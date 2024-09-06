%% Copyright 2016-2024, OpenX.  All rights reserved.
%% Licensed under the conditions specified in the accompanying LICENSE file.

-module(ox_thrift_binary_tests).

-define(STATS_TABLE, ox_thrift_stats_binary).
-define(GLOBALS_TABLE, ox_thrift_globals_binary).

-define(PROTOCOL, ox_thrift_protocol_binary).
-include("ox_thrift_tests.hrl").

echo (#'AllTypes'{byte_field = 43}) ->
  {ox_thrift_pre_encoded, binary, <<2,0,1,1,3,0,2,43,6,0,6,127,14,8,0,5,255,255,255,133,10,0,4,1,35,69,103,138,188,222,240,4,0,3,64,36,64,0,0,0,0,0,11,0,7,0,0,0,5,120,121,122,122,121,15,0,8,8,0,0,0,3,0,0,0,1,0,0,0,2,0,0,0,3,14,0,9,11,0,0,0,3,0,0,0,2,98,98,0,0,0,3,99,99,99,0,0,0,1,97,13,0,10,11,8,0,0,0,3,0,0,0,1,73,0,0,0,1,0,0,0,1,88,0,0,0,10,0,0,0,1,86,0,0,0,5,15,0,11,2,0,0,0,4,1,1,0,1,15,0,12,3,0,0,0,0,15,0,13,4,0,0,0,2,63,240,0,0,0,0,0,0,192,0,0,0,0,0,0,0,15,0,14,11,0,0,0,3,0,0,0,3,111,110,101,0,0,0,3,116,119,111,0,0,0,5,116,104,114,101,101,0>>};
echo (V) ->
  V.
