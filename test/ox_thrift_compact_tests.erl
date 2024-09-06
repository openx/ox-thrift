%% Copyright 2016-2024, OpenX.  All rights reserved.
%% Licensed under the conditions specified in the accompanying LICENSE file.

-module(ox_thrift_compact_tests).

-define(STATS_TABLE, ox_thrift_stats_compact).
-define(GLOBALS_TABLE, ox_thrift_globals_compact).

-define(PROTOCOL, ox_thrift_protocol_compact).
-include("ox_thrift_tests.hrl").

echo (#'AllTypes'{byte_field = 43}) ->
  {ox_thrift_pre_encoded, compact,
    <<17,19,43,68,156,252,3,5,10,245,1,6,8,224,251,230,171,241,217,162,163,2,7,6,0,0,0,0,0,64,36,64,72,5,120,121,122,122,121,25,53,2,4,6,26,56,2,98,98,3,99,99,99,1,97,27,3,133,1,73,2,1,88,20,1,86,10,25,65,2,2,0,2,25,3,25,39,0,0,0,0,0,0,240,63,0,0,0,0,0,0,0,192,25,56,3,111,110,101,3,116,119,111,5,116,104,114,101,101,0>>
    };
echo (V) ->
  V.
