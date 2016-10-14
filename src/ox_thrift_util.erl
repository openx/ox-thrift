%% Copyright 2016, OpenX.  All rights reserved.
%% Licensed under the conditions specified in the accompanying LICENSE file.

-module(ox_thrift_util).

-export([ format_error_message/1 ]).

format_error_message (Reason) ->
  case application:get_env(ox_thrift, exceptions_include_traces) of
    {ok, true} -> list_to_binary(io_lib:format("An error occurred: error:~p ~p~n", [ Reason, erlang:get_stacktrace() ]));
    _          -> list_to_binary(io_lib:format("An error occurred: error:~p", [ Reason ]))
  end.
