%% Copyright 2016, 2018 OpenX.  All rights reserved.
%% Licensed under the conditions specified in the accompanying LICENSE file.

-module(ox_thrift_util).

-export([ format_error_message/1, format_error_message/3 ]).

-spec format_error_message(Reason::term()) -> Message::binary().
format_error_message (Reason) ->
  format_error_message(error, Reason, no_stackrace).

-spec format_error_message(Type::'error'|'throw', Reason::term(), Stacktrace::term()) -> Message::binary().
format_error_message (Type, Reason, Stacktrace) when is_atom(Type) ->
  case application:get_env(ox_thrift, exceptions_include_traces) of
    {ok, true} -> list_to_binary(io_lib:format("An error occurred: ~s:~p ~9999p~n", [ Type, Reason, Stacktrace ]));
    _          -> list_to_binary(io_lib:format("An error occurred: ~s:~p", [ Type, Reason ]))
  end.
