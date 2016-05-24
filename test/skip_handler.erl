-module(skip_handler).

-export([ handle_function/2, handle_error/2 ]).


handle_function (Function, Args) ->
  case Function of
    missing -> {reply, hd(Args)}                % Just return the first arg.
  end.

handle_error (_Function, _Reason) ->
  %% io:format(standard_error, "handle_error ~p ~p\n", [ _Function, _Reason ]),
  ok.
