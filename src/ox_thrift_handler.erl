%% Copyright 2016, OpenX.  All rights reserved.
%% Licensed under the conditions specified in the accompanying LICENSE file.

-module(ox_thrift_handler).

-callback handle_function(Function::atom(), Args::list()) -> Reply::ok|{reply, term()}.

-callback handle_error(Function::atom(), Reason::term()) -> Ignored::term().
