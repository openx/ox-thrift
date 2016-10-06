<!-- -*- mode:gfm; word-wrap:nil -*- github-flavored markdown -->

# OpenX Erlang Thrift Implementation

OX Thrift (`ox-thrift`) is a reimplementation of the
[Apache Thrift](https://thrift.apache.org/) Erlang library with an
emphasis on speed of encoding and decoding.  In its current
incarnation, it uses the structure definitions produced by the Apache
Thrift code generator.  However it has the following differences from
the Apache Thrift Erlang library.

* It supports only framed transports and binary protocol.

* It gives up the ability to stream to or from the transport.
  Instead, the processor layer decodes the thrift message from a
  binary buffer and encodes to an iolist.  This simplifies the
  implementation and avoids a lot of record modifications that are
  inefficient in Erlang.

* The Thrift server uses the
  [Ranch](https://github.com/ninenines/ranch) acceptor pool instead of
  rolling its own.

* The `HandlerModule:handle_function(Function, Args)` interface
  expects the HandlerModule to take its arguments as a list instead of
  a tuple.

* For a Thrift `map` type, for encoding OX Thrift will accept either a
  dict (as Apache Thrift does) or a proplist.  For decoding, however,
  OX Thrift always returns a dict (as Apache Thrift does).

* Like the Apache Thrift Erlang library, OX Thrift does not enforce
  required struct fields, on either encoding or decoding.

* Like the Apache library, OX Thrift does not populate the structs
  with default values on decoding.

## Interface

### Client

The OX Thrift client interface is provided by the `ox_thrift_client`
module.  This module exports two functions, `new`, and `call`.

#### `new` -- Manage a Connection to a Thrift Server

``` erlang
{ok, Client} = ox_thrift_client:new(SocketFun, Transport, Protocol, Service)
{ok, Client} = ox_thrift_client:new(SocketFun, Transport, Protocol, Service, Options)
```

* SocketFun: A zero-arity function that returns a new passive-mode
  connection to the thrift server.  The OX Thrift client will call
  this function to open the initial connection to the Thrift server,
  and to open a new connection whenever it encounters an error on the
  existing connection.
* Transport: A module that provides the transport layer, e.g.,
  `gen_tcp`.  This module is expected to supply `send/2`, `recv/2`,
  and `close/1` functions.
* Protocol: A module that provides the Thrift protocol layer, e.g.,
  `ox_thrift_protocol_binary`.
* Service: A module, produced by the Thrift IDL compiler from the
  service's Thrift definition, that provides the Service layer.
* Options: A list of options.
  * `{recv_timeout, Milliseconds}` or `{recv_timeout, infinity}`
  The receive timeout.

The following shows an example SocketFun for use with the `gen_tcp` Transport.

``` erlang
make_get_socket (Host, Port) ->
  fun () ->
      case gen_tcp:connect(Host, Port, [ binary, {active, false} ]) of
        {ok, Socket} -> Socket;
        {error, Reason} -> error({connect, Host, Port, Reason})
      end
  end.
```

The OX Thrift library expects the Transport module to support the
following functions on this Socket.

* `ok = Transport:send(Socket, IOData)`,
* `{ok, Binary} = Transport:recv(Socket, Length, Timeout)`, and
* `ok = Transport:close(Socket)`

#### `call` -- Make a Call to a Thrift Server

``` erlang
{OClient, Result} = ox_thrift_client:call(IClient, Function, Args)
```

* IClient: An `ox_thrift_client` record, as produced by the
  `ox_thrift_client:new` call.
* Function: An atom representing the function to call.
* Args: A list containing the arguments to Function.

### Server

The OX Thrift server interface is provided by the `ox_thrift_server`
module.  This module is designed to be used with the
[Ranch](https://github.com/ninenines/ranch) acceptor pool.  Your
application needs to start Ranch, and then to register the
`ox_thrift_server` module with Ranch as a protocol handler.

```erlang
ranch:start_listener(?THRIFT_SERVICE_REF,
                     10,                      % Number of acceptors.
                     ranch_tcp,
                     [ {port, Port}           % https://github.com/ninenines/ranch/blob/master/doc/src/manual/ranch_tcp.asciidoc
                     , {reuseaddr, true}
                     ],
                     ox_thrift_server,        % Ranch protocol module.
                     #ox_thrift_config{
                        service_module = service_thrift,
                        protocol_module = ox_thrift_protocol_binary,
                        handler_module = ?MODULE}).
```

Your Thrift server must supply two functions, `handle_function` and
`handle_error`.  These functions are defined by the `ox_thrift_server`
behaviour.

Unlike the Apache Thrift's `handle_function` Erlang interface, OX
Thrift passes the Args parameter as a list instead of a tuple.

```erlang
-behaviour(ox_thrift_server).

handle_function (Function, Args) when is_atom(Function), is_list(Args) ->
  case apply(?MODULE, Function, Args) of
    ok    -> ok;
    Reply -> {reply, Reply}
  end.

handle_error(Function, Reason) ->
  ok.
```

The `handle_function` function may return `ok` for a void function, or
`{reply, Reply}` for a non-void function.  In addition, the function
may request that the Thrift server close the client's connection after
returning the reply by returning `{ok, close}` for a void function or
`{reply, Reply, close}` for a non-void function.

If the Thrift function wants to return one of its declared exceptions,
`E`, it may call `{throw, E}`.  The Thrift server will catch the
exception and return a message to the client, where the exception will
be re-thrown.


#### Setting the Server's `recv` Timeout

The default `recv` timeout is `infinity`, which means that the server
will keep a socket open indefinitely waiting for a client to send a
request. You can override this with the `recv_timeout` option.

``` erlang
#ox_thrift_config{options = [ { recv_timeout, TimeoutMilliseconds } ]}

```

#### Stats Collection

The OX Thrift server will optionally call a `handle_stats` function in
a module that you define when you start the Ranch listener.

``` erlang
#ox_thrift_config{options = [ { stats_module, StatsModule } ]}
```

The interface for the stats-collection function is:

``` erlang
handle_stats (Function, [ {Stat, Value} ]) ->
  ok.
```

The statistics currently collected are:

* `decode_time`: The call arguments decoding time in microseconds.
* `decode_size`: The size of the encoded Thrift message being decoded.
* `decode_reductions`: The number of reductions consumed decoding the Thrift message.
* `encode_time`: The call reply encoding time in microseconds.
* `encode_size`: The size of the encoded Thrift message.
* `encode_reductions`: The number of reductions consumed encoding the Thrift message.
* `connect_time`: The total connect time in microseconds.
* `call_count`: The number of calls to the Thrift server.


### Direct Encoding and Decoding of Records to and from Binaries

The `ox_thrift_protocol_binary:encode_record/2` function encodes a
Thrift struct record to a binary, and
`ox_thrift_protocol_binary:decode_record/2` decodes the binary back to
the original record.

``` erlang
Binary = encode_record({ServiceTypesModule, StructName}, Record)
```

``` erlang
Record = decode_record({ServiceTypesModule, StructName}, Binary)
```

If your Thrift service is "svc" the ServiceTypesModule will be `svc_types`.


## Speedup

These numbers were taken from benchmarks of a production SSRTB system.

|        |Apache Thrift|OX Thrift|Speedup|
|--------|-------------|---------|-------|
|Decoding|      1098 us|   244 us|  4.5 x|
|Encoding|       868 us|   185 us|  4.7 x|

<!--
```
[{decode,13569,14898510,1098},
 {encode,10764,9338300,868}]
```

New Code
```
(erlang@xfga-e27.xf.dc.openx.org)8> ssrtb_thrift_service:print_stats().
decode    8135310 us /    33373 =  244 us
encode    4902716 us /    26462 =  185 us
```
-->

## Message Protocol

See the [message protocol documentation](MessageProtocol.md).

## Other Info

* Anthony pointed me a talk by [Péter Gömöri](https://github.com/gomoripeti) on
  [The Fun Part of Writing a Thrift Codec](http://www.erlang-factory.com/static/upload/media/1442407543231431thefunpartofwritingathriftcodec.pdf),
  which describes his work to speed up Thrift encoding and decoding.
  Unfortunately, the code is not public.  The author claims the
  following speedups:

  |Optimization   |Decoding|Encoding|
  |---------------|--------|--------|
  |Layer Squashing|2x      |8x      |
  |Generated Funs |16x     |16x     |

* Erlang documentation on
  [Constructing and Matching Binaries](http://erlang.org/doc/efficiency_guide/binaryhandling.html),
  which may be useful for optimization.

* The
  [Thrift paper](https://thrift.apache.org/static/files/thrift-20070401.pdf),
  which describes the design and data encoding protocol for Thrift.
