<!-- -*- mode:gfm; word-wrap:nil -*- github-flavored markdown -->

# OpenX Erlang Thrift Implementation

OX Thrift (`ox-thrift`) is a reimplementation of the
[Apache Thrift](https://thrift.apache.org/) Erlang library with an
emphasis on speed of encoding and decoding.  In its current
incarnation, it uses the structure definitions produced by the Apache
Thrift code generator.  However it has the following differences from
the Apache Thrift Erlang library.

* It supports only framed transports and binary and compact protocols.

* It gives up the ability to stream directly to or from the transport.
  Instead, the processor layer decodes the thrift message from a
  binary buffer and encodes to an iolist.  Compared to the Apache
  Erlang Thrift library, this simplifies the implementation and avoids
  a lot of record modifications that are inefficient in Erlang.

* The Thrift server uses the
  [Ranch](https://github.com/ninenines/ranch) acceptor pool instead of
  rolling its own.

* The `HandlerModule:handle_function(Function, Args)` interface
  expects the HandlerModule to take its arguments as a list instead of
  a tuple.  This is more consistent with Erlang conventions for
  functions that take a variable number of arguments.

* For the Thrift `map` type, for encoding OX Thrift will accept either
  a dict (as Apache Thrift does), a proplist, or an Erlang map.  For
  decoding, however, OX Thrift can be configured to return a dict or a
  map.  See the documentation for the `map_module` option.

* Like the Apache library, OX Thrift does not enforce required struct
  fields, on either encoding or decoding.

* Like the Apache library, OX Thrift does not populate the structs
  with default values on decoding.

## Interface

### Client

The OX Thrift client interface is provided by the `ox_thrift_client`
module.  This module exports two functions, `new`, and `call`.

#### `new` -- Manage a Connection to a Thrift Server

``` erlang
{ok, Client} = ox_thrift_client:new(Connection, ConnectionState, Transport, Protocol, Service)
{ok, Client} = ox_thrift_client:new(Connection, ConnectionState, Transport, Protocol, Service, Options)
```

* Connection: A module that manages a connection to the Thrift server.
  This module must support the interface defined by
  `ox_thrift_connection`.  The OX Thrift client will call the module's
  `checkout` and `checkin` functions.  The OX Thrift library supplies
  several modules that implement this interface, described in the
  [Connection Managers](#connection-managers) section below.
* ConnectionState: State managed by the Connection module.
* Transport: A module that provides the transport layer, such as
  `gen_tcp`.  This module is expected to export `send/2`, `recv/3`,
  and `close/1` functions.
* Protocol: A module that provides the Thrift protocol layer, e.g.,
  `ox_thrift_protocol_binary` or `ox_thrift_protocol_compact`.
* Service: A module, produced by the Thrift IDL compiler from the
  service's Thrift definition, that provides the Service layer.
* Options: A list of options.
  * `{map_module, MapModule}` where `MapModule` is `dict` or `maps`
    specifies the module that is used to decode Thrift `map` fields.
    The default is `dict`.
  * `{recv_timeout, Milliseconds}` or `{recv_timeout, infinity}` The
    receive timeout.  The default is `infinity`.

The Connection module's `checkout` function should return a socket
Socket, and the OX Thrift library expects the Transport module to
support the following functions on this Socket.

* `ok = Transport:send(Socket, IOData)`,
* `{ok, Binary} = Transport:recv(Socket, Length, Timeout)`, and
* `ok = Transport:close(Socket)`

#### `call` -- Make a Call to a Thrift Server

``` erlang
{ok, OClient, Result} = ox_thrift_client:call(IClient, Function, Args)
```

* IClient: An `ox_thrift_client` record, as returned by the
  `ox_thrift_client:new` call or a previous `ox_thrift_client:call`
  call.
* Function: An atom representing the function to call.
* Args: A list containing the arguments to Function.

#### Connection Managers

OX Thrift supplies several connection managers for use with the OX
Thrift client.

* `ox_thrift_simple_socket` implements a non-reconnecting connection
  to the Thrift server.

* `ox_thrift_reconnecting_socket` implements a simple dedicated
  connection to the Thrift server that automatically reopens the
  connection when there is an error.

* `ox_thrift_socket_pool` implements a pool of connections that can be
  shared among multiple processes.  The pool is managed by a
  gen_server which you are expected to link into your supervisor tree
  by calling the `ox_thrift_socket_pool:start_link/4` function.

### Server

The OX Thrift server interface is provided by the `ox_thrift_server`
module.  This module is designed to be used with the
[Ranch](https://github.com/ninenines/ranch) acceptor pool.  Your
application needs to start Ranch, and then to register the
`ox_thrift_server` module with Ranch as a protocol handler.

```erlang
ranch:start_listener(
  ?THRIFT_SERVICE_REF,
  10,                % Number of acceptors.
  ranch_tcp,
  [ {port, Port} ],  % https://github.com/ninenines/ranch/blob/master/doc/src/manual/ranch_tcp.asciidoc
  ox_thrift_server,  % Ranch protocol module.
  #ox_thrift_config{
     service_module = service_thrift,
     protocol_module = ox_thrift_protocol_binary,
     handler_module = ?MODULE}).
```

Your Thrift server must supply two functions, `handle_function` and
`handle_error`.  These functions are defined by the `ox_thrift_server`
behaviour, and the module that supplies these functions is specified
in the `handler_module` field.

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

The protocol is specified by setting the `protocol_module` field to
one of the following values.

* `ox_thrift_protocol_binary`: binary protocol
* `ox_thrift_protocol_compact`: compact protocol

#### Thrift `map` Field Decoding

By default, Thrift `map` fields are decoded to Erlang `dict`s.  You
can specify that they be decoded as Erlang `maps` with the
`map_module` option.

``` erlang
#ox_thrift_config{options = [ { map_module, maps } ]}
```

#### Setting the Server's `recv` Timeout

The default `recv` timeout is `infinity`, which means that the server
will keep a socket open indefinitely waiting for a client to send a
request.  You can override this with the `recv_timeout` option.

``` erlang
#ox_thrift_config{options = [ { recv_timeout, TimeoutMilliseconds } ]}
```

#### Limiting the Maximum Message Size

By default the OX Thrift server will accept Thrift messages of unlimited size
(`infinity`).  If you are in an environment where clients may sent you
improperly-framed thrift messages (for example, random requests coming from a
port scanner), ox_thrift allows you to limit the maximum size of the message
the server will accept.  Messages that are larger than this limit will result
in a call to the `handle_error` function with a `large_message` error.

``` erlang
#ox_thrift_config{options = [ { max_message_size, MaxSizeBytes } ]}
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

These numbers were taken from benchmarks of the binary protocol on a
production SSRTB system.

|        |Apache Thrift|OX Thrift|Speedup|
|--------|------------:|--------:|------:|
|Decoding|      1098 us|   244 us|  4.5 x|
|Encoding|       868 us|   185 us|  4.7 x|

## Message Protocol

See the [message protocol documentation](MessageProtocol.md).

## Other Info

* Anthony pointed me a talk by [Péter Gömöri](https://github.com/gomoripeti) on
  [The Fun Part of Writing a Thrift Codec](http://www.erlang-factory.com/static/upload/media/1442407543231431thefunpartofwritingathriftcodec.pdf),
  which describes his work to speed up Thrift encoding and decoding.
  Unfortunately, the code is not public.  The author claims the
  following speedups:

  |Optimization   |Decoding|Encoding|
  |---------------|-------:|-------:|
  |Layer Squashing|2x      |8x      |
  |Generated Funs |16x     |16x     |

* [Thrash](https://github.com/dantswain/thrash) is an Elixir library
  that automatically generates functions for decoding and encoding
  Thrift messages using the binary protocol.  The author claims
  speedups of 18x for decoding and 5x for encoding over the Apache
  Thrift library.

* [elixir-thrift](https://github.com/pinterest/elixir-thrift) is another
  Elixir Thrift library.  The author claims a speedup of between 10x and 25x
  over the Apache Thrift library.  It appears that this library generates code
  from the Thrift definition directly instead of using the structure
  definitions that the Apache Thrift code generator produces as Thrash does.

* Erlang documentation on
  [Constructing and Matching Binaries](http://erlang.org/doc/efficiency_guide/binaryhandling.html),
  which may be useful for optimization.

* The
  [Thrift paper](https://thrift.apache.org/static/files/thrift-20070401.pdf),
  which describes the design and data encoding protocol for Thrift.

* Erik van Oosten's
  [Thrift Specification -- Remote Procedure Call](https://erikvanoosten.github.io/thrift-missing-specification/)
  (a.k.a, "Thrift: The Missing Specification").

* Andrew Prunicki's "Apache Thrift" overview has a [comparison of Thrift with
  other protocols](http://jnb.ociweb.com/jnb/jnbJun2009.html#compare) that
  compares encoding size and has some benchmarks.

## Acknowledgements

Many thanks to [Yoshihiro Tanaka](https://github.com/hirotnk)
and [Anthony Molinaro](https://github.com/djnym) for their insights
and suggestions.
