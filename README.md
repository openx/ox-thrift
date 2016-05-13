<!-- -*- mode:gfm; word-wrap:nil -*- github-flavored markdown -->

# OpenX Erlang Thrift Implementation

OX Thrift (`ox-thrift`) is a reimplementation of the
[Apache Thrift](https://thrift.apache.org/) Erlang library with an
emphasis on speed of encoding and decoding.  In its current
incarnation, it uses the structure definitions produced by the Apache
Thrift code generator.  However it has the following differences from
the Apache Thrift Erlang library.

* It supports only framed transport.

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

## Interface

### Client

The OX Thrift client interface is provided by the `ox_thrift_client`
module.  This module exports two functions, `new`, and `call`.

#### `new` -- Manage a Connection to a Thrift Server

``` erlang
{ok, Client} = ox_thrift_client:new(SocketFun, Transport, Protocol, Service)
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

The OX Thrift library expects to be able to call
* `ok = Transport:send(Socket, IOData)`,
* `{ok, Binary} = Transport:recv(Socket, Length)`, and
* `ok = Transport:close(Socket)`
on this Socket.

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
                        codec_module = ox_thrift_protocol_binary,
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
  ok;
```

## Speedup

These numbers were taken from benchmarks of a production SSRTB system.

|        |Apache Thrift|OX Thrift|Speedup|
|--------|-------------|---------|-------|
|Encoding|       736 us|   171 us|  4.3 x|
|Decoding|      1051 us|   250 us|  4.2 x|

<!--
```
[{decode,159370,167525741,1051},
 {encode,132855,97760667,736}]
```

New Code
```
(erlang@xfga-e27.xf.dc.openx.org)9> ox_thrift_stats:read().
[{decode,481342,127065950,264},
 {encode,379650,83756142,221}]
```

With Inline

```
[{decode,119448,29868742,250},
 {encode,101370,17324422,171}]
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

  |Optimization   |Encoding|Decoding|
  |---------------|--------|--------|
  |Layer Squashing|8x      |2x      |
  |Generated Funs |16x     |16x     |

* Erlang documentation on
  [Constructing and Matching Binaries](http://erlang.org/doc/efficiency_guide/binaryhandling.html),
  which may be useful for optimization.

* The
  [Thrift paper](https://thrift.apache.org/static/files/thrift-20070401.pdf),
  which describes the design and data encoding protocol for Thrift.
