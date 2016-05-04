<!-- -*- mode:gfm; word-wrap:nil -*- github-flavored markdown -->

The official Thrift documentation is a little unclear on the message
formats involved in RPCs.  This document describes my discoveries from
reading the code.

# Function Calls

Thrift supports two types of function calls, a normal `call` with a
return value (the return value may be void) and a `oneway` call
without a return value.

Note that there is no guarantee that a one-way call will be handled
asynchronously on the service side, so even though the client may
continue executing after sending the request instead of blocking on a
reply, if the client sends a second request over the same transport it
is possible that the server will not read the second request until it
finishes processing the first.

# Function Arguments.

For the arguments to a function call, Thrift sends a struct with a
field for each argument

For each function call, the client will send a `MessageBegin` with
`name` set to the function name, `type` set to `1`, and a sequence
number, `seq`.  This will be followed by struct with a field for each
argument in the function call.

I don't know whether it is a bug, but it appears that `MessageBegin`
`type` is `1` ("CALL") and not `4` ("ONEWAY"), even for `oneway`
functions.  This means that the server must independently look up the
reply type for the function.  This also means that it is unsafe to
change a function from `void` to `oneway void`, because if the client
and server disagree on whether on the function is `oneway` or not, the
client may receive an unexpected reply message, or may hang waiting
for a reply that the server doesn't send.

``` thrift
void myfun(1: i32 ifield, 2: string sfield)

```

``` erlang
thrift_client:myfun(7331, "xyzzy")
```

```
MessageBegin(name="myfun", type=call, seqid=0)
  StructBegin(name="myfun")
    FieldBegin(name="ifield", type=i32, id=1)
      I32(val=7331)
    FieldEnd()
    FieldBegin(name="sfield", type=string, id=2)
      String(val="xyzzy")
    FieldEnd()
    FieldStop()
  StructEnd()
MessageEnd()
```

# Function Return

Thrift creates a virtual dummy struct in which the first field is the
success return value from the function.  If the function can throw any
exceptions, this virtual struct also contains fields for each of the
exceptions.  However, since only one of the fields in this virtual
struct can be set (either the return value for a normal return or one
exception fields if the function returns a declared exception) and
since the Thrift message encoder skips undefined fields, only one of
the vields will actually be sent.

``` thrift
exception SimpleException {
  1:  string file
  2:  i32 line_number
  3:  string message
}

i32 myfun() throws (1: SimpleException e)

```

This example shows the reply message when the function returns the
value `7331`.  The field ID `0` is used for the success field; the
client uses this to distinguish a normal return from an exception.

```
MessageBegin(name="myfun", type=return, seqid=0) // The seqid 0 matches the request. 
  StructBegin(name="myfun_result") // name is not important (?)
    FieldBegin(name=?, type=struct, id=0) // id=0 used for normal return.
      I32(val=7331)
    FieldEnd()
    FieldStop()
  StructEnd()
MessageEnd()

```

This example shows the reply messages when the function returns a
declared exception.

```
MessageBegin(name="myfun", type=return, seqid=0) // Message type is 'return', not 'exception'!
  StructBegin(name="myfun") // name is not important (?)
    FieldBegin(name=?, type=i32, id=1) // id=1 corresponds to exception ID in thrift definition.
      StructBegin(name="SimpleException")
        FieldBegin(name="file", type=string, id=1)
  		  String(val="module.erl")
  		FieldEnd()
  		FieldBegin(name="line_number", type=i32, id=2)
  		  I32(val=123)
  		FieldEnd()
  		FieldBegin(name="message", type=string, id=3)
  		  String(val="an error occurred")
  		FieldEnd()
  		FieldStop()
      StringEnd()
    FieldEnd()
	FieldStop()
  StructEnd()
MessageEnd()
```

# Function Exception

When a Thrift function throws an undeclared exception, the Thrift
server returns an `exception` message.

```
MessageBegin(name="myfun", type=exception, seqid=0)
  StructBegin(name="TApplicationException")
    FieldBegin(name="message", type=string, id=1)
	  String(val="an uncaught exception occurred")
	FieldEnd()
	FieldBegin(name="type", type=i32, id=2)
	  I32(val=6) // 6 == internal error
	FieldEnd()
    FieldStop()
  StructEnd()
MessageEnd()
```

