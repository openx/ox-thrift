all : test.thrift
	thrift --gen erl:otp16 -out . $<

clean :
	rm -f test_types.erl test_types.hrl test_service_thrift.erl test_service_thrift.hrl test_constants.hrl
