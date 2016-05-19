all : test_types.hrl test_skip_types.hrl

%_service_thrift.erl %_service_thrift.hrl %_constants.hrl %_types.erl %_types.hrl : %.thrift
	thrift --gen erl:otp16 -out . $<

clean :
	rm -f *_types.erl *_types.hrl *_service_thrift.erl *_service_thrift.hrl *_constants.hrl
