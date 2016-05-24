all : skip.thrift test_types.hrl skip_types.hrl

skip.thrift : test.thrift
	perl -p -e 'm/SKIP/ and $$_ = "";' -e 's/TestService/SkipService/;' $< >$@

%_service_thrift.erl %_service_thrift.hrl %_constants.hrl %_types.erl %_types.hrl : %.thrift
	thrift --gen erl:otp16 -out . $<

clean :
	rm -f skip.thrift *_types.erl *_types.hrl *_service_thrift.erl *_service_thrift.hrl *_constants.hrl
