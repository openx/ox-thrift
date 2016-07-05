all : skip.thrift test_types.hrl skip_types.hrl

skip.thrift : test.thrift
	perl -p -e 'm/SKIP/ and $$_ = "";' -e 's/TestService/SkipService/;' $< >$@

%_service_thrift.erl %_service_thrift.hrl %_constants.hrl %_types.erl %_types.hrl : %.thrift
	thrift --gen erl:otp16 -out . $<
	perl -p -i \
	  -e '! $$type_fix and m/^$$/ and do { print "\n-include(\"namespaced_types.hrl\").\n"; $$type_fix = 1; };' \
	  -e 's/\b(dict|set)\(\)/remote_$$1()/g;' \
	  $*_types.hrl

clean :
	rm -f skip.thrift *_types.erl *_types.hrl *_service_thrift.erl *_service_thrift.hrl *_constants.hrl
