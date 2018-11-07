# -*- mode: makefile-gmake -*-

REBAR3=rebar3
# export ERL_COMPILER_OPTIONS=bin_opt_info

all:
	$(REBAR3) compile

test:
	-$(REBAR3) dialyzer
	$(REBAR3) as test do xref,eunit,cover

_build/default/lib:
	$(REBAR3) get-deps

TAGS: src/*.erl src/*.hrl _build/default/lib
	etags $$(find src _build/default/lib \( -name '*.erl' -o -name '*.hrl' \) -print)

clean:
	if test -d _build; then $(REBAR3) clean; fi
	rm -rf _build/*/lib/ox_thrift

maintainer-clean: clean
	rm -rf _build TAGS ebin

version:
	@echo $(VERSION)

.PHONY: all test package release clean maintainer-clean
