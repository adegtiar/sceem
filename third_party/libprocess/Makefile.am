# Makefile for libprocess. Note that third_party needs to be built
# first (see third_party/Makefile.am).

ACLOCAL_AMFLAGS = -I m4

AUTOMAKE_OPTIONS = foreign

SUBDIRS = third_party .

include third_party/versions.am

BOOST = third_party/boost-$(BOOST_VERSION)
GLOG = third_party/glog-$(GLOG_VERSION)
GMOCK = third_party/gmock-$(GMOCK_VERSION)
GTEST = $(GMOCK)/gtest
RY_HTTP_PARSER = third_party/ry-http-parser-$(RY_HTTP_PARSER_VERSION)
LIBEV = third_party/libev-$(LIBEV_VERSION)


# Library. It is not installable presently because most people link
# the libprocess statically into their resulting library or binary and
# don't want any parts of libprocess to get installed (which happens
# even if you attempt to do conditional installation via configure
# arguments, see configure.ac).
noinst_LTLIBRARIES = libprocess.la

libprocess_la_SOURCES = src/process.cpp src/pid.cpp src/latch.cpp	\
	src/tokenize.cpp src/config.hpp src/decoder.hpp			\
	src/encoder.hpp src/foreach.hpp src/gate.hpp			\
	src/synchronized.hpp src/thread.hpp src/tokenize.hpp

libprocess_la_CPPFLAGS = -I$(srcdir)/include -I$(BOOST) -I$(GLOG)/src	\
	-I$(RY_HTTP_PARSER) -I$(LIBEV) $(AM_CPPFLAGS)

libprocess_la_LIBADD = $(GLOG)/libglog.la				\
	third_party/libry_http_parser.la $(LIBEV)/libev.la


# Headers.
libprocess_la_SOURCES += $(top_srcdir)/include/process/clock.hpp	\
	$(top_srcdir)/include/process/collect.hpp			\
	$(top_srcdir)/include/process/defer.hpp				\
	$(top_srcdir)/include/process/deferred.hpp			\
	$(top_srcdir)/include/process/dispatch.hpp			\
	$(top_srcdir)/include/process/event.hpp				\
	$(top_srcdir)/include/process/executor.hpp			\
	$(top_srcdir)/include/process/filter.hpp			\
	$(top_srcdir)/include/process/future.hpp			\
	$(top_srcdir)/include/process/gc.hpp				\
	$(top_srcdir)/include/process/http.hpp				\
	$(top_srcdir)/include/process/latch.hpp				\
	$(top_srcdir)/include/process/message.hpp			\
	$(top_srcdir)/include/process/option.hpp			\
	$(top_srcdir)/include/process/pid.hpp				\
	$(top_srcdir)/include/process/preprocessor.hpp			\
	$(top_srcdir)/include/process/process.hpp			\
	$(top_srcdir)/include/process/protobuf.hpp			\
	$(top_srcdir)/include/process/run.hpp				\
	$(top_srcdir)/include/process/socket.hpp			\
	$(top_srcdir)/include/process/timeout.hpp			\
	$(top_srcdir)/include/process/timer.hpp


# Tests.
check_PROGRAMS = tests

tests_SOURCES = src/tests.cpp
tests_CPPFLAGS = -I$(GTEST)/include -I$(GMOCK)/include $(libprocess_la_CPPFLAGS)
tests_LDADD = third_party/libgmock.la libprocess.la
