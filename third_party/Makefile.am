BUILT_SOURCES =
CLEANFILES =

SUBCONFIG_ARGS = @SUBCONFIG_ARGS@ --srcdir=.
SUBMAKEFLAGS = $(AM_MAKEFLAGS)

GLOG_VERSION = 0.3.1
GMOCK_VERSION = 1.6.0
ZOOKEEPER_VERSION = 3.3.1
PROTOBUF_VERSION = 2.3.0
BOOST_VERSION = 1.37.0
DISTRIBUTE_VERSION = 0.6.19
BOTO_VERSION = 2.0b2

BOOST = boost-$(BOOST_VERSION)
BOTO = boto-$(BOTO_VERSION)
DISTRIBUTE = distribute-$(DISTRIBUTE_VERSION)
GLOG = glog-$(GLOG_VERSION)
GMOCK = gmock-$(GMOCK_VERSION)
GTEST = $(GMOCK)/gtest
LEVELDB = leveldb
PROTOBUF = protobuf-$(PROTOBUF_VERSION)
LIBPROCESS = libprocess
ZOOKEEPER = zookeeper-$(ZOOKEEPER_VERSION)

EXTRA_DIST = \
	     $(BOOST).tar.gz \
	     $(BOTO).tar.gz \
	     $(DISTRIBUTE)/distribute-$(DISTRIBUTE_VERSION)-py2.6.egg \
	     $(GLOG).tar.gz \
	     $(GMOCK).tar.gz \
	     $(LEVELDB).tar.gz \
	     $(LIBPROCESS).tar.gz \
	     $(PROTOBUF).tar.gz \
	     $(ZOOKEEPER).tar.gz

CLEAN_EXTRACTED = \
		  $(BOOST) \
		  $(BOTO) \
		  $(GLOG) \
		  $(GMOCK) \
		  $(LEVELDB) \
		  $(LIBPROCESS) \
		  $(PROTOBUF) \
		  $(ZOOKEEPER)

check_LTLIBRARIES = libgmock.la
nodist_libgmock_la_SOURCES = $(GTEST)/src/gtest-all.cc $(GMOCK)/src/gmock-all.cc
libgmock_la_CPPFLAGS = -I$(GTEST)/include         \
                       -I$(GMOCK)/include         \
                       -I$(GTEST)                 \
                       -I$(GMOCK)

BUILT_SOURCES += $(nodist_libgmock_la_SOURCES)

%-stamp:: %.tar.gz
	gzip -d -c $^ | tar xf -
	touch $@

$(GTEST)/src/gtest-all.cc: $(GMOCK)-stamp
$(GMOCK)/src/gmock-all.cc: $(GMOCK)-stamp

$(GLOG)/libglog.la: $(GLOG)-stamp
	cd $(GLOG) && ./configure $(SUBCONFIG_ARGS) && $(MAKE) $(SUBMAKEFLAGS)

$(LIBPROCESS)/libprocess.a: $(LIBPROCESS)-stamp
	cd $(LIBPROCESS) && ./configure $(SUBCONFIG_ARGS) && $(MAKE) $(SUBMAKEFLAGS)

$(ZOOKEEPER)/src/c/libzookeeper_mt.la: $(ZOOKEEPER)-stamp
	cd $(ZOOKEEPER)/src/c && ./configure $(SUBCONFIG_ARGS) && $(MAKE) $(SUBMAKEFLAGS)

# TODO(charles): Figure out PIC options in our configure or create
# 		 an configure.ac for leveldb.
$(LEVELDB)/libleveldb.a: $(LEVELDB)-stamp
	cd $(LEVELDB) && $(MAKE) $(SUBMAKEFLAGS) CC="$(CXX)" OPT="$(CXXFLAGS) -fPIC"

$(PROTOBUF)/src/protoc $(PROTOBUF)/src/libprotobuf.la: $(PROTOBUF)-build-stamp

$(PROTOBUF)-build-stamp: $(PROTOBUF)-stamp
	cd $(PROTOBUF) && ./configure $(SUBCONFIG_ARGS) && $(MAKE) $(SUBMAKEFLAGS)
	touch $@

$(BOOST)/boost: $(BOOST)-stamp

THIRD_PARTY_LIBS = $(GLOG)/libglog.la $(LIBPROCESS)/libprocess.a \
	   $(PROTOBUF)/src/libprotobuf.la $(PROTOBUF)/src/protoc \
	   $(LEVELDB)/libleveldb.a $(DISTRIBUTE) $(BOTO)-stamp $(BOOST)-stamp

if WITH_INCLUDED_ZOOKEEPER
THIRD_PARTY_LIBS += $(ZOOKEEPER)/src/c/libzookeeper_mt.la
endif

all-local: $(THIRD_PARTY_LIBS)

clean-local:
	rm -r -f $(CLEAN_EXTRACTED)
	rm -f *-stamp
