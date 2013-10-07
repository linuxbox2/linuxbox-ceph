message( STATUS "librados is here" )

set(librados_srcs
	../librados/librados.cc 
	../librados/RadosClient.cc
	../librados/IoCtxImpl.cc 
)

include_directories("..")

add_library(librados ${librados_srcs})

add_dependencies(librados cls_lock osdc)

target_link_libraries(librados osdc cls_lock common ${CRYPTO_LIBS} ${EXTRALIBS})

set_target_properties(librados PROPERTIES VERSION 2.0.0 SOVERSION 1)

# librados
#librados_SOURCES = \
#	librados/librados.cc \
#	librados/RadosClient.cc \
#	librados/IoCtxImpl.cc \
#	osdc/Objecter.cc \
#	osdc/Striper.cc \
#	cls/lock/cls_lock_client.cc \
#	cls/lock/cls_lock_types.cc \
#	cls/lock/cls_lock_ops.cc
#librados_la_SOURCES = ${librados_SOURCES}
#librados_la_CFLAGS = ${CRYPTO_CFLAGS} ${AM_CFLAGS}
#librados_la_CXXFLAGS = ${AM_CXXFLAGS}
#librados_la_LIBADD = libcommon.la $(PTHREAD_LIBS) $(CRYPTO_LIBS) $(EXTRALIBS)
#librados_la_LDFLAGS = ${AM_LDFLAGS} -version-info 2:0:0 -export-symbols-regex '^rados_.*'
#lib_LTLIBRARIES += librados.la

