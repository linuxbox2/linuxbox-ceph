
add_definitions(cxxflags ${CXXFLAGS})
add_definitions(cflags ${CFLAGS})
add_definitions(cryptoflags ${CRYPTO_CFLAGS})
add_definitions(cryptoflags -version-info 2:0:0 -export-symbols-regex '^rados_.* ${LDFLAGS})

set(srcs
/home/ali/ceph-local/src/librados/librados.cc 
/home/ali/ceph-local/src/librados/RadosClient.cc
/home/ali/ceph-local/src/librados/IoCtxImpl.cc 
/home/ali/ceph-local/src/osdc/Objecter.cc 
/home/ali/ceph-local/src/osdc/Striper.cc 
/home/ali/ceph-local/src/cls/lock/cls_lock_client.cc 
/home/ali/ceph-local/src/cls/lock/cls_lock_types.cc 
/home/ali/ceph-local/src/cls/lock/cls_lock_ops.cc
)

add_library(librados_srcs STATIC ${srcs})

#set(extralibs
#find_package(libuuid)
#set(CMAKE_MODULE_PATH ${extralibs} "$/home/ali/ceph-local/cmake/modules")
#)
#set()
#set()


# add_library(librados_libs 
# STATIC ${PTHREAD_LIBS}
# STATIC ${CRYPTO_LIBS}
# STATIC ${extralibs}
# )

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


message( STATUS "librados good is here" )


