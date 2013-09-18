message( STATUS "src cmake is here" )

add_definitions("-DCEPH_LIBDIR=${LIBRARY_OUTPUT_PATH}")

set(CMAKE_CXX_FLAGS "-Wno-invalid-offsetof" )

add_definitions("-DHAVE_CONFIG_H -D__CEPH__ -D_FILE_OFFSET_BITS=64 -D_REENTRANT -D_THREAD_SAFE -D__STDC_FORMAT_MACROS -D_GNU_SOURCE")

set(EXTRALIBS uuid rt)

set(PTHREAD_LIBS pthread)

set(CRYPTO_LIBS cryptopp)

set(LIBOS_LIBS leveldb snappy libos) 

set(LIBGLOBAL_LIBS ${EXTRALIBS} ${PTHREAD_LIBS} ${CRYPTO_LIBS} libglobal m)

add_subdirectory(common)
add_subdirectory(cls/lock)
add_subdirectory(osdc)
add_subdirectory(librados)
add_subdirectory(mon)
add_subdirectory(os)
add_subdirectory(global)

set(ceph_mon_srcs 
	ceph_mon.cc
	common/TextTable.cc
)

option(WITH_TCMALLOC "Use TCMalloc as Allocator" ON)

set(tcmalloc_safety_flags -fno-builtin-malloc -fno-builtin-calloc -fno-builtin-realloc -fno-builtin-fre)

if(${WITH_TCMALLOC})
  set(CEPH_MON_LIBS tcmalloc)
#  add_definitions("${tcmalloc_safety_flags}")
  set(ceph_mon_srcs
	perfglue/heap_profiler.cc
	${ceph_mon_srcs})
else()
  set(ceph_mon_srcs
	perfglue/disabled_heap_profiler.cc
	${ceph_mon_srcs})
endif(${WITH_TCMALLOC})

include_directories(".")

add_executable(ceph_mon ${ceph_mon_srcs})

target_link_libraries(ceph_mon libmon libcommon ${LIBOS_LIBS} ${LIBGLOBAL_LIBS} ${CEPH_MON_LIBS})

# tcmalloc?
#if WITH_TCMALLOC
#tcmalloc_safety_flags = -fno-builtin-malloc -fno-builtin-calloc -fno-builtin-realloc -fno-builtin-free
#ceph_osd_LDADD += -ltcmalloc
#ceph_osd_CXXFLAGS += ${tcmalloc_safety_flags}
#ceph_osd_SOURCES += perfglue/heap_profiler.cc
#ceph_mds_LDADD += -ltcmalloc
#ceph_mds_CXXFLAGS += ${tcmalloc_safety_flags}
#ceph_mds_SOURCES += perfglue/heap_profiler.cc
#ceph_mon_LDADD += -ltcmalloc
#ceph_mon_CXXFLAGS += ${tcmalloc_safety_flags}
#ceph_mon_SOURCES += perfglue/heap_profiler.cc
#if WITH_FUSE
#ceph_fuse_LDADD += -ltcmalloc
#ceph_fuse_CXXFLAGS += ${tcmalloc_safety_flags}
#endif #WITH_FUSE
#else
#ceph_osd_SOURCES += perfglue/disabled_heap_profiler.cc
#ceph_mds_SOURCES += perfglue/disabled_heap_profiler.cc
#ceph_mon_SOURCES += perfglue/disabled_heap_profiler.cc
#endif # WITH_TCMALLOC

