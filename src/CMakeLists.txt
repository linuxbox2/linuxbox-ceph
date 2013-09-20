add_definitions("-DCEPH_LIBDIR=${LIBRARY_OUTPUT_PATH}")

set(AM_COMMON_FLAGS "-DHAVE_CONFIG_H -D__CEPH__ -D_FILE_OFFSET_BITS=64 -D_REENTRANT -D_THREAD_SAFE -D__STDC_FORMAT_MACROS -D_GNU_SOURCE -rdynamic -Wall -Wtype-limits -Wignored-qualifiers -Winit-self -Wpointer-arith -Werror=format-security -fno-strict-aliasing -fsigned-char -fPIC")

set(CMAKE_CXX_FLAGS "-Wno-invalid-offsetof -Wnon-virtual-dtor -Wno-invalid-offsetof -Wstrict-null-sentinel")

add_definitions("${AM_COMMON_FLAGS}")

set(EXTRALIBS uuid rt profiler)

set(CRYPTO_LIBS cryptopp)

if (${HAVE_ATOMIC_OPS})
  set(EXTRALIBS
	atomic_ops
	${EXTRALIBS})
endif(${HAVE_ATOMIC_OPS})

set(LIBGLOBAL_LIBS libglobal ${CMAKE_THREAD_LIBS_INIT} ${CRYPTO_LIBS}  m ${EXTRALIBS})

add_subdirectory(common)
add_subdirectory(cls/lock)
add_subdirectory(osdc)
add_subdirectory(librados)
add_subdirectory(mon)
add_subdirectory(os)
add_subdirectory(global)
add_subdirectory(osd)
add_subdirectory(mds)

if(${WITH_TCMALLOC})
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-builtin-malloc -fno-builtin-calloc -fno-builtin-realloc -fno-builtin-free")
  set(TCMALLOC_LIBS tcmalloc)
  set(TCMALLOC_srcs perfglue/heap_profiler.cc)
else()
  set(TCMALLOC_srcs perfglue/disabled_heap_profiler.cc)
endif(${WITH_TCMALLOC})

include_directories(".")

#monitor
set(ceph_mon_srcs 
	ceph_mon.cc
	common/TextTable.cc
	${TCMALLOC_srcs}
)

add_executable(ceph-mon ${ceph_mon_srcs})

target_link_libraries(ceph-mon libmon boost_thread libcommon libos ${LIBGLOBAL_LIBS} ${TCMALLOC_LIBS})

# osd
set(ceph_osd_srcs
	ceph_osd.cc 
	objclass/class_debug.cc 
	objclass/class_api.cc
	${TCMALLOC_srcs}
)

 add_executable(ceph-osd ${ceph_osd_srcs})

 target_link_libraries(ceph-osd libosd libos ${LIBGLOBAL_LIBS} ${TCMALLOC_LIBS})

message( STATUS "ceph_osd: ${CMAKE_DL_LIBS}")

# mds
set(ceph_mds_srcs
	ceph_mds.cc
	${TCMALLOC_srcs}
)
add_executable(ceph-mds ${ceph_mds_srcs})

target_link_libraries(ceph-mds libmds libosdc ${CMAKE_DL_LIBS} ${LIBGLOBAL_LIBS} ${TCMALLOC_LIBS} boost_thread)

message( STATUS "src cmake is here" )

