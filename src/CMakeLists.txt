add_definitions("-DCEPH_LIBDIR=${LIBRARY_OUTPUT_PATH}")

set(AM_COMMON_FLAGS "-DHAVE_CONFIG_H -D__CEPH__ -D_FILE_OFFSET_BITS=64 -D_REENTRANT -D_THREAD_SAFE -D__STDC_FORMAT_MACROS -D_GNU_SOURCE -rdynamic -Wall -Wtype-limits -Wignored-qualifiers -Winit-self -Wpointer-arith -Werror=format-security -fno-strict-aliasing -fsigned-char -fPIC")

set(CMAKE_CXX_FLAGS "${CMAKE_C_FLAGS} -Wno-invalid-offsetof -Wnon-virtual-dtor -Wno-invalid-offsetof -Wstrict-null-sentinel")

if (HAVE_XIO)

  set(CMAKE_CXX_FLAGS "${CMAKE_C_FLAGS} -I${Xio_INCLUDE_DIR} -DHAVE_XIO")

endif(HAVE_XIO)

add_definitions("${AM_COMMON_FLAGS}")

set(EXTRALIBS uuid rt profiler dl)

set(CRYPTO_LIBS cryptopp)

if(${HAVE_ATOMIC_OPS})
  set(EXTRALIBS
	atomic_ops
	${EXTRALIBS})
endif(${HAVE_ATOMIC_OPS})

set(bindir ${CMAKE_INSTALL_PREFIX}/bin)
set(sbindir ${CMAKE_INSTALL_PREFIX}/sbin)
set(libdir ${CMAKE_INSTALL_PREFIX}/lib)
set(sysconfdir ${CMAKE_INSTALL_PREFIX}/etc)
set(prefix ${CMAKE_INSTALL_PREFIX})

if (HAVE_XIO)
  set(EXTRALIBS ${EXTRALIBS}
    ${Xio_LIBRARY} ibverbs rdmacm pthread rt)
endif (HAVE_XIO)

set(GCOV_PREFIX_STRIP 4)

# Python stuff
find_package(PythonInterp 2 QUIET)
if(NOT PYTHONINTERP_FOUND)
  message(FATAL_ERROR "Python 2 interpreter not found.")
endif(NOT PYTHONINTERP_FOUND)

# if CMAKE_INSTALL_PREFIX is an empty string, must replace
# it with "/" to make PYTHON_INSTALL_TEMPLATE an absolute path to be
# consistent with all other installation paths.
if(CMAKE_INSTALL_PREFIX)
  set(PYTHON_INSTALL_TEMPLATE "${CMAKE_INSTALL_PREFIX}")
else(CMAKE_INSTALL_PREFIX)
  set(PYTHON_INSTALL_TEMPLATE "/")
endif(CMAKE_INSTALL_PREFIX)

execute_process(
  COMMAND
  ${PYTHON_EXECUTABLE} -c "from distutils import sysconfig; print sysconfig.get_python_lib(1,0,prefix='${PYTHON_INSTALL_TEMPLATE}')"
  OUTPUT_VARIABLE PYTHON_INSTDIR
  OUTPUT_STRIP_TRAILING_WHITESPACE
  )

configure_file(
${CMAKE_SOURCE_DIR}/src/ceph.in.cmake
${CMAKE_BINARY_DIR}/ceph
@ONLY
)


configure_file(
${CMAKE_SOURCE_DIR}/src/ceph-crush-location.in
${CMAKE_BINARY_DIR}/ceph-crush-location
@ONLY
)


configure_file(
${CMAKE_SOURCE_DIR}/src/init-ceph.in
${CMAKE_BINARY_DIR}/init-ceph
@ONLY
)

configure_file(
${CMAKE_SOURCE_DIR}/src/mkcephfs.in
${CMAKE_BINARY_DIR}/mkcephfs
@ONLY
)

configure_file(
${CMAKE_SOURCE_DIR}/src/ceph-coverage.in
${CMAKE_BINARY_DIR}/ceph-coverage
@ONLY
)

configure_file(
${CMAKE_SOURCE_DIR}/src/ceph-debugpack.in
${CMAKE_BINARY_DIR}/ceph-debugpack
@ONLY
)

include_directories(".")

add_subdirectory(common)
add_subdirectory(osdc)
add_subdirectory(cls)
add_subdirectory(librados)
add_subdirectory(mon)
add_subdirectory(os)
add_subdirectory(global)
add_subdirectory(osd)
add_subdirectory(mds)
add_subdirectory(client)
add_subdirectory(tools)
add_subdirectory(test)

if(${WITH_RADOSGW})
add_subdirectory(rgw)
endif(${WITH_RADOSGW})

if(${WITH_TCMALLOC})
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-builtin-malloc -fno-builtin-calloc -fno-builtin-realloc -fno-builtin-free")
  set(TCMALLOC_LIBS tcmalloc)
  set(TCMALLOC_srcs perfglue/heap_profiler.cc)
else()
  set(TCMALLOC_srcs perfglue/disabled_heap_profiler.cc)
endif(${WITH_TCMALLOC})

#monitor
set(ceph_mon_srcs 
	ceph_mon.cc
	common/TextTable.cc
	${TCMALLOC_srcs}
)

add_executable(ceph-mon ${ceph_mon_srcs})

target_link_libraries(ceph-mon mon boost_regex boost_thread common os global ${TCMALLOC_LIBS})

# osd
set(ceph_osd_srcs
	ceph_osd.cc 
	objclass/class_api.cc
	${TCMALLOC_srcs}
)

add_executable(ceph-osd ${ceph_osd_srcs})

target_link_libraries(ceph-osd osd os global ${TCMALLOC_LIBS})

#mds
set(ceph_mds_srcs
	ceph_mds.cc
	${TCMALLOC_srcs}
)
add_executable(ceph-mds ${ceph_mds_srcs})

target_link_libraries(ceph-mds mds osdc ${CMAKE_DL_LIBS} global ${TCMALLOC_LIBS} boost_thread)

# admin tools

if(FALSE) # removed due to conflict with libcephfs target
	# cephfs
	set(cephfs_srcs cephfs.cc)
	add_executable(cephfs ${cephfs_srcs})
	target_link_libraries(cephfs common)
	install(TARGETS cephfs DESTINATION bin)
endif(FALSE)

# synthetic client
set(ceph_syn_srcs
	ceph_syn.cc 
	client/SyntheticClient.cc
)

add_executable(ceph-syn ${ceph_syn_srcs})
target_link_libraries(ceph-syn client global)


#librados_config_srcs
set(librados_config_srcs
	librados-config.cc 
)

add_executable(librados-config ${librados_config_srcs})
target_link_libraries(librados-config librados global)


#ceph-fuse
if(WITH_FUSE)
set(ceph_fuse_srcs
ceph_fuse.cc
client/fuse_ll.cc
)

add_executable(ceph-fuse ${ceph_fuse_srcs})
target_link_libraries(ceph-fuse fuse client global)
set(ceph_fuse_bin "ceph-fuse")
endif(WITH_FUSE)

# add the install targets
install(TARGETS
  ceph-mon
  ceph-osd
  ceph-mds
  ${ceph_fuse_bin}
  ceph-syn
  librados-config
  DESTINATION bin)

install(DIRECTORY
pybind
DESTINATION bin
) 

install(PROGRAMS
  ${CMAKE_BINARY_DIR}/ceph-crush-location
  ${CMAKE_BINARY_DIR}/init-ceph
  ${CMAKE_BINARY_DIR}/mkcephfs
  ${CMAKE_SOURCE_DIR}/src/vstart.sh
  ${CMAKE_SOURCE_DIR}/src/ceph_common.sh
  ${CMAKE_BINARY_DIR}/ceph
  DESTINATION bin)

install(DIRECTORY ${CMAKE_SOURCE_DIR}/src/pybind/
  DESTINATION ${PYTHON_INSTDIR})


