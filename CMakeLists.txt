cmake_minimum_required(VERSION 2.8.7)

project(Ceph)

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake/modules/")

include_directories(  
  ${PROJECT_BINARY_DIR}/src/include
)

#Check Includes

include(CheckIncludeFiles)
include(CheckFunctionExists)

CHECK_FUNCTION_EXISTS(fallocate CEPH_HAVE_FALLOCATE)
#cmakedefine DARWIN no
#cmakedefine DEBUG_GATHER no 
#cmakedefine ENABLE_COVERAGE no
CHECK_INCLUDE_FILES("fastcgi/fcgiapp.h" FASTCGI_INCLUDE_DIR)
CHECK_INCLUDE_FILES("arpa/inet.h" HAVE_ARPA_INET_H)
CHECK_INCLUDE_FILES("boost/random/discrete_distribution.hpp" HAVE_BOOST_RANDOM_DISCRETE_DISTRIBUTION)
CHECK_INCLUDE_FILES("dirent.h" HAVE_DIRENT_H)
CHECK_INCLUDE_FILES("dlfcn.h" HAVE_DLFCN_H)
#cmakedefine HAVE_FIEMAP_H 1 no
#cmakedefine HAVE_FUSE_GETGROUPS 1 no
#/* linux/fiemap.h was found, fiemap ioctl will be used */
#cmakedefine HAVE_FIEMAP_H 1 no
CHECK_INCLUDE_FILES("inttypes.h" HAVE_INTTYPES_H)
CHECK_INCLUDE_FILES("atomic_ops.h" HAVE_ATOMIC_OPS)
#cmakedefine HAVE_LEVELDB_FILTER_POLICY need to put in
CHECK_INCLUDE_FILES("leveldb/filter_policy.h" HAVE_LEVELDB_FILTER_POLICY)

#cmakedefine HAVE_LIBAIO need to put in
option(WITH_AIO "AIO is here ON" ON)
if(${WITH_AIO})
find_package(aio)
set(HAVE_LIBAIO ${AIO_FOUND})
message(STATUS "${AIO_LIBS}")
endif(${WITH_AIO})

#cmakedefine HAVE_LIBBOOST_SYSTEM 1 variable not used elsewhere define on the bottom
#cmakedefine HAVE_LIBBOOST_SYSTEM_MT 1 variable not used elsewhere defined on bottom
#cmakedefine HAVE_LIBBOOST_THREAD 1 here
#cmakedefine HAVE_LIBBOOST_THREAD_MT 1 here

#cmakedefine HAVE_LIBFUSE need to put in
option(WITH_FUSE "Fuse is here" ON)
if(${WITH_FUSE})
find_package(fuse)
set(HAVE_LIBFUSE ${FUSE_FOUND})
CHECK_FUNCTION_EXISTS(fuse_getgroups HAVE_FUSE_GETGROUPS)
endif(${WITH_FUSE})

#cmakedefine HAVE_LIBLEVELDB 1  need to put in
option(WITH_LEVELDB "LevelDB is here" ON)
if(${WITH_LEVELDB})
find_package(leveldb)
set(HAVE_LIBLEVELDB ${LEVELDB_FOUND})
endif(${WITH_LEVELDB})

#cmakedefine HAVE_LIBPROFILER 1 need to put in
option(WITH_PROFILER "The Profiler is here" ON)
if(${WITH_PROFILER})
find_package(profiler)
set(HAVE_PROFILER ${PROFILER_FOUND})
endif(${WITH_PROFILER})

#cmakedefine HAVE_LIBSNAPPY 1 need to put in
option(WITH_SNAPPY "Snappy is here" ON)
if(${WITH_SNAPPY})
find_package(snappy)
set(HAVE_LIBSNAPPY ${SNAPPY_FOUND})
endif(${WITH_SNAPPY})

#cmakedefine HAVE_LIBTCMALLOC
option(WITH_TCMALLOC "Use TCMalloc as Allocator" ON)
if(${WITH_TCMALLOC})
find_package(tcmalloc)
set(HAVE_LIBTCMALLOC ${Tcmalloc_FOUND})
endif(${WITH_TCMALLOC})

CHECK_INCLUDE_FILES("memory.h" HAVE_MEMORY_H)
CHECK_INCLUDE_FILES("ndir.h" HAVE_NDIR_H)
CHECK_INCLUDE_FILES("netdb.h" HAVE_NETDB_H)
CHECK_INCLUDE_FILES("netinet/in.h" HAVE_NETINET_IN_H)
#PROFILER need to put in
option(WITH_PROFILER "The Profiler is ON" ON)
#PTHREAD need to put in
CHECK_INCLUDE_FILES("stdint.h" HAVE_STDINT_H)
CHECK_INCLUDE_FILES("stdlib.h" HAVE_STDLIB_H)
CHECK_INCLUDE_FILES("arpa/inet.h" HAVE_ARPA_INET_H)
CHECK_INCLUDE_FILES("strings.h" HAVE_STRINGS_H)
CHECK_INCLUDE_FILES("string.h" HAVE_STRING_H)
CHECK_FUNCTION_EXISTS(sync_file_range HAVE_SYNC_FILE_RANGE)
CHECK_INCLUDE_FILES("syslog.h" HAVE_SYSLOG_H)
CHECK_INCLUDE_FILES("sys/dir.h" HAVE_SYS_DIR_H)
CHECK_INCLUDE_FILES("sys/file.h" HAVE_SYS_FILE_H)
CHECK_INCLUDE_FILES("sys/ioctl.h" HAVE_SYS_IOCTL_H)
CHECK_INCLUDE_FILES("sys/mount.h" HAVE_SYS_MOUNT_H)
CHECK_INCLUDE_FILES("sys/ndir.h" HAVE_SYS_NDIR_H)
CHECK_INCLUDE_FILES("sys/param.h" HAVE_SYS_PARAM_H)
CHECK_INCLUDE_FILES("sys/socket.h" HAVE_SYS_SOCKET_H)
CHECK_INCLUDE_FILES("sys/statvfs.h" HAVE_SYS_STATVFS_H)
CHECK_INCLUDE_FILES("sys/stat.h" HAVE_SYS_STAT_H)
CHECK_FUNCTION_EXISTS(syncfs HAVE_SYS_SYNCFS)
# HAVE_SYS_SYNCFS no idea
CHECK_INCLUDE_FILES("sys/time.h" HAVE_SYS_TIME_H)
CHECK_INCLUDE_FILES("sys/types.h" HAVE_SYS_TYPES_H)
CHECK_INCLUDE_FILES("sys/vfs.h" HAVE_SYS_VFS_H)
CHECK_INCLUDE_FILES("sys/wait.h" HAVE_SYS_WAIT_H)
CHECK_INCLUDE_FILES("sys/xattr.h" HAVE_SYS_XATTR_H)
CHECK_INCLUDE_FILES("unistd.h" HAVE_UNISTD_H)
CHECK_INCLUDE_FILES("utime.h" HAVE_UTIME_H)
#LT_OBJDIR no idea
#NO_ATOMIC_OPS no idea
#cmakedefine NO_MINUS_C_MINUS_O no idea

#cmakedefine PACKAGE no idea
#cmakedefine PACKAGE_BUGREPORT no idea
#cmakedefine PACKAGE_NAME no idea
#cmakedefine PACKAGE_STRING no idea
#cmakedefine PACKAGE_TARNAME no idea
#cmakedefine PACKAGE_URL no idea
#cmakedefine PACKAGE_VERSION no idea

#cmakedefine PG_DEBUG_REFS no idea
#cmakedefine PTHREAD_CREATE_JOINABLE no idea
#cmakedefine STDC_HEADERS no idea
option(USE_CRYPTOPP "Cryptopp is ON" ON)
find_package(cryptopp)
if(CRYPTOPP_FOUND)
MESSAGE(STATUS "${CRYPTOPP_LIBRARIES}")
set(CRYPTO_LIBS ${CRYPTOPP_LIBRARIES})
set(USE_NSS 0)
else()
MESSAGE(STATUS "Cryptopp not here using NSS instead")
set(USE_NSS 1)
MESSAGE(STATUS "${NSS_LIBRARIES}")
find_package(NSS)
	if(NSS_FOUND)
	MESSAGE(STATUS "${NSS_LIBRARIES}")
		if(NOT CRYPTOPP_FOUND)
		set(CRYPTO_LIBS ${NSS_LIBRARIES}) 
		endif(NOT CRYPTOPP_FOUND)
	endif(NSS_FOUND)
endif(CRYPTOPP_FOUND)
 
#cmakedefine VERSION no idea
#cmakedefine WITH_RADOSGW no idea

# Now create a useable config.h
configure_file(
  ${PROJECT_SOURCE_DIR}/src/include/config-h.in.cmake
  ${PROJECT_BINARY_DIR}/src/include/acconfig.h
)

add_definitions(-D__linux__)

set(Boost_USE_STATIC_LIBS   ON)
set(Boost_USE_MULTITHREADED ON)
find_package(Boost COMPONENTS thread system REQUIRED)
include_directories(${Boost_INCLUDE_DIRS})

find_package(Threads REQUIRED)

# find out which platform we are building on
if(${CMAKE_SYSTEM_NAME} MATCHES "Linux")
  set(LINUX ON)
  set(UNIX ON)
endif(${CMAKE_SYSTEM_NAME} MATCHES "Linux")

add_subdirectory(src)
