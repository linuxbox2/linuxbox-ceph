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
CHECK_FUNCTION_EXISTS(syncfs HAVE_SYS_SYNCFS) 
CHECK_FUNCTION_EXISTS(sync_file_range HAVE_SYNC_FILE_RANGE)
CHECK_INCLUDE_FILES("fastcgi/fcgiapp.h" FASTCGI_INCLUDE_DIR)
CHECK_INCLUDE_FILES("arpa/inet.h" HAVE_ARPA_INET_H)
CHECK_INCLUDE_FILES("boost/random/discrete_distribution.hpp" HAVE_BOOST_RANDOM_DISCRETE_DISTRIBUTION)
CHECK_INCLUDE_FILES("dirent.h" HAVE_DIRENT_H)
CHECK_INCLUDE_FILES("dlfcn.h" HAVE_DLFCN_H)
CHECK_INCLUDE_FILES("inttypes.h" HAVE_INTTYPES_H)
CHECK_INCLUDE_FILES("atomic_ops.h" HAVE_ATOMIC_OPS)
CHECK_INCLUDE_FILES("leveldb/filter_policy.h" HAVE_LEVELDB_FILTER_POLICY)
CHECK_INCLUDE_FILES("memory.h" HAVE_MEMORY_H)
CHECK_INCLUDE_FILES("ndir.h" HAVE_NDIR_H)
CHECK_INCLUDE_FILES("netdb.h" HAVE_NETDB_H)
CHECK_INCLUDE_FILES("netinet/in.h" HAVE_NETINET_IN_H)
CHECK_INCLUDE_FILES("stdint.h" HAVE_STDINT_H)
CHECK_INCLUDE_FILES("stdlib.h" HAVE_STDLIB_H)
CHECK_INCLUDE_FILES("arpa/inet.h" HAVE_ARPA_INET_H)
CHECK_INCLUDE_FILES("strings.h" HAVE_STRINGS_H)
CHECK_INCLUDE_FILES("string.h" HAVE_STRING_H)
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
CHECK_INCLUDE_FILES("sys/time.h" HAVE_SYS_TIME_H)
CHECK_INCLUDE_FILES("sys/types.h" HAVE_SYS_TYPES_H)
CHECK_INCLUDE_FILES("sys/vfs.h" HAVE_SYS_VFS_H)
CHECK_INCLUDE_FILES("sys/wait.h" HAVE_SYS_WAIT_H)
CHECK_INCLUDE_FILES("sys/xattr.h" HAVE_SYS_XATTR_H)
CHECK_INCLUDE_FILES("unistd.h" HAVE_UNISTD_H)
CHECK_INCLUDE_FILES("utime.h" HAVE_UTIME_H)
CHECK_INCLUDE_FILES("linux/fiemap.h" HAVE_FIEMAP_H)

option(DEBUG_GATHER "C_Gather debugging" OFF)
option(WITH_RADOSGW "Rados Gateway is enabled" ON)
option(PG_DEBUG_THREADS "pg ref debugging" OFF)
option(ENABLE_COVERAGE "Enabling coverage" OFF)

option(WITH_AIO "AIO is here ON" ON)
if(${WITH_AIO})
find_package(aio)
set(HAVE_LIBAIO ${AIO_FOUND})
message(STATUS "${AIO_LIBS}")
endif(${WITH_AIO})

option(WITH_FUSE "Fuse is here" ON)
if(${WITH_FUSE})
find_package(fuse)
set(HAVE_LIBFUSE ${FUSE_FOUND})
CHECK_FUNCTION_EXISTS(fuse_getgroups HAVE_FUSE_GETGROUPS)
endif(${WITH_FUSE})

option(WITH_LEVELDB "LevelDB is here" ON)
if(${WITH_LEVELDB})
find_package(leveldb)
set(HAVE_LIBLEVELDB ${LEVELDB_FOUND})
CHECK_INCLUDE_FILES("leveldb/filter_policy.h" HAVE_LEVELDB_FILTER_POLICY)
endif(${WITH_LEVELDB})

option(WITH_PROFILER "The Profiler is here" ON)
if(${WITH_PROFILER})
find_package(profiler)
set(HAVE_PROFILER ${PROFILER_FOUND})
endif(${WITH_PROFILER})

option(WITH_SNAPPY "Snappy is here" ON)
if(${WITH_SNAPPY})
find_package(snappy)
set(HAVE_LIBSNAPPY ${SNAPPY_FOUND})
endif(${WITH_SNAPPY})

option(WITH_TCMALLOC "Use TCMalloc as Allocator" ON)
if(${WITH_TCMALLOC})
find_package(tcmalloc)
set(HAVE_LIBTCMALLOC ${Tcmalloc_FOUND})
endif(${WITH_TCMALLOC})

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
  FIND_PACKAGE(Threads)
endif(${CMAKE_SYSTEM_NAME} MATCHES "Linux")

# find out which platform we are building on
if(${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
set(OperatingSystem "Mac OS X")  
endif(${CMAKE_SYSTEM_NAME} MATCHES "Darwin")

add_subdirectory(src)
