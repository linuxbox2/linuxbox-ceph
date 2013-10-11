cmake_minimum_required(VERSION 2.8.7)

project(Ceph)

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake/modules/")

include_directories(  
  ${PROJECT_BINARY_DIR}/src/include
)

#Check Includes

include(CheckIncludeFiles)

#cmakedefine CEPH_HAVE_FALLOCATE no
#cmakedefine DARWIN no
#cmakedefine DEBUG_GATHER no 
#cmakedefine ENABLE_COVERAGE no
#cmakedefine FASTCGI_INCLUDE_DIR no
CHECK_INCLUDE_FILES("arpa/inet.h" HAVE_ARPA_INET_H)
#cmakedefine HAVE_BOOST_RANDOM_DISCRETE_DISTRIBUTION here
CHECK_INCLUDE_FILES("dirent.h" HAVE_DIRENT_H)
CHECK_INCLUDE_FILES("dlfcn.h" HAVE_DLFCN_H)
#cmakedefine HAVE_FIEMAP_H 1 no
#cmakedefine HAVE_FUSE_GETGROUPS 1 no
#/* linux/fiemap.h was found, fiemap ioctl will be used */
#cmakedefine HAVE_FIEMAP_H 1 no
#CHECK_INCLUDE_FILES("linux/fiemap.h" HAVE_FIEMAP_H)
#cmakedefine HAVE_FUSE_GETGROUPS 1 no
CHECK_INCLUDE_FILES("inttypes.h" HAVE_INTTYPES_H)
CHECK_INCLUDE_FILES("atomic_ops.h" HAVE_ATOMIC_OPS)
#cmakedefine HAVE_LEVELDB_FILTER_POLICY need to put in
#cmakedefine HAVE_LIBAIO need to put in
#CHECK_INCLUDE_FILES("libaio.h" HAVE_LIBAIO)
#cmakedefine HAVE_LIBBOOST_SYSTEM 1 here
#cmakedefine HAVE_LIBBOOST_SYSTEM_MT 1 here
#cmakedefine HAVE_LIBBOOST_THREAD 1 here
#cmakedefine HAVE_LIBBOOST_THREAD_MT 1 here
#cmakedefine HAVE_LIBFUSE need to put in

#cmakedefine HAVE_LIBLEVELDB 1  need to put in

#cmakedefine HAVE_LIBPROFILER 1 need to put in
#cmakedefine HAVE_LIBSNAPPY 1 need to put in
option(WITH_LIBSNAPPY "Snappy is here" ON)
if(${WITH_TCMALLOC})
find_package(tcmalloc)
set(HAVE_LIBTCMALLOC ${Tcmalloc_FOUND})
endif(${WITH_TCMALLOC})

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
#HAVE_SYNC_FS no idea
#HAVE_SYNC_FILERANGE no idea
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
set(CRYPTO_LIBS ${CRYPTOPP_LIBRARIES}) 
endif(CRYPTOPP_FOUND)
#cmakedefine USE_NSS no idea
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


