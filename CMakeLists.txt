cmake_minimum_required(VERSION 2.8.7)

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake/modules/")

project(Ceph)

add_subdirectory(/home/ali/ceph-local/src)

include(CheckIncludeFiles)

CHECK_INCLUDE_FILES("stdint.h" HAVE_STDINT_H)
CHECK_INCLUDE_FILES("inttypes.h" HAVE_INTTYPES_H)

add_definitions(-D__linux__)

# Find packages and libs we need for building
#include(CheckIncludeFiles)
#include(CheckLibraryExists)
#include(CheckCSourceCompiles)
#include(TestBigEndian)

#check_include_files(stdbool.h HAVE_STDBOOL_H)
#check_include_files(strings.h HAVE_STRINGS_H)
#check_include_files(string.h HAVE_STRING_H)

#find_package(libuuid)
#set(CMAKE_MODULE_PATH ${extralibs} "$/home/ali/ceph-local/cmake/modules")

#find_package(cryptopp)
#set(CMAKE_MODULE_PATH ${cryptopp} "$/home/ali/ceph-local/cmake/modules")

#find_package(threads)
#set(CMAKE_MODULE_PATH ${threads} "$/home/ali/ceph-local/cmake/modules")

# message(STATUS" do I need to target link libraries this these things!?")
# do I need to 
#target_link_libraries(pthreads ${CMAKE_THREAD_LIBS_INIT})


# sort out which allocator to use
#set(ALLOCATOR "tcmalloc" CACHE STRING
#  "specify the memory allocator to use: jemalloc|tcmalloc|libc")
#if(${ALLOCATOR} STREQUAL "tcmalloc")
#  find_package(TcMalloc)
#  if(TCMALLOC_FOUND)
#    set(SYSTEM_LIBRARIES ${TCMALLOC_LIBRARIES})
#  else(TCMALLOC_FOUND)
#    message(WARNING "tcmalloc not found, falling back to libc")
#    set(ALLOCATOR "libc")
#  endif(TCMALLOC_FOUND)
#else()
#  if(NOT ${ALLOCATOR} STREQUAL "libc")
#    message(SEND_ERROR "${ALLOCATOR}  is not a valid allocator option.")
#  endif()
# endif()

