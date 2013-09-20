cmake_minimum_required(VERSION 2.8.7)

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake/modules/")

project(Ceph)

include(CheckIncludeFiles)

CHECK_INCLUDE_FILES("stdint.h" HAVE_STDINT_H)
CHECK_INCLUDE_FILES("inttypes.h" HAVE_INTTYPES_H)
CHECK_INCLUDE_FILES("libaio.h" HAVE_LIBAIO)
CHECK_INCLUDE_FILES("atomic_ops.h" HAVE_ATOMIC_OPS)

add_definitions(-D__linux__)

set(Boost_USE_STATIC_LIBS   ON)
set(Boost_USE_MULTITHREADED ON)
find_package(Boost COMPONENTS thread system REQUIRED)
include_directories(${Boost_INCLUDE_DIRS})

find_package(Threads REQUIRED)

option(WITH_TCMALLOC "Use TCMalloc as Allocator" ON)

option(WITH_PROFILER "The Profiler is ON" ON)

add_subdirectory(src)

# example of boost libraries
# target_link_libraries(my_exe
#            ${CMAKE_THREAD_LIBS_INIT}
#            #...
#        ${Boost_LIBRARIES}
#    )


