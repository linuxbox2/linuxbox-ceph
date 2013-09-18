cmake_minimum_required(VERSION 2.8.7)

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake/modules/")

project(Ceph)

add_subdirectory(/home/ali/ceph-local/src)

include(CheckIncludeFiles)

CHECK_INCLUDE_FILES("stdint.h" HAVE_STDINT_H)
CHECK_INCLUDE_FILES("inttypes.h" HAVE_INTTYPES_H)

add_definitions(-D__linux__)




