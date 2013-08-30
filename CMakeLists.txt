cmake_minimum_required(VERSION 2.8.7)

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake/modules/")

project(Ceph)

add_subdirectory(/home/ali/ceph-local/src)
add_subdirectory(/home/ali/ceph-local/man)
add_subdirectory(/home/ali/ceph-local/wireshark/ceph)


