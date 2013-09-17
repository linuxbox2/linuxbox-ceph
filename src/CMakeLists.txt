#add_subdirectory(/home/ali/ceph-local/src/librados)

#message( STATUS "librados is here" )

#add_subdirectory(/home/ali/ceph-local/src/rgw)

#message( STATUS "rgw is here" )

add_definitions("-DCEPH_LIBDIR=${LIBRARY_OUTPUT_PATH}")

set(CMAKE_CXX_FLAGS "-Wno-invalid-offsetof" )

add_subdirectory(common)

message( STATUS "src cmake is here" )






