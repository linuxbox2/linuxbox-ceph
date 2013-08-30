#test
message( STATUS "SRC is here" )

#librados_srcs

add_subdirectory(/home/ali/ceph-local/src/cls/rbd)

message( STATUS "rbd is here" )

add_subdirectory(/home/ali/ceph-local/src/cls/refcount)

message( STATUS "refcount is here" )

add_subdirectory(/home/ali/ceph-local/src/cls/rgw)

message( STATUS "rgw is here" )

add_subdirectory(/home/ali/ceph-local/src/cls/lock)

message( STATUS "lock is here" )

add_subdirectory(/home/ali/ceph-local/src/osdc)

message( STATUS "osdc is here" )

add_subdirectory(/home/ali/ceph-local/src/librados)

message( STATUS "librados is here" )

add_subdirectory(/home/ali/ceph-local/src/rgw)

message( STATUS "rgw is here" )

# sort out which allocator to use
set(ALLOCATOR "tcmalloc" CACHE STRING
  "specify the memory allocator to use: jemalloc|tcmalloc|libc")

if(${ALLOCATOR} STREQUAL "jemalloc")
  find_package(JeMalloc)
  if(JEMALLOC_FOUND)
    set(SYSTEM_LIBRARIES ${JEMALLOC_LIBRARIES})
  else(JEMALLOC_FOUND)
    message(WARNING "jemalloc not found, falling back to libc")
    set(ALLOCATOR "libc")
  endif(JEMALLOC_FOUND)
elseif(${ALLOCATOR} STREQUAL "tcmalloc")
  find_package(TcMalloc)
  if(TCMALLOC_FOUND)
    set(SYSTEM_LIBRARIES ${TCMALLOC_LIBRARIES})
  else(TCMALLOC_FOUND)
    message(WARNING "tcmalloc not found, falling back to libc")
    set(ALLOCATOR "libc")
  endif(TCMALLOC_FOUND)
else()
  if(NOT ${ALLOCATOR} STREQUAL "libc")
    message(SEND_ERROR "${ALLOCATOR}  is not a valid allocator option.")
  endif()
endif()
# maybe end that there
