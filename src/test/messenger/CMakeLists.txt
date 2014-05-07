add_definitions("-DCEPH_LIBDIR=${LIBRARY_OUTPUT_PATH}")

include_directories("."
  "${PROJECT_SOURCE_DIR}/src/")

if(${WITH_TCMALLOC})
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-builtin-malloc -fno-builtin-calloc -fno-builtin-realloc -fno-builtin-free")
  set(TCMALLOC_LIBS tcmalloc)
  set(TCMALLOC_srcs
    ${PROJECT_SOURCE_DIR}/src/perfglue/heap_profiler.cc)
else()
  set(TCMALLOC_srcs
    ${PROJECT_SOURCE_DIR}/src/perfglue/disabled_heap_profiler.cc)
endif(${WITH_TCMALLOC})

set(simple_server_srcs
	simple_server.cc
	simple_dispatcher.cc
	${TCMALLOC_srcs}
)

add_executable(simple_server ${simple_server_srcs})

target_link_libraries(simple_server os global common boost_regex
  ${TCMALLOC_LIBS})

set(simple_client_srcs
	simple_client.cc
	simple_dispatcher.cc
	${TCMALLOC_srcs}
)
add_executable(simple_client ${simple_client_srcs})

target_link_libraries(simple_client os global common boost_regex
  ${TCMALLOC_LIBS})

if (HAVE_XIO)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -I${Xio_INCLUDE_DIR}")

set(xio_server_srcs
	xio_server.cc
	xio_dispatcher.cc
	${TCMALLOC_srcs}
)
add_executable(xio_server ${xio_server_srcs})
target_link_libraries(xio_server os global common boost_regex ${TCMALLOC_LIBS}
  ${Xio_LIBRARY} ibverbs rdmacm pthread rt)

set(xio_client_srcs
	xio_client.cc
	xio_dispatcher.cc
	${TCMALLOC_srcs}
)
add_executable(xio_client ${xio_client_srcs})
target_link_libraries(xio_client os global common boost_regex ${TCMALLOC_LIBS}
  ${Xio_LIBRARY} ibverbs rdmacm pthread rt)

endif(HAVE_XIO)