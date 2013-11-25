add_definitions("-DCEPH_LIBDIR=${LIBRARY_OUTPUT_PATH}")

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}")

include_directories("."
  "${PROJECT_SOURCE_DIR}/src/")

set(simple_server_srcs
	simple_server.cc
	address_helper.cc
	simple_dispatcher.cc
	${TCMALLOC_srcs}
)

add_executable(simple_server ${simple_server_srcs})

target_link_libraries(simple_server os global common boost_regex
  ${TCMALLOC_LIBS})

set(simple_client_srcs
	simple_client.cc
	address_helper.cc
	simple_dispatcher.cc
	${TCMALLOC_srcs}
)
add_executable(simple_client ${simple_client_srcs})

target_link_libraries(simple_client os global common boost_regex
  ${TCMALLOC_LIBS})

if (HAVE_XIO)

set(CMAKE_CXX_FLAGS "${CMAKE_C_FLAGS} -I${Xio_INCLUDE_DIR}")

set(xio_server_srcs
	xio_server.cc
	address_helper.cc
	simple_dispatcher.cc
	${TCMALLOC_srcs}
)
add_executable(xio_server ${xio_server_srcs})
target_link_libraries(xio_server os global common boost_regex ${TCMALLOC_LIBS}
  ${Xio_LIBRARY} ibverbs rdmacm pthread rt)

set(xio_client_srcs
	xio_client.cc
	address_helper.cc
	simple_dispatcher.cc
	${TCMALLOC_srcs}
)
add_executable(xio_client ${xio_client_srcs})
target_link_libraries(xio_client os global common boost_regex ${TCMALLOC_LIBS}
  ${Xio_LIBRARY} ibverbs rdmacm pthread rt)

endif(HAVE_XIO)