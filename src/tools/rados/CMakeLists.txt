set(rados_srcs
	rados.cc 
	rados_import.cc 
	rados_export.cc 
	rados_sync.cc 
	${CMAKE_SOURCE_DIR}/src/common/obj_bencher.cc
)
add_executable(rados ${rados_srcs})
target_link_libraries(rados cls_lock librados global ${LTTNG_LIBS})

install(TARGETS rados DESTINATION bin)

