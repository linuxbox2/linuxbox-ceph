
set(librados_srcs
	librados.cc 
	RadosClient.cc
	IoCtxImpl.cc 
)

include_directories("..")

add_library(librados ${librados_srcs})

add_dependencies(librados cls_lock osdc)

target_link_libraries(librados osdc cls_lock common ${CRYPTO_LIBS} ${EXTRALIBS})

set_target_properties(librados PROPERTIES VERSION 2.0.0 SOVERSION 2)


