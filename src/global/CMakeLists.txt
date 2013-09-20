message( STATUS "libglobal is here" )

set(libglobal_srcs
	../global/global_context.cc 
	../global/global_init.cc 
	../global/pidfile.cc 
	../global/signal_handler.cc
)

include_directories("..")

add_library(libglobal ${libglobal_srcs})


target_link_libraries(libglobal libcommon ${CMAKE_THREAD_LIBS_INIT} ${CRYPTO_LIBS}  m ${EXTRALIBS})


