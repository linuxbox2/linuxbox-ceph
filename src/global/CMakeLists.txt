
set(libglobal_srcs
	../global/global_context.cc 
	../global/global_init.cc 
	../global/pidfile.cc 
	../global/signal_handler.cc
)

add_library(global ${libglobal_srcs})


target_link_libraries(global common ${CMAKE_THREAD_LIBS_INIT} ${CRYPTO_LIBS} m ${EXTRALIBS})


