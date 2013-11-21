
set(LIBEDIT_LIBS edit)

set(libclient_srcs
	../client/Client.cc 
	../client/barrier.cc
	../client/Inode.cc 
	../client/Dentry.cc 
	../client/MetaRequest.cc 
	../client/ClientSnapRealm.cc 
	../client/MetaSession.cc 
	../client/Trace.cc
)

include_directories("..")

add_library(client ${libclient_srcs})

target_link_libraries(client osdc ${LIBEDIT_LIBS})
