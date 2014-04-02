
set(LIBEDIT_LIBS edit)

set(libclient_srcs
	Client.cc 
	ClientSnapRealm.cc 
	Dentry.cc 
	Inode.cc 
	MetaRequest.cc 
	MetaSession.cc 
	Trace.cc
)

include_directories("..")

add_library(client ${libclient_srcs})

target_link_libraries(client osdc ${LIBEDIT_LIBS})
