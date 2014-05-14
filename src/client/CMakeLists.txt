
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

add_library(client ${libclient_srcs})

target_link_libraries(client osdc boost_regex ${LIBEDIT_LIBS})

# libcephfs
if (WITH_LIBCEPHFS)
	set(libcephfs_srcs ../libcephfs.cc)
	add_library(cephfs SHARED ${libcephfs_srcs})
	target_link_libraries(cephfs client global)
	install(TARGETS cephfs DESTINATION lib)
	install(DIRECTORY "${CMAKE_SOURCE_DIR}/src/include/cephfs" DESTINATION include)
endif (WITH_LIBCEPHFS)
