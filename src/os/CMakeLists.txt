set(libos_srcs
	FileJournal.cc 
	FileStore.cc 
	chain_xattr.cc 
	ObjectStore.cc 
	JournalingObjectStore.cc 
	LFNIndex.cc 
	HashIndex.cc 
	IndexManager.cc 
	FlatIndex.cc 
	DBObjectMap.cc 
	LevelDBStore.cc 
	WBThrottle.cc
	GenericFileStoreBackend.cc
	BtrfsFileStoreBackend.cc
	ZFSFileStoreBackend.cc
)

add_library(os ${libos_srcs})

if(${HAVE_LIBAIO})
target_link_libraries(os aio)
endif(${HAVE_LIBAIO})

target_link_libraries(os leveldb snappy)
