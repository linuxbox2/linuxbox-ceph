message( STATUS "libos is here" )

set(libos_srcs
	../os/FileJournal.cc 
	../os/FileStore.cc 
	../os/chain_xattr.cc 
	../os/ObjectStore.cc 
	../os/JournalingObjectStore.cc 
	../os/LFNIndex.cc 
	../os/HashIndex.cc 
	../os/IndexManager.cc 
	../os/FlatIndex.cc 
	../os/DBObjectMap.cc 
	../os/LevelDBStore.cc 
	../os/WBThrottle.cc
)

include_directories("..")

add_library(libos ${libos_srcs})

