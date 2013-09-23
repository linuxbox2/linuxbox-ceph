set(mds_srcs
	../mds/Anchor.cc 
	../mds/Capability.cc 
	../mds/Dumper.cc 
	../mds/Resetter.cc 
	../mds/MDS.cc
	../mds/flock.cc
	../mds/locks.c 
	../mds/journal.cc 
	../mds/Server.cc 
	../mds/Mutation.cc 
	../mds/MDCache.cc 
	../mds/Locker.cc 
	../mds/Migrator.cc 
	../mds/MDBalancer.cc 
	../mds/CDentry.cc 
	../mds/CDir.cc 
	../mds/CInode.cc 
	../mds/LogEvent.cc 
	../mds/MDSTable.cc
	../mds/InoTable.cc 
	../mds/MDSTableClient.cc 
	../mds/MDSTableServer.cc 
	../mds/AnchorServer.cc 
	../mds/AnchorClient.cc
	../mds/SnapRealm.cc 
	../mds/SnapServer.cc 
	../mds/snap.cc 
	../mds/SessionMap.cc 
	../mds/MDLog.cc
	../osdc/Journaler.cc
)

include_directories("..")

add_library(libmds ${mds_srcs})

message( STATUS "libmds is here" )
