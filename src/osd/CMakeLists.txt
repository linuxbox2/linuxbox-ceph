set(osd_srcs
	../osd/PG.cc 
	../osd/PGLog.cc 
	../osd/ReplicatedPG.cc 
	../osd/Ager.cc 
	../osd/OSD.cc 
	../osd/OSDCap.cc 
	../osd/Watch.cc 
	../osd/ClassHandler.cc 
	../osd/OpRequest.cc 
	../osd/SnapMapper.cc
)

include_directories("..")

add_library(libosd ${osd_srcs})

target_link_libraries(libosd dl leveldb)

message( STATUS "osd is here" )
