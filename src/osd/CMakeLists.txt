
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

add_library(osd ${osd_srcs})

target_link_libraries(osd dl leveldb)
