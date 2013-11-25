set(osd_srcs
	Ager.cc 
	ClassHandler.cc 
	OpRequest.cc 
	OSD.cc 
	OSDCap.cc 
	PG.cc 
	PGLog.cc 
	ReplicatedBackend.cc 
	ReplicatedPG.cc 
	SnapMapper.cc
	Watch.cc 
)

set(osdc_srcs
	${CMAKE_SOURCE_DIR}/src/osdc/Objecter.cc
	${CMAKE_SOURCE_DIR}/src/osdc/Striper.cc
)

add_library(osd ${osd_srcs} ${osdc_srcs})

target_link_libraries(osd dl leveldb)
