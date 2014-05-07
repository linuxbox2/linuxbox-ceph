
set(lib_mon_srcs
	../auth/cephx/CephxKeyServer.cc 
	../auth/cephx/CephxServiceHandler.cc 
	../auth/cephx/CephxSessionHandler.cc 
	../auth/AuthServiceHandler.cc 
	../auth/AuthSessionHandler.cc 
	Monitor.cc 
	Paxos.cc 
	PaxosService.cc 
	OSDMonitor.cc 
	MDSMonitor.cc 
	MonmapMonitor.cc 
	PGMonitor.cc 
	PGMap.cc 
	LogMonitor.cc 
	AuthMonitor.cc 
	Elector.cc 
	MonitorStore.cc 
	../os/LevelDBStore.cc 
	HealthMonitor.cc 
	DataHealthService.cc 
	ConfigKeyService.cc 
	../common/util.cc
)


add_library(mon ${lib_mon_srcs})
