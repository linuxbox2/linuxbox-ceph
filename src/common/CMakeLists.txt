
set(arch_files
	../arch/intel.c
	../arch/neon.c
	../arch/probe.cc
)

set(auth_files
	../auth/AuthAuthorizeHandler.cc
	../auth/AuthClientHandler.cc
	../auth/AuthSessionHandler.cc
	../auth/AuthMethodList.cc
	../auth/cephx/CephxAuthorizeHandler.cc
	../auth/cephx/CephxClientHandler.cc
	../auth/cephx/CephxProtocol.cc
	../auth/cephx/CephxSessionHandler.cc
	../auth/none/AuthNoneAuthorizeHandler.cc
	../auth/unknown/AuthUnknownAuthorizeHandler.cc
	../auth/Crypto.cc
	../auth/KeyRing.cc
	../auth/RotatingKeyRing.cc
)

set (crush_files 
	../crush/builder.c 
	../crush/mapper.c 
	../crush/crush.c 
	../crush/hash.c 
	../crush/CrushWrapper.cc 
	../crush/CrushCompiler.cc 
	../crush/CrushTester.cc
)

add_custom_target(check_version ALL ${CMAKE_SOURCE_DIR}/src/check_version ${CMAKE_SOURCE_DIR}/src/.git_version) 

add_custom_command(
PRE_BUILD
OUTPUT ${CMAKE_SOURCE_DIR}/src/.git_version
COMMAND ${CMAKE_SOURCE_DIR}/src/check_version ${CMAKE_SOURCE_DIR}/src/.git_version
DEPENDS ${CMAKE_SOURCE_DIR}/src/check_version
)

add_custom_command(
PRE_BUILD
OUTPUT ${CMAKE_SOURCE_DIR}/src/ceph_ver.h
COMMAND ${CMAKE_SOURCE_DIR}/src/make_version ${CMAKE_SOURCE_DIR}/src/.git_version ${CMAKE_SOURCE_DIR}/src/ceph_ver.h
DEPENDS ${CMAKE_SOURCE_DIR}/src/make_version ${CMAKE_SOURCE_DIR}/src/.git_version
)

add_custom_target(ceph_ver 
DEPENDS check_version ${CMAKE_SOURCE_DIR}/src/ceph_ver.h
)

if (HAVE_XIO)

set(xio_messenger_cc
  ../msg/XioConnection.cc
  ../msg/XioMsg.cc
  ../msg/XioMessenger.cc
  )

endif(HAVE_XIO)

# this list ommits the ceph_ver.c file
set (libcommon_files
	${CMAKE_SOURCE_DIR}/src/ceph_ver.h
	../ceph_ver.c 
	DecayCounter.cc 
	LogClient.cc 
	LogEntry.cc 
	PrebufferedStreambuf.cc 
	BackTrace.cc 
	perf_counters.cc 
	Mutex.cc 
	OutputDataSocket.cc 
	admin_socket.cc 
	admin_socket_client.cc 
	cmdparse.cc 
	escape.c 
	Clock.cc 
	Throttle.cc 
	Timer.cc 
	Finisher.cc 
	environment.cc
	crc32c.cc
	sctp_crc32.c
	crc32c_intel_baseline.c
	crc32c_intel_fast.c
	assert.cc 
        run_cmd.cc 
	WorkQueue.cc 
	ConfUtils.cc 
	MemoryModel.cc 
	armor.c 
	fd.cc 
	xattr.c 
	safe_io.c 
	snap_types.cc 
	str_list.cc 
	errno.cc 
	../json_spirit/json_spirit_reader.cpp 
	../json_spirit/json_spirit_writer.cpp 
	../json_spirit/json_spirit_value.cpp 
	../log/Log.cc 
	../log/SubsystemMap.cc 
	../mon/MonCap.cc 
	../mon/MonClient.cc 
	../mon/MonMap.cc
	../msg/Accepter.cc 
	../msg/DispatchQueue.cc 
	../msg/Message.cc 
	RefCountedObj.cc 
	../msg/Messenger.cc 
	../msg/Pipe.cc
	../msg/PipeConnection.cc
	../msg/SimpleMessenger.cc
	${xio_messenger_cc}
	../msg/msg_types.cc 
	hobject.cc 
	../osd/OSDMap.cc 
	../osd/osd_types.cc 
	../mds/MDSMap.cc 
	../mds/inode_backtrace.cc 
	../mds/mdstypes.cc 
	blkdev.cc 
	common_init.cc 
	pipe.c 
	ceph_argparse.cc 
	ceph_context.cc 
	buffer.cc 
	code_environment.cc 
	dout.cc 
	signal.cc 
	simple_spin.cc 
	Thread.cc 
	Formatter.cc 
	HeartbeatMap.cc 
	ceph_fs.cc 
	ceph_hash.cc 
	ceph_strings.cc 
	ceph_frag.cc
	config.cc 
	utf8.c 
	mime.c 
	strtol.cc 
	page.cc 
	lockdep.cc 
	version.cc 
	hex.cc 
	entity_name.cc 
	ceph_crypto.cc 
	ceph_crypto_cms.cc 
	ceph_json.cc 
	ipaddr.cc 
	pick_address.cc
	address_helper.cc
	addr_parsing.c 
	SloppyCRCMap.cc
	TrackedOp.cc
	${arch_files}
	${auth_files}
	${crush_files}
)


if (${WITH_PROFILER})
  set (libcommon_files
	../perfglue/cpu_profiler.cc
	${libcommon_files})

else ()
  set (libcommon_files
	../perfglue/disabled_stubs.cc
	${libcommon_files}) 
endif (${WITH_PROFILER})

add_definitions("-DHAVE_CONFIG_H -D__CEPH__ -D_FILE_OFFSET_BITS=64 -D_REENTRANT -D_THREAD_SAFE -D__STDC_FORMAT_MACROS -D_GNU_SOURCE")

add_library (common ${libcommon_files})

set_source_files_properties(${CMAKE_SOURCE_DIR}/src/ceph_ver.c ${CMAKE_SOURCE_DIR}/sversion.cc ${CMAKE_SOURCE_DIR}/src/test/encoding/ceph_dencoder.cc APPEND PROPERTY OBJECT_DEPENDS ${CMAKE_SOURCE_DIR}/src/ceph_ver.h)

add_dependencies(common check_version)

if (${WITH_PROFILER})
  target_link_libraries(common profiler)
endif(${WITH_PROFILER})

target_link_libraries(common ${CRYPTO_LIBS} rt ${Boost_LIBRARIES}) 




