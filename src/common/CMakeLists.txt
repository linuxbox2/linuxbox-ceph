
set (crush_files 
	../crush/builder.c 
	../crush/mapper.c 
	../crush/crush.c 
	../crush/hash.c 
	../crush/CrushWrapper.cc 
	../crush/CrushCompiler.cc 
	../crush/CrushTester.cc
)

add_custom_target(check_version ALL ${CMAKE_SOURCE_DIR}/src/check_version ${CMAKE_SOURCE_DIR}/src/.git_version 
COMMENT "ceph_ver.h is up to date")

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

if (HAVE_XIO)

set(CMAKE_CXX_FLAGS "${CMAKE_C_FLAGS} -I${Xio_INCLUDE_DIR}")

set(xio_messenger_cc
  ../msg/XioConnection.cc
  ../msg/XioMessage.cc
  ../msg/XioMessenger.cc
  )

endif(HAVE_XIO)


# this list ommits the ceph_ver.c file
set (libcommon_files
	${CMAKE_SOURCE_DIR}/src/ceph_ver.h
	../ceph_ver.c 
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
	../common/DecayCounter.cc 
	../common/LogClient.cc 
	../common/LogEntry.cc 
	../common/PrebufferedStreambuf.cc 
	../common/BackTrace.cc 
	../common/perf_counters.cc 
	../common/Mutex.cc 
	../common/OutputDataSocket.cc 
	../common/admin_socket.cc 
	../common/admin_socket_client.cc 
	../common/cmdparse.cc 
	../common/escape.c 
	../common/Clock.cc 
	../common/Throttle.cc 
	../common/Timer.cc 
	../common/Finisher.cc 
	../common/environment.cc
	../common/sctp_crc32.c
	../common/assert.cc 
        ../common/run_cmd.cc 
	../common/WorkQueue.cc 
	../common/ConfUtils.cc 
	../common/MemoryModel.cc 
	../common/armor.c 
	../common/fd.cc 
	../common/xattr.c 
	../common/safe_io.c 
	../common/snap_types.cc 
	../common/str_list.cc 
	../common/errno.cc 
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
	../common/RefCountedObj.cc 
	../msg/Messenger.cc 
	../msg/Pipe.cc 
	../msg/SimpleMessenger.cc
	${xio_messenger_cc}
	../msg/msg_types.cc 
	../os/hobject.cc 
	../osd/OSDMap.cc 
	../osd/osd_types.cc 
	../mds/MDSMap.cc 
	../mds/inode_backtrace.cc 
	../mds/mdstypes.cc 
	../common/blkdev.cc 
	../common/common_init.cc 
	../common/pipe.c 
	../common/ceph_argparse.cc 
	../common/ceph_context.cc 
	../common/buffer.cc 
	../common/code_environment.cc 
	../common/dout.cc 
	../common/signal.cc 
	../common/simple_spin.cc 
	../common/Thread.cc 
	../common/Formatter.cc 
	../common/HeartbeatMap.cc 
	../include/ceph_fs.cc 
	../include/ceph_hash.cc 
	../include/ceph_strings.cc 
	../include/ceph_frag.cc
	../common/config.cc 
	../common/utf8.c 
	../common/mime.c 
	../common/strtol.cc 
	../common/page.cc 
	../common/lockdep.cc 
	../common/version.cc 
	../common/hex.cc 
	../common/entity_name.cc 
	../common/ceph_crypto.cc 
	../common/ceph_crypto_cms.cc 
	../common/ceph_json.cc 
	../common/ipaddr.cc 
	../common/pick_address.cc
	../include/addr_parsing.c 
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

include_directories("..")

add_library (common ${libcommon_files})

set_source_files_properties(${CMAKE_SOURCE_DIR}/src/ceph_ver.c ${CMAKE_SOURCE_DIR}/src/common/version.cc ${CMAKE_SOURCE_DIR}/src/test/encoding/ceph_dencoder.cc APPEND PROPERTY OBJECT_DEPENDS ${CMAKE_SOURCE_DIR}/src/ceph_ver.h)

add_dependencies(common check_version)

if (${WITH_PROFILER})
  target_link_libraries(common profiler)
endif(${WITH_PROFILER})

target_link_libraries(common ${CRYPTO_LIBS} rt ${Boost_LIBRARIES}) 




