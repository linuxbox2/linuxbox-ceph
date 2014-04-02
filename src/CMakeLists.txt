include(GetGitRevisionDescription)

add_definitions("-DCEPH_LIBDIR=${LIBRARY_OUTPUT_PATH}")

set(AM_COMMON_FLAGS "-DHAVE_CONFIG_H -D__CEPH__ -D_FILE_OFFSET_BITS=64 -D_REENTRANT -D_THREAD_SAFE -D__STDC_FORMAT_MACROS -D_GNU_SOURCE -rdynamic -Wall -Wtype-limits -Wignored-qualifiers -Winit-self -Wpointer-arith -Werror=format-security -fno-strict-aliasing -fsigned-char -fPIC")

set(CMAKE_CXX_FLAGS "${CMAKE_C_FLAGS} -Wno-invalid-offsetof -Wnon-virtual-dtor -Wno-invalid-offsetof -Wstrict-null-sentinel")

add_definitions("${AM_COMMON_FLAGS}")

set(EXTRALIBS uuid rt dl)
if(${WITH_PROFILER})
  list(APPEND EXTRALIBS profiler)
endif(${WITH_PROFILER})

set(CRYPTO_LIBS cryptopp)

if(${HAVE_ATOMIC_OPS})
  list(APPEND EXTRALIBS atomic_ops)
endif(${HAVE_ATOMIC_OPS})

set(bindir ${CMAKE_INSTALL_PREFIX}/bin)
set(sbindir ${CMAKE_INSTALL_PREFIX}/sbin)
set(libdir ${CMAKE_INSTALL_PREFIX}/lib)
set(sysconfdir ${CMAKE_INSTALL_PREFIX}/etc)
set(pkgdatadir ${CMAKE_INSTALL_PREFIX}/share)
set(prefix ${CMAKE_INSTALL_PREFIX})
set(GCOV_PREFIX_STRIP 4)
get_git_head_revision(GIT_REFSPEC CEPH_GIT_VER)
git_describe(CEPH_GIT_NICE_VER --always)

# Python stuff
find_package(PythonInterp 2 QUIET)
if(NOT PYTHONINTERP_FOUND)
  message(FATAL_ERROR "Python 2 interpreter not found.")
endif(NOT PYTHONINTERP_FOUND)

# if CMAKE_INSTALL_PREFIX is an empty string, must replace
# it with "/" to make PYTHON_INSTALL_TEMPLATE an absolute path to be
# consistent with all other installation paths.
if(CMAKE_INSTALL_PREFIX)
  set(PYTHON_INSTALL_TEMPLATE "${CMAKE_INSTALL_PREFIX}")
else(CMAKE_INSTALL_PREFIX)
  set(PYTHON_INSTALL_TEMPLATE "/")
endif(CMAKE_INSTALL_PREFIX)

execute_process(
  COMMAND
  ${PYTHON_EXECUTABLE} -c "from distutils import sysconfig; print sysconfig.get_python_lib(1,0,prefix='${PYTHON_INSTALL_TEMPLATE}')"
  OUTPUT_VARIABLE PYTHON_INSTDIR
  OUTPUT_STRIP_TRAILING_WHITESPACE
  )

configure_file(
${CMAKE_SOURCE_DIR}/src/init-ceph.in.cmake
${CMAKE_BINARY_DIR}/init-ceph
@ONLY
)

#add_custom_target(init-ceph SOURCES init-ceph)

configure_file(
${CMAKE_SOURCE_DIR}/src/mkcephfs.in.cmake
${CMAKE_BINARY_DIR}/mkcephfs
@ONLY
)

configure_file(
${CMAKE_SOURCE_DIR}/src/ceph-coverage.in.cmake
${CMAKE_BINARY_DIR}/ceph-coverage
@ONLY
)

configure_file(
${CMAKE_SOURCE_DIR}/src/ceph-debugpack.in.cmake
${CMAKE_BINARY_DIR}/ceph-debugpack
@ONLY
)

configure_file(
${CMAKE_SOURCE_DIR}/src/ceph.in.cmake
${CMAKE_BINARY_DIR}/ceph
@ONLY
)

configure_file(
${CMAKE_SOURCE_DIR}/src/ceph_ver.h.in.cmake
${CMAKE_BINARY_DIR}/src/include/ceph_ver.h
@ONLY
)

add_custom_target(shell_scripts ALL
COMMAND chmod 755 ${CMAKE_BINARY_DIR}/mkcephfs ${CMAKE_BINARY_DIR}/init-ceph ${CMAKE_BINARY_DIR}/ceph-debugpack ${CMAKE_BINARY_DIR}/ceph-coverage ${CMAKE_BINARY_DIR}/ceph
DEPENDS ${CMAKE_BINARY_DIR}/mkcephfs ${CMAKE_BINARY_DIR}/init-ceph ${CMAKE_BINARY_DIR}/ceph-debugpack ${CMAKE_BINARY_DIR}/ceph-coverage ${CMAKE_BINARY_DIR}/ceph
COMMENT "Shell Scripts have permission")

add_subdirectory(rgw)
if(${WITH_MDS})
  add_subdirectory(mds)
endif(${WITH_MDS})
if(${WITH_CLIENT})
  add_subdirectory(client)
endif(${WITH_CLIENT})

if(${WITH_TCMALLOC})
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-builtin-malloc -fno-builtin-calloc -fno-builtin-realloc -fno-builtin-free")
  set(TCMALLOC_LIBS tcmalloc)
  set(TCMALLOC_srcs perfglue/heap_profiler.cc)
else()
  set(TCMALLOC_srcs perfglue/disabled_heap_profiler.cc)
endif(${WITH_TCMALLOC})

include_directories(".")

##########################################

# tcmalloc heap profiler
set(heap_profiler_files ${TCMALLOC_srcs})
add_library(heap_profiler_objs OBJECT ${heap_profiler_files})

#osdc
set(osdc_files
	osdc/Objecter.cc 
	osdc/Filer.cc 
)
set(osdc_rbd_files osdc/ObjectCacher.cc osdc/Striper.cc)

add_library(osdc_rbd_objs OBJECT ${osdc_rbd_files})

add_library(osdc STATIC ${osdc_files} $<TARGET_OBJECTS:osdc_rbd_objs>)

#os
set(libos_srcs
	os/FileJournal.cc 
	os/FileStore.cc 
	os/chain_xattr.cc 
	os/ObjectStore.cc 
	os/JournalingObjectStore.cc 
	os/LFNIndex.cc 
	os/HashIndex.cc 
	os/IndexManager.cc 
	os/FlatIndex.cc 
        os/LevelDBStore.cc 	
        os/DBObjectMap.cc 
	os/WBThrottle.cc
        os/GenericFileStoreBackend.cc
        os/BtrfsFileStoreBackend.cc
        os/ZFSFileStoreBackend.cc

)
set(os_mon_files os/LevelDBStore.cc)

add_library(os_mon_objs OBJECT ${os_mon_files})
add_library(os STATIC ${libos_srcs} $<TARGET_OBJECTS:os_mon_objs>)
if(${HAVE_LIBAIO})
  target_link_libraries(os aio)
endif(${HAVE_LIBAIO})
target_link_libraries(os leveldb snappy)

#osd
set(osdc_osd_srcs osdc/Objecter.cc osdc/Striper.cc)

set(osd_srcs
	osd/Ager.cc 
	osd/OSD.cc 
	osd/OSDCap.cc 
	osd/Watch.cc 
	osd/ClassHandler.cc 
	osd/OpRequest.cc 
	osd/SnapMapper.cc
        osd/PG.cc 
        osd/PGLog.cc 
        osd/ReplicatedPG.cc 
        osd/ReplicatedBackend.cc
        ${osdc_osd_srcs}
)
set(osd_mon_files mon/Monitor.cc)

add_library(osd_mon_objs OBJECT ${osd_mon_files})
add_library(osd STATIC ${osd_srcs} $<TARGET_OBJECTS:osd_mon_objs> $<TARGET_OBJECTS:cls_references_objs>
)
target_link_libraries(osd dl leveldb)

#mon
set(lib_mon_srcs
	auth/cephx/CephxKeyServer.cc 
	auth/cephx/CephxServiceHandler.cc 
	auth/AuthServiceHandler.cc
        ${osd_mon_files}
        ${mon_common_files} 
	mon/Paxos.cc 
	mon/PaxosService.cc 
	mon/OSDMonitor.cc 
	mon/MDSMonitor.cc 
	mon/MonmapMonitor.cc 
	mon/LogMonitor.cc
        mon/PGMonitor.cc 
        mon/PGMap.cc 
	mon/AuthMonitor.cc 
	mon/Elector.cc
        mon/MonitorStore.cc  
	mon/HealthMonitor.cc
        ${os_mon_files} 
	mon/DataHealthService.cc 
	mon/ConfigKeyService.cc 
)
set(common_util_src common/util.cc)
set(monitorstore_src mon/MonitorStore.cc)

add_library(common_util_obj OBJECT ${common_util_src})
add_library(monitorstore_obj OBJECT ${monitorstore_src})
add_library(mon STATIC ${lib_mon_srcs}
	$<TARGET_OBJECTS:mon_common_objs>
	$<TARGET_OBJECTS:os_mon_objs>
	$<TARGET_OBJECTS:osd_mon_objs>
	$<TARGET_OBJECTS:common_util_obj>
	$<TARGET_OBJECTS:monitorstore_obj>
	)

#global
set(libglobal_srcs
	global/global_init.cc 
	global/pidfile.cc 
	global/signal_handler.cc
)
set(global_common_files
	global/global_context.cc 
)
add_library(global_common_objs OBJECT ${global_common_files})
add_library(global STATIC ${libglobal_srcs} $<TARGET_OBJECTS:global_common_objs>)
target_link_libraries(global common ${CMAKE_THREAD_LIBS_INIT} ${CRYPTO_LIBS} m ${EXTRALIBS})
if(${ENABLE_SHARED})
  set_target_properties(global PROPERTIES OUTPUT_NAME ceph-global VERSION "1.0.0" SOVERSION "1")
endif(${ENABLE_SHARED})

#common
set (crush_files
	crush/builder.c
	crush/mapper.c
	crush/crush.c
	crush/hash.c
	crush/CrushWrapper.cc
	crush/CrushCompiler.cc
	crush/CrushTester.cc
)

set (mds_files)
if (${WITH_MDS})
	list(APPEND mds_files
		mds/MDSMap.cc
		mds/inode_backtrace.cc
		mds/mdstypes.cc
	)
endif (${WITH_MDS})


# this list ommits the ceph_ver.c file
set (libcommon_files
	${CMAKE_BINARY_DIR}/src/include/ceph_ver.h
	ceph_ver.c
	auth/AuthAuthorizeHandler.cc
	auth/AuthClientHandler.cc
	auth/AuthMethodList.cc
	auth/cephx/CephxAuthorizeHandler.cc
	auth/cephx/CephxClientHandler.cc
	auth/cephx/CephxProtocol.cc
	auth/none/AuthNoneAuthorizeHandler.cc
	auth/unknown/AuthUnknownAuthorizeHandler.cc
	auth/Crypto.cc
	auth/KeyRing.cc
	auth/RotatingKeyRing.cc
        arch/intel.c
        arch/neon.c
        arch/probe.cc
	common/DecayCounter.cc
	common/LogClient.cc
	common/LogEntry.cc
	common/PrebufferedStreambuf.cc
	common/BackTrace.cc
	common/perf_counters.cc
	common/Mutex.cc
	common/OutputDataSocket.cc
	common/admin_socket.cc
	common/admin_socket_client.cc
	common/cmdparse.cc
	common/escape.c
	common/Clock.cc
	common/Throttle.cc
	common/Timer.cc
	common/Finisher.cc
	common/environment.cc
	common/sctp_crc32.c
        common/crc32c.cc
        common/crc32c_intel_baseline.c
        common/crc32c_intel_fast.c
	common/assert.cc
        common/run_cmd.cc
	common/WorkQueue.cc
	common/ConfUtils.cc
	common/MemoryModel.cc
	common/fd.cc
	common/xattr.c
	common/snap_types.cc
	common/str_list.cc
	common/errno.cc
        common/TrackedOp.cc
        common/SloppyCRCMap.cc
	json_spirit/json_spirit_reader.cpp
	json_spirit/json_spirit_writer.cpp
	json_spirit/json_spirit_value.cpp
	log/Log.cc
	log/SubsystemMap.cc
	mon/MonCap.cc
	mon/MonClient.cc
	mon/MonMap.cc
	msg/Accepter.cc
	msg/DispatchQueue.cc
	msg/Message.cc
	common/RefCountedObj.cc
	msg/Messenger.cc
	msg/Pipe.cc
	msg/SimpleMessenger.cc
	msg/msg_types.cc
	common/hobject.cc
	osd/OSDMap.cc
	osd/osd_types.cc
	${mds_files}
	common/blkdev.cc
	common/common_init.cc
	common/pipe.c
	common/ceph_argparse.cc
	common/ceph_context.cc
	common/buffer.cc
	common/code_environment.cc
	common/dout.cc
	common/signal.cc
	common/simple_spin.cc
	common/Thread.cc
	common/Formatter.cc
	common/HeartbeatMap.cc
	common/ceph_fs.cc
	common/ceph_hash.cc
	common/ceph_strings.cc
	common/ceph_frag.cc
	common/config.cc
	common/utf8.c
	common/mime.c
	common/strtol.cc
	common/page.cc
	common/lockdep.cc
	common/version.cc
	common/hex.cc
	common/entity_name.cc
	common/ceph_crypto.cc
	common/ceph_crypto_cms.cc
	common/ceph_json.cc
	common/ipaddr.cc
	common/pick_address.cc
	${crush_files}
)
set(mon_common_files
	auth/AuthSessionHandler.cc
	auth/cephx/CephxSessionHandler.cc
)
add_library(mon_common_objs OBJECT ${mon_common_files})
set(common_mountcephfs_files
	common/armor.c
	common/safe_io.c
	common/addr_parsing.c
)
add_library(common_mountcephfs_objs OBJECT ${common_mountcephfs_files})

if (${WITH_PROFILER})
  list(APPEND libcommon_files perfglue/cpu_profiler.cc)
else ()
  list(APPEND libcommon_files perfglue/disabled_stubs.cc)
endif (${WITH_PROFILER})

if(${ENABLE_SHARED})
  list(APPEND libcommon_files $<TARGET_OBJECTS:global_common_objs>)
endif(${ENABLE_SHARED})

add_library (common STATIC ${libcommon_files}
	$<TARGET_OBJECTS:mon_common_objs>
	$<TARGET_OBJECTS:common_mountcephfs_objs>
)

set_source_files_properties(${CMAKE_SOURCE_DIR}/src/ceph_ver.c ${CMAKE_SOURCE_DIR}/src/common/version.cc ${CMAKE_SOURCE_DIR}/src/test/encoding/ceph_dencoder.cc APPEND PROPERTY OBJECT_DEPENDS ${CMAKE_BINARY_DIR}/src/include/ceph_ver.h)

if (${WITH_PROFILER})
  target_link_libraries(common profiler)
endif(${WITH_PROFILER})

target_link_libraries(common ${CRYPTO_LIBS} rt ${Boost_LIBRARIES})

#cls references
set(cls_references_files
        objclass/class_api.cc
)
add_library(cls_references_objs OBJECT ${cls_references_files})

#cls/rbd
set(cls_rbd_files
	cls/rbd/cls_rbd.cc
)
add_library(cls_rbd_objs OBJECT ${cls_rbd_files})
add_library(cls_rbd ${CEPH_SHARED} $<TARGET_OBJECTS:cls_rbd_objs>)
if(${ENABLE_SHARED})
  set_target_properties(cls_rbd PROPERTIES OUTPUT_NAME cls_rbd VERSION "1.0.0" SOVERSION "1")
endif(${ENABLE_SHARED})

#cls/refcount
set(cls_refcount_files
	cls/refcount/cls_refcount.cc
        cls/refcount/cls_refcount_ops.cc
        cls/refcount/cls_refcount_client.cc
)
add_library(cls_refcount_objs OBJECT ${cls_refcount_files})
add_library(cls_refcount ${CEPH_SHARED} $<TARGET_OBJECTS:cls_refcount_objs>)
if(${ENABLE_SHARED})
  set_target_properties(cls_refcount PROPERTIES OUTPUT_NAME cls_refcount VERSION "1.0.0" SOVERSION "1")
endif(${ENABLE_SHARED})

#cls/lock
set(cls_lock_files
	cls/lock/cls_lock_client.cc  
	cls/lock/cls_lock_types.cc 
	cls/lock/cls_lock_ops.cc
        cls/lock/cls_lock.cc
)
add_library(cls_lock_objs OBJECT ${cls_lock_files})
add_library(cls_lock ${CEPH_SHARED} $<TARGET_OBJECTS:cls_lock_objs>)
if(${ENABLE_SHARED})
  set_target_properties(cls_lock PROPERTIES OUTPUT_NAME cls_lock VERSION "1.0.0" SOVERSION "1")
endif(${ENABLE_SHARED})

#cls/version
set(cls_version_files
        cls/version/cls_version.cc
        cls/version/cls_version_types.cc 
        cls/version/cls_version_client.cc
)
add_library(cls_version_objs OBJECT ${cls_version_files})
add_library(cls_version ${CEPH_SHARED} $<TARGET_OBJECTS:cls_version_objs>)
if(${ENABLE_SHARED})
  set_target_properties(cls_version PROPERTIES OUTPUT_NAME cls_version VERSION "1.0.0" SOVERSION "1")
endif(${ENABLE_SHARED})

#cls/log
set(cls_log_files
        cls/log/cls_log.cc
        cls/log/cls_log_client.cc

)
add_library(cls_log_objs OBJECT ${cls_log_files})
add_library(cls_log ${CEPH_SHARED} $<TARGET_OBJECTS:cls_log_objs>)
if(${ENABLE_SHARED})
  set_target_properties(cls_log PROPERTIES OUTPUT_NAME cls_log VERSION "1.0.0" SOVERSION "1")
endif(${ENABLE_SHARED})

#cls/replica_log
set(cls_replica_log_files
        cls/replica_log/cls_replica_log.cc
        cls/replica_log/cls_replica_log_client.cc 
        cls/replica_log/cls_replica_log_types.cc
        cls/replica_log/cls_replica_log_ops.cc
)
add_library(cls_replica_log_objs OBJECT ${cls_replica_log_files})
add_library(cls_replica_log ${CEPH_SHARED} $<TARGET_OBJECTS:cls_replica_log_objs>)
if(${ENABLE_SHARED})
  set_target_properties(cls_replica_log PROPERTIES OUTPUT_NAME cls_replica_log VERSION "1.0.0" SOVERSION "1")
endif(${ENABLE_SHARED})

#cls/statelog
set(cls_statelog_files
        cls/statelog/cls_statelog.cc 
        cls/statelog/cls_statelog_client.cc
)
add_library(cls_statelog_objs OBJECT ${cls_statelog_files})
add_library(cls_statelog ${CEPH_SHARED} $<TARGET_OBJECTS:cls_statelog_objs>)
if(${ENABLE_SHARED})
  set_target_properties(cls_statelog PROPERTIES OUTPUT_NAME cls_statelog VERSION "1.0.0" SOVERSION "1")
endif(${ENABLE_SHARED})

#cls/rgw
set(cls_rgw_files 
        cls/rgw/cls_rgw_client.cc 
        cls/rgw/cls_rgw_types.cc 
        cls/rgw/cls_rgw_ops.cc
        cls/rgw/cls_rgw.cc
)

add_library(cls_rgw_objs OBJECT ${cls_rgw_files})
add_library(cls_rgw ${CEPH_SHARED} $<TARGET_OBJECTS:cls_rgw_objs>)
if(${ENABLE_SHARED})
  set_target_properties(cls_rgw PROPERTIES OUTPUT_NAME cls_rgw VERSION "1.0.0" SOVERSION "1")
endif(${ENABLE_SHARED})

#monitor
set(ceph_mon_srcs
	ceph_mon.cc
	common/TextTable.cc
)

add_executable(ceph-mon ${ceph_mon_srcs} $<TARGET_OBJECTS:heap_profiler_objs>)

target_link_libraries(ceph-mon mon boost_thread common os global ${CMAKE_DL_LIBS} ${TCMALLOC_LIBS})

#ceph-osd
set(ceph_osd_srcs
	ceph_osd.cc
	objclass/class_api.cc
)

add_executable(ceph-osd ${ceph_osd_srcs} $<TARGET_OBJECTS:heap_profiler_objs>)

target_link_libraries(ceph-osd osd os global ${TCMALLOC_LIBS})

if(${WITH_MDS})
#mds
set(ceph_mds_srcs
	ceph_mds.cc
)
add_executable(ceph-mds ${ceph_mds_srcs} $<TARGET_OBJECTS:heap_profiler_objs>)

target_link_libraries(ceph-mds mds osdc ${CMAKE_DL_LIBS} global ${TCMALLOC_LIBS} boost_thread)
endif(${WITH_MDS})

# admin tools

#cephfs
set(cephfs_srcs
	cephfs.cc
)

add_executable(cephfs ${cephfs_srcs})

target_link_libraries(cephfs common ${EXTRALIBS})

# synthetic client
set(ceph_syn_srcs
	ceph_syn.cc
	client/SyntheticClient.cc
)

if(${WITH_CLIENT})
  add_executable(ceph-syn ${ceph_syn_srcs})
  target_link_libraries(ceph-syn client global)
endif(${WITH_CLIENT})

set(librados_srcs
	librados/librados.cc 
	librados/RadosClient.cc
	librados/IoCtxImpl.cc
)

add_library(librados ${CEPH_SHARED} ${librados_srcs} $<TARGET_OBJECTS:cls_lock_objs> $<TARGET_OBJECTS:cls_references_objs>)
add_dependencies(librados osdc)
target_link_libraries(librados osdc common osd os ${CRYPTO_LIBS} ${EXTRALIBS})
if(${ENABLE_SHARED})
  set_target_properties(librados PROPERTIES OUTPUT_NAME rados VERSION 2.0.0 SOVERSION 2)
endif(${ENABLE_SHARED})
install(FILES
	include/rados/librados.h
	include/rados/rados_types.h
	include/rados/rados_types.hpp
	include/rados/librados.hpp
	include/buffer.h
	include/page.h
	include/crc32c.h
	DESTINATION include/rados
)

#rados
set(rados_srcs
	tools/rados/rados.cc
	tools/rados/rados_import.cc
	tools/rados/rados_export.cc
	tools/rados/rados_sync.cc
	common/obj_bencher.cc
)
add_executable(rados ${rados_srcs} $<TARGET_OBJECTS:heap_profiler_objs>)
target_link_libraries(rados librados global ${CMAKE_DL_LIBS} ${TCMALLOC_LIBS})

set (rbd_bin)
if(${WITH_RBD})
#librbd
set(librbd_srcs
        librbd/librbd.cc
        librbd/AioCompletion.cc
        librbd/AioRequest.cc
        cls/rbd/cls_rbd_client.cc
        librbd/ImageCtx.cc
        librbd/internal.cc
        librbd/LibrbdWriteback.cc
        librbd/WatchCtx.cc
        librados/snap_set_diff.cc
)
add_library(librbd ${CEPH_SHARED} ${librbd_srcs}
	$<TARGET_OBJECTS:cls_lock_objs> $<TARGET_OBJECTS:osdc_rbd_objs>
	$<TARGET_OBJECTS:common_util_obj>
	)
target_link_libraries(librbd ${CMAKE_DL_LIBS})
if(${ENABLE_SHARED})
	set_target_properties(librbd PROPERTIES VERSION "1.0.0" SOVERSION "1" OUTPUT_NAME rbd)
endif(${ENABLE_SHARED})

#rbd
set(rbd_srcs
        rbd.cc
	common/TextTable.cc
)
set(rbd_mountcephfs_files common/secret.c)
add_library(rbd_mountcephfs_objs OBJECT ${rbd_mountcephfs_files})
add_executable(rbd ${rbd_srcs}
	$<TARGET_OBJECTS:common_util_obj>
	$<TARGET_OBJECTS:rbd_mountcephfs_objs>
        $<TARGET_OBJECTS:heap_profiler_objs>
)
set_target_properties(rbd PROPERTIES OUTPUT_NAME rbd)
target_link_libraries(rbd global librbd librados common keyutils ${CMAKE_DL_LIBS} ${TCMALLOC_LIBS})
list(APPEND rbd_bin rbd)
install(FILES
	include/rbd/features.h
	include/rbd/librbd.h
	include/rbd/librbd.hpp
	DESTINATION include/rbd
)
endif(${WITH_RBD})

#radosgw worry about option logic at a different time
if(${WITH_RADOSGW})
set(radosgw_srcs
	rgw/rgw_resolve.cc
        rgw/rgw_rest.cc
        rgw/rgw_rest_swift.cc
        rgw/rgw_rest_s3.cc
        rgw/rgw_rest_usage.cc
        rgw/rgw_rest_user.cc
        rgw/rgw_rest_bucket.cc
        rgw/rgw_http_client.cc
        rgw/rgw_swift.cc
	rgw/rgw_swift_auth.cc
	rgw/rgw_main.cc
        rgw/rgw_keystone.cc
        rgw/rgw_rest_client.cc
        rgw/rgw_metadata.cc
        rgw/rgw_rest_config.cc     
        rgw/rgw_rest_conn.cc 
        rgw/rgw_rest_log.cc  
        rgw/rgw_rest_metadata.cc
        rgw/rgw_rest_opstate.cc 
        rgw/rgw_rest_replica_log.cc
        ${kvs_srcs} 
)

add_executable(radosgw ${radosgw_srcs} $<TARGET_OBJECTS:heap_profiler_objs>)

target_link_libraries(radosgw librgw_a librados cls_rgw cls_lock cls_refcount cls_log cls_statelog cls_version cls_replica_log cls_kvs curl expat global fcgi resolv ${TCMALLOC_LIBS})
endif(${WITH_RADOSGW})

#librados_config_srcs
set(librados_config_srcs
	librados-config.cc
)

add_executable(librados-config ${librados_config_srcs} $<TARGET_OBJECTS:heap_profiler_objs>)
target_link_libraries(librados-config librados global ${CMAKE_DL_LIBS} ${TCMALLOC_LIBS})


#ceph-conf
set(ceph_conf_srcs
	tools/ceph_conf.cc
)

add_executable(ceph-conf ${ceph_conf_srcs})
target_link_libraries(ceph-conf global)

#monmaptool
set(monmaptool_srcs
	tools/monmaptool.cc
)

add_executable(monmaptool ${monmaptool_srcs})
target_link_libraries(monmaptool global)

#crushtool
set(crushtool_srcs
	tools/crushtool.cc
)

if (${WITH_CLIENT})
  add_executable(crushtool ${crushtool_srcs})
  target_link_libraries(crushtool global)
endif (${WITH_CLIENT})

#osdmaptool
set(osdomaptool_srcs
	tools/osdmaptool.cc
)

add_executable(osdmaptool  ${osdomaptool_srcs})
target_link_libraries(osdmaptool global ${CMAKE_DL_LIBS})

#ceph-authtool
set(ceph_authtool_srcs
	tools/ceph_authtool.cc
)

add_executable(ceph-authtool ${ceph_authtool_srcs})
target_link_libraries(ceph-authtool global)

#ceph-fuse
if(WITH_FUSE)
set(ceph_fuse_srcs
ceph_fuse.cc
client/fuse_ll.cc
)

add_executable(ceph-fuse ${ceph_fuse_srcs})
target_link_libraries(ceph-fuse fuse client global)
set(ceph_fuse_bin "ceph-fuse")
endif(WITH_FUSE)

#ceph_mon_store_converter
set(ceph_mon_store_converter_srcs
	tools/mon_store_converter.cc
)

add_executable(ceph_mon_store_converter ${ceph_mon_store_converter_srcs}
	$<TARGET_OBJECTS:monitorstore_obj>)
target_link_libraries(ceph_mon_store_converter os global)

#kvs
if(${WITH_KVS})
set(kvs_srcs
	key_value_store/cls_kvs.cc
)

add_library(cls_kvs ${CEPH_SHARED} ${kvs_srcs})
if(${ENABLE_SHARED})
  set_target_properties(cls_kvs PROPERTIES OUTPUT_NAME cls_kvs VERSION "1.0.0" SOVERSION "1")
endif(${ENABLE_SHARED})

endif(${WITH_KVS})

#admin tools
# XXX needs mon/PGMap.cc which is no fun
#set(ceph_srcs tools/ceph.cc tools/common.cc)
#add_executable(ceph ${mount_ceph_srcs}
#	$<TARGET_OBJECTS:common_cephtool_objs>
#)

#mount.ceph
set(mount_ceph_srcs mount/mount.ceph.c)
add_executable(mount.ceph ${mount_ceph_srcs}
	$<TARGET_OBJECTS:common_mountcephfs_objs>
	$<TARGET_OBJECTS:rbd_mountcephfs_objs>
)
target_link_libraries(mount.ceph keyutils)

set (ceph_mds_bin)
if (${WITH_MDS})
       list(APPEND ceph_mds_bin ceph-mds)
endif (${WITH_MDS})
set (ceph_client_bin)
if (${WITH_CLIENT})
       list(APPEND ceph_client_bin ceph-syn crushtool)
endif (${WITH_CLIENT})


# add the install targets
install(TARGETS
  ceph-mon
  ceph-osd
  ${ceph_mds_bin}
  cephfs
  ${ceph_fuse_bin}
  ${ceph_client_bin}
  rados
  ${rbd_bin}
  librados-config
  ceph-conf
  monmaptool
  osdmaptool
  ceph-authtool
  ceph_mon_store_converter
  DESTINATION bin)
install(TARGETS librados librbd DESTINATION lib)
install(TARGETS cls_refcount cls_lock cls_rbd DESTINATION lib/rados-classes)
if(${WITH_KVS})
  install(TARGETS cls_kvs DESTINATION lib/rados-classes)
endif(${WITH_KVS})
if(${WITH_RADOSGW})
  install(TARGETS radosgw DESTINATION bin)
endif(${WITH_RADOSGW})

install(PROGRAMS
  ${CMAKE_BINARY_DIR}/init-ceph
  ${CMAKE_SOURCE_DIR}/doc/start/ceph.conf
  ${CMAKE_SOURCE_DIR}/src/vstart.sh
  ${CMAKE_SOURCE_DIR}/src/ceph-clsinfo
  ${CMAKE_BINARY_DIR}/ceph-debugpack
  ${CMAKE_BINARY_DIR}/ceph
  ${CMAKE_SOURCE_DIR}/src/ceph-rbdnamer
  ${CMAKE_BINARY_DIR}/ceph-coverage
  ${CMAKE_SOURCE_DIR}/src/ceph-run
  DESTINATION bin)
install(PROGRAMS
  ${CMAKE_SOURCE_DIR}/src/ceph_common.sh
  DESTINATION lib/ceph)
install(PROGRAMS
  ${CMAKE_BINARY_DIR}/mkcephfs
  ${CMAKE_SOURCE_DIR}/src/ceph-disk-prepare
  ${CMAKE_SOURCE_DIR}/src/ceph-disk-udev
  ${CMAKE_SOURCE_DIR}/src/ceph-disk-activate
  ${CMAKE_SOURCE_DIR}/src/ceph-disk
  ${CMAKE_SOURCE_DIR}/src/ceph-create-keys
  DESTINATION sbin)
install(TARGETS
  mount.ceph
  DESTINATION sbin)

install(DIRECTORY ${CMAKE_SOURCE_DIR}/src/pybind/
  DESTINATION ${PYTHON_INSTDIR})
