# test_timers
set(test_timers_srcs TestTimers.cc)
add_executable(test_timers ${test_timers_srcs})
target_link_libraries(test_timers global ${CMAKE_DL_LIBS} ${TCMALLOC_LIBS})

# test_signal_handlers
set(test_signal_handlers_srcs TestSignalHandlers.cc)
add_executable(test_signal_handlers ${test_signal_handlers_srcs})
target_link_libraries(test_signal_handlers global ${CMAKE_DL_LIBS} ${TCMALLOC_LIBS})

# test_msgr
set(test_msgr_srcs testmsgr.cc)
add_executable(test_msgr ${test_msgr_srcs})
target_link_libraries(test_msgr global ${CMAKE_DL_LIBS} ${TCMALLOC_LIBS})

# test_crypto
set(test_crypto_srcs testcrypto.cc)
add_executable(test_crypt ${test_crypto_srcs})
target_link_libraries(test_msgr global ${CMAKE_DL_LIBS} ${TCMALLOC_LIBS})

# test_rados
set(test_rados_srcs osd/TestRados.cc osd/TestOpStat.cc osd/Object.cc osd/RadosModel.cc)
add_executable(test_rados ${test_rados_srcs})
target_link_libraries(test_rados global librados ${CMAKE_DL_LIBS} ${TCMALLOC_LIBS})

# test_mutate
set(test_mutate_srcs test_mutate.cc)
add_executable(test_mutate ${test_mutate_srcs})
target_link_libraries(test_mutate global librados ${CMAKE_DL_LIBS} ${TCMALLOC_LIBS})

# test_rewrite_latency
set(test_rewrite_latency_srcs test_rewrite_latency.cc)
add_executable(test_rewrite_latency ${test_rewrite_latency_srcs})
target_link_libraries(test_rewrite_latency common ${CMAKE_DL_LIBS} ${TCMALLOC_LIBS}
  ${CMAKE_THREAD_LIBS_INIT} ${CRYPTO_LIBS} m ${EXTRALIBS})

# streamtest
set(streamtest_srcs streamtest.cc)
add_executable(streamtest ${streamtest_srcs})
target_link_libraries(streamtest os global ${CMAKE_DL_LIBS} ${TCMALLOC_LIBS})

# test_trans
set(test_trans_srcs test_trans.cc)
add_executable(test_trans ${test_trans_srcs})
target_link_libraries(test_trans os global ${CMAKE_DL_LIBS} ${TCMALLOC_LIBS})

# test_keys
set(test_keys_srcs testkeys.cc)
add_executable(test_keys ${test_keys_srcs})
target_link_libraries(test_keys mon global ${CMAKE_DL_LIBS} ${TCMALLOC_LIBS})

## dencoder
set(dencoder_srcs encoding/ceph_dencoder.cc)
add_executable(dencoder ${dencoder_srcs})

target_link_libraries(dencoder osd mds mon global)
if(${WITH_RADOSGW})
target_link_libraries(dencoder librgw_a librados cls_rgw cls_lock cls_refcount
    cls_log cls_statelog cls_version cls_replica_log cls_kvs curl expat)
endif(${WITH_RADOSGW})
target_link_libraries(dencoder ${CMAKE_DL_LIBS} ${TCMALLOC_LIBS})

# get_command_descriptions
add_executable(get_command_descriptions common/get_command_descriptions.cc)
target_link_libraries(get_command_descriptions mon common global
  ${CMAKE_DL_LIBS} ${TCMALLOC_LIBS})

## Benchmarks

# smalliobench
set(smalliobench_srcs bench/small_io_bench.cc bench/rados_backend.cc
  bench/detailed_stat_collector.cc bench/bencher.cc)
add_executable(smalliobench ${smalliobench_srcs})
target_link_libraries(smalliobench librados boost_program_options global
  ${CMAKE_DL_LIBS} ${TCMALLOC_LIBS})

# smalliobenchfs
set(smalliobenchfs_srcs bench/small_io_bench_fs.cc
  bench/testfilestore_backend.cc bench/detailed_stat_collector.cc
  bench/bencher.cc)
add_executable(smalliobenchfs ${smalliobenchfs_srcs})
target_link_libraries(smalliobenchfs librados boost_program_options os global
  ${CMAKE_DL_LIBS} ${TCMALLOC_LIBS})

# smalliobenchdumb
set(smalliobenchdumb_srcs bench/small_io_bench_dumb.cc
  bench/dumb_backend.cc bench/detailed_stat_collector.cc
  bench/bencher.cc)
add_executable(smalliobenchdumb ${smalliobenchdumb_srcs})
target_link_libraries(smalliobenchdumb librados boost_program_options os global
  ${CMAKE_DL_LIBS} ${TCMALLOC_LIBS})

# smalliobenchrbd
set(smalliobenchrbd_srcs bench/small_io_bench_rbd.cc
  bench/rbd_backend.cc bench/detailed_stat_collector.cc
  bench/bencher.cc)
add_executable(smalliobenchrbd ${smalliobenchrbd_srcs})
target_link_libraries(smalliobenchrbd librbd librados boost_program_options os
  global ${CMAKE_DL_LIBS} ${TCMALLOC_LIBS})

# tpbench
set(tpbench_srcs bench/tp_bench.cc bench/detailed_stat_collector.cc)
add_executable(tpbench ${tpbench_srcs})
target_link_libraries(tpbench librados boost_program_options os global
  ${CMAKE_DL_LIBS} ${TCMALLOC_LIBS})

# omapbench
set(omapbench_srcs omap_bench.cc)
add_executable(omapbench ${omapbench_srcs})
target_link_libraries(omapbench librados global ${CMAKE_DL_LIBS}
  ${TCMALLOC_LIBS})

# kvstorebench
set(kvstorebench_srcs kv_store_bench.cc
  ${CMAKE_SOURCE_DIR}/src/key_value_store/kv_flat_btree_async.cc)
add_executable(kvstorebench ${kvstorebench_srcs})
target_link_libraries(kvstorebench librados global ${CMAKE_DL_LIBS}
  ${TCMALLOC_LIBS})

## System tests

# systest
set(libsystest_srcs system/cross_process_sem.cc
  system/systest_runnable.cc
  system/systest_settings.cc
  system/st_rados_create_pool.cc
  system/st_rados_delete_pool.cc
  system/st_rados_list_objects.cc
  system/st_rados_watch.cc
  system/st_rados_notify.cc)
add_library(systest STATIC ${libsystest_srcs})

# test_rados_list_parallel
add_executable(test_rados_list_parallel system/rados_list_parallel.cc)
target_link_libraries(test_rados_list_parallel librados systest global pthread
  rt ${CMAKE_DL_LIBS} ${TCMALLOC_LIBS})

# test_rados_open_pools_parallel
set(test_rados_open_pools_parallel_srcs system/rados_open_pools_parallel.cc)
add_executable(test_rados_open_pools_parallel
  ${test_rados_open_pools_parallel_srcs})
target_link_libraries(test_rados_open_pools_parallel librados systest global
  pthread rt ${CMAKE_DL_LIBS} ${TCMALLOC_LIBS})

# test_rados_delete_pools_parallel
set(test_rados_delete_pools_parallel_srcs
  system/rados_delete_pools_parallel.cc
  system/st_rados_create_pool.cc
  system/st_rados_delete_pool.cc
  system/st_rados_list_objects.cc

  )
add_executable(test_rados_delete_pools_parallel
  ${test_rados_delete_pools_parallel_srcs})
target_link_libraries(test_rados_delete_pools_parallel librados systest global
  pthread rt ${CMAKE_DL_LIBS} ${TCMALLOC_LIBS})

# test_rados_watch_notify
set(test_rados_watch_notify_srcs
  system/rados_watch_notify.cc
  system/st_rados_create_pool.cc
  system/st_rados_delete_pool.cc
  system/st_rados_delete_objs.cc
  system/st_rados_watch.cc
  system/st_rados_notify.cc
  )
add_executable(test_rados_watch_notify
  ${test_rados_watch_notify_srcs})
target_link_libraries(test_rados_watch_notify librados systest global
  pthread rt ${CMAKE_DL_LIBS} ${TCMALLOC_LIBS})

# bench_log
set(bench_log_srcs
  bench_log.cc
  )
add_executable(bench_log ${bench_log_srcs})
target_link_libraries(bench_log global pthread rt ${CMAKE_DL_LIBS} ${TCMALLOC_LIBS})

## Unit tests

set(UNITTEST_LIBS gtest gtest ${PTHREAD_LIBS})
set(UNITTEST_CXX_FLAGS "${CMAKE_CXX_FLAGS} -I${CMAKE_SOURCE_DIR}/src/gtest/include -I${CMAKE_BINARY_DIR}/src/gtest/include -fno-strict-aliasing")
  

# unittest_encoding
set(unittest_encoding_srcs
  encoding.cc
  )
add_executable(unittest_encoding ${unittest_encoding_srcs})
target_link_libraries(unittest_encoding cephfs librados pthread rt m
  ${CMAKE_DL_LIBS} ${TCMALLOC_LIBS} ${UNITTEST_LIBS})
set_target_properties(unittest_encoding
  PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})

# unittest_addrs
set(unittest_addrs_srcs
  test_addrs.cc
  )
add_executable(unittest_addrs ${unittest_addrs_srcs})
target_link_libraries(unittest_addrs cephfs librados pthread rt m
  ${CMAKE_DL_LIBS} ${TCMALLOC_LIBS} ${UNITTEST_LIBS})
set_target_properties(unittest_addrs
  PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})

# unittest_bloom_filter
set(unittest_bloom_filter_srcs
  common/test_bloom_filter.cc
  )
add_executable(unittest_bloom_filter ${unittest_bloom_filter_srcs})
target_link_libraries(unittest_bloom_filter global
  ${CMAKE_DL_LIBS} ${TCMALLOC_LIBS} ${UNITTEST_LIBS})
set_target_properties(unittest_bloom_filter
  PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})

# unittest_histogram
add_executable(unittest_histogram common/histogram.cc)
target_link_libraries(unittest_histogram global
  ${CMAKE_DL_LIBS} ${TCMALLOC_LIBS} ${UNITTEST_LIBS})
set_target_properties(unittest_histogram
  PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})

# unittest_str_map
set(unittest_str_map_srcs
  common/test_str_map.cc
  )
add_executable(unittest_str_map ${unittest_str_map_srcs})
target_link_libraries(unittest_str_map global
  ${CMAKE_DL_LIBS} ${TCMALLOC_LIBS} ${UNITTEST_LIBS})
set_target_properties(unittest_str_map
  PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})

# unittest_sharedptr_registry
set(unittest_sharedptr_registry_srcs
  common/test_sharedptr_registry.cc
  )
add_executable(unittest_sharedptr_registry ${unittest_sharedptr_registry_srcs})
target_link_libraries(unittest_sharedptr_registry global
  ${CMAKE_DL_LIBS} ${TCMALLOC_LIBS} ${UNITTEST_LIBS})
set_target_properties(unittest_sharedptr_registry
  PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})

# unittest_sloppy_crc_map
set(unittest_sloppy_crc_map_srcs
  common/test_sloppy_crc_map.cc
  )
add_executable(unittest_sloppy_crc_map ${unittest_sloppy_crc_map_srcs})
target_link_libraries(unittest_sloppy_crc_map global
  ${CMAKE_DL_LIBS} ${TCMALLOC_LIBS} ${UNITTEST_LIBS})
set_target_properties(unittest_sloppy_crc_map
  PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})

# unittest_util
set(unittest_util_srcs
  common/test_util.cc
  )
add_executable(unittest_util ${unittest_util_srcs})
target_link_libraries(unittest_util global
  ${CMAKE_DL_LIBS} ${TCMALLOC_LIBS} ${UNITTEST_LIBS})
set_target_properties(unittest_util
  PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})

# unittest_osdmap
set(unittest_osdmap_srcs osd/TestOSDMap.cc)
add_executable(unittest_osdmap ${unittest_osdmap_srcs})
target_link_libraries(unittest_osdmap global ${CMAKE_DL_LIBS} ${TCMALLOC_LIBS} ${UNITTEST_LIBS})
set_target_properties(unittest_osdmap PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})

# unittest_workqueue
set(unittest_workqueue_srcs test_workqueue.cc)
add_executable(unittest_workqueue ${unittest_workqueue_srcs})
target_link_libraries(unittest_workqueue global ${CMAKE_DL_LIBS} ${TCMALLOC_LIBS} ${UNITTEST_LIBS})
set_target_properties(unittest_workqueue PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})

# unittest_striper
set(unittest_striper_srcs test_striper.cc)
add_executable(unittest_striper ${unittest_striper_srcs})
target_link_libraries(unittest_striper global ${CMAKE_DL_LIBS} ${TCMALLOC_LIBS} ${UNITTEST_LIBS})
set_target_properties(unittest_striper PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})

# unittest_prebufferedstreambuf
set(unittest_prebufferedstreambuf_srcs test_prebufferedstreambuf.cc)
add_executable(unittest_prebufferedstreambuf ${unittest_prebufferedstreambuf_srcs})
target_link_libraries(unittest_prebufferedstreambuf global ${CMAKE_DL_LIBS} ${TCMALLOC_LIBS} ${UNITTEST_LIBS})
set_target_properties(unittest_prebufferedstreambuf PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})

# unittest_str_list
set(unittest_str_list_srcs test_str_list.cc)
add_executable(unittest_str_list ${unittest_str_list_srcs})
target_link_libraries(unittest_str_list global ${CMAKE_DL_LIBS} ${TCMALLOC_LIBS} ${UNITTEST_LIBS})
set_target_properties(unittest_str_list PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})

# unittest_log
set(unittest_log_srcs ${CMAKE_SOURCE_DIR}/src/log/test.cc)
add_executable(unittest_log ${unittest_log_srcs})
target_link_libraries(unittest_log global ${CMAKE_DL_LIBS} ${TCMALLOC_LIBS} ${UNITTEST_LIBS})
set_target_properties(unittest_log PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})

# unittest_throttle
set(unittest_throttle_srcs common/Throttle.cc)
add_executable(unittest_throttle ${unittest_throttle_srcs})
target_link_libraries(unittest_throttle global ${CMAKE_DL_LIBS} ${TCMALLOC_LIBS} ${UNITTEST_LIBS})
set_target_properties(unittest_throttle PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})

# unittest_base64
set(unittest_base64_srcs base64.cc)
add_executable(unittest_base64 ${unittest_base64_srcs})
target_link_libraries(unittest_base64 global ${CMAKE_DL_LIBS} ${TCMALLOC_LIBS} ${UNITTEST_LIBS})
set_target_properties(unittest_base64 PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})

# unittest_ceph_argparse
set(unittest_ceph_argparse_srcs ceph_argparse.cc)
add_executable(unittest_ceph_argparse ${unittest_ceph_argparse_srcs})
target_link_libraries(unittest_ceph_argparse global ${CMAKE_DL_LIBS} ${TCMALLOC_LIBS} ${UNITTEST_LIBS})
set_target_properties(unittest_ceph_argparse PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})

# unittest_ceph_compatset
set(unittest_ceph_compatset_srcs ceph_compatset.cc)
add_executable(unittest_ceph_compatset ${unittest_ceph_compatset_srcs})
target_link_libraries(unittest_ceph_compatset global ${CMAKE_DL_LIBS} ${TCMALLOC_LIBS} ${UNITTEST_LIBS})
set_target_properties(unittest_ceph_compatset PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})

# unittest_osd_types
set(unittest_osd_types_srcs osd/types.cc)
add_executable(unittest_osd_types ${unittest_osd_types_srcs})
target_link_libraries(unittest_osd_types global ${CMAKE_DL_LIBS} ${TCMALLOC_LIBS} ${UNITTEST_LIBS})
set_target_properties(unittest_osd_types PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})

# unittest_gather
set(unittest_gather_srcs gather.cc)
add_executable(unittest_gather ${unittest_gather_srcs})
target_link_libraries(unittest_gather global ${CMAKE_DL_LIBS} ${TCMALLOC_LIBS} ${UNITTEST_LIBS})
set_target_properties(unittest_gather PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})

# run_cmd
set(unittest_run_cmd_srcs run_cmd.cc)
add_executable(unittest_run_cmd ${unittest_run_cmd_srcs})
target_link_libraries(unittest_run_cmd global ${CMAKE_DL_LIBS} ${TCMALLOC_LIBS} ${UNITTEST_LIBS})
set_target_properties(unittest_run_cmd PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})

# signals
set(unittest_signals_srcs signals.cc)
add_executable(unittest_signals ${unittest_signals_srcs})
target_link_libraries(unittest_signals global ${CMAKE_DL_LIBS} ${TCMALLOC_LIBS} ${UNITTEST_LIBS})
set_target_properties(unittest_signals PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})

# unittest_simple_spin
set(unittest_simple_spin_srcs simple_spin.cc)
add_executable(unittest_simple_spin ${unittest_simple_spin_srcs})
target_link_libraries(unittest_simple_spin global ${CMAKE_DL_LIBS} ${TCMALLOC_LIBS} ${UNITTEST_LIBS})
set_target_properties(unittest_simple_spin PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})

# unittest_librados
set(unittest_librados_srcs librados/librados.cc)
add_executable(unittest_librados ${unittest_librados_srcs})
target_link_libraries(unittest_librados global ${CMAKE_DL_LIBS} ${TCMALLOC_LIBS} ${UNITTEST_LIBS})
set_target_properties(unittest_librados PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})

# unittest_bufferlist
set(unittest_bufferlist_srcs bufferlist.cc)
add_executable(unittest_bufferlist ${unittest_bufferlist_srcs})
target_link_libraries(unittest_bufferlist global ${CMAKE_DL_LIBS} ${TCMALLOC_LIBS} ${UNITTEST_LIBS})
set_target_properties(unittest_bufferlist PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})

# unittest_crc32
set(unittest_crc32_srcs common/test_crc32c.cc)
add_executable(unittest_crc32 ${unittest_crc32_srcs})
target_link_libraries(unittest_crc32 global ${CMAKE_DL_LIBS} ${TCMALLOC_LIBS} ${UNITTEST_LIBS})
set_target_properties(unittest_crc32 PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})

# unittest_arch
set(unittest_arch_srcs test_arch.cc)
add_executable(unittest_arch ${unittest_arch_srcs})
target_link_libraries(unittest_arch global ${CMAKE_DL_LIBS} ${TCMALLOC_LIBS} ${UNITTEST_LIBS})
set_target_properties(unittest_arch PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})

# unittest_crypto_init
set(unittest_crypto_init_srcs crypto_init.cc)
add_executable(unittest_crypto_init ${unittest_crypto_init_srcs})
target_link_libraries(unittest_crypto_init global ${CMAKE_DL_LIBS} ${TCMALLOC_LIBS} ${UNITTEST_LIBS})
set_target_properties(unittest_crypto_init PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})

# unittest_perf_counters
set(unittest_perf_counters_srcs perf_counters.cc)
add_executable(unittest_perf_counters ${unittest_perf_counters_srcs})
target_link_libraries(unittest_perf_counters global ${CMAKE_DL_LIBS} ${TCMALLOC_LIBS} ${UNITTEST_LIBS})
set_target_properties(unittest_perf_counters PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})

# unittest_admin_socket
set(unittest_admin_socket_srcs admin_socket.cc)
add_executable(unittest_admin_socket ${unittest_admin_socket_srcs})
target_link_libraries(unittest_admin_socket global ${CMAKE_DL_LIBS} ${TCMALLOC_LIBS} ${UNITTEST_LIBS})
set_target_properties(unittest_admin_socket PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})

# unittest_ceph_crypto
set(unittest_ceph_crypto_srcs ceph_crypto.cc)
add_executable(unittest_ceph_crypto ${unittest_ceph_crypto_srcs})
target_link_libraries(unittest_ceph_crypto global ${CMAKE_DL_LIBS} ${TCMALLOC_LIBS} ${UNITTEST_LIBS})
set_target_properties(unittest_ceph_crypto PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})

# unittest_utf8
set(unittest_utf8_srcs utf8.cc)
add_executable(unittest_utf8 ${unittest_utf8_srcs})
target_link_libraries(unittest_utf8 global ${CMAKE_DL_LIBS} ${TCMALLOC_LIBS} ${UNITTEST_LIBS})
set_target_properties(unittest_utf8 PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})

# unittest_mime
set(unittest_mime_srcs mime.cc)
add_executable(unittest_mime ${unittest_mime_srcs})
target_link_libraries(unittest_mime global ${CMAKE_DL_LIBS} ${TCMALLOC_LIBS} ${UNITTEST_LIBS})
set_target_properties(unittest_mime PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})

# unittest_escape
set(unittest_escape_srcs escape.cc)
add_executable(unittest_escape ${unittest_escape_srcs})
target_link_libraries(unittest_escape global ${CMAKE_DL_LIBS} ${TCMALLOC_LIBS} ${UNITTEST_LIBS})
set_target_properties(unittest_escape PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})

# unittest_chain_xattr
set(unittest_chain_xattr_srcs objectstore/chain_xattr.cc)
add_executable(unittest_chain_xattr ${unittest_chain_xattr_srcs})
target_link_libraries(unittest_chain_xattr global ${CMAKE_DL_LIBS} ${TCMALLOC_LIBS} ${UNITTEST_LIBS})
set_target_properties(unittest_chain_xattr PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})

# unittest_flatindex
set(unittest_flatindex_srcs os/TestFlatIndex.cc)
add_executable(unittest_flatindex ${unittest_flatindex_srcs})
target_link_libraries(unittest_flatindex global ${CMAKE_DL_LIBS} ${TCMALLOC_LIBS} ${UNITTEST_LIBS})
set_target_properties(unittest_flatindex PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})

# unittest_strtol
set(unittest_strtol_srcs strtol.cc)
add_executable(unittest_strtol ${unittest_strtol_srcs})
target_link_libraries(unittest_strtol global ${CMAKE_DL_LIBS} ${TCMALLOC_LIBS} ${UNITTEST_LIBS})
set_target_properties(unittest_strtol PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})

# unittest_confutils
set(unittest_confutils_srcs confutils.cc)
add_executable(unittest_confutils ${unittest_confutils_srcs})
target_link_libraries(unittest_confutils global ${CMAKE_DL_LIBS} ${TCMALLOC_LIBS} ${UNITTEST_LIBS})
set_target_properties(unittest_confutils PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})

# unittest_config
set(unittest_config_srcs common/test_config.cc)
add_executable(unittest_config ${unittest_config_srcs})
target_link_libraries(unittest_config global ${CMAKE_DL_LIBS} ${TCMALLOC_LIBS} ${UNITTEST_LIBS})
set_target_properties(unittest_config PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})

# unittest_context
set(unittest_context_srcs common/test_context.cc)
add_executable(unittest_context ${unittest_context_srcs})
target_link_libraries(unittest_context global ${CMAKE_DL_LIBS} ${TCMALLOC_LIBS} ${UNITTEST_LIBS})
set_target_properties(unittest_context PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})

# unittest_heartbeatmap
set(unittest_heartbeatmap_srcs heartbeat_map.cc)
add_executable(unittest_heartbeatmap ${unittest_heartbeatmap_srcs})
target_link_libraries(unittest_heartbeatmap global ${CMAKE_DL_LIBS} ${TCMALLOC_LIBS} ${UNITTEST_LIBS})
set_target_properties(unittest_heartbeatmap PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})

if(${WITH_RADOSGW})
  # unittest_formatter
  set(unittest_formatter_srcs formatter.cc rgw/rgw_formats.cc)
  add_executable(unittest_formatter ${unittest_formatter_srcs})
  target_link_libraries(unittest_formatter global ${CMAKE_DL_LIBS} ${TCMALLOC_LIBS} ${UNITTEST_LIBS})
  set_target_properties(unittest_formatter PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})
endif(${WITH_RADOSGW})

# unittest_libcephfs_config
set(unittest_libcephfs_config_srcs libcephfs_config.cc)
add_executable(unittest_libcephfs_config ${unittest_libcephfs_config_srcs})
target_link_libraries(unittest_libcephfs_config cephfs ${CMAKE_DL_LIBS} ${TCMALLOC_LIBS} ${UNITTEST_LIBS})
set_target_properties(unittest_libcephfs_config PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})

# unittest_lfnindex
set(unittest_lfnindex_srcs os/TestLFNIndex.cc)
add_executable(unittest_lfnindex ${unittest_lfnindex_srcs})
target_link_libraries(unittest_lfnindex os global ${CMAKE_DL_LIBS} ${TCMALLOC_LIBS} ${UNITTEST_LIBS})
set_target_properties(unittest_lfnindex PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})

# unittest_librados_config
set(unittest_librados_config_srcs librados/librados_config.cc)
add_executable(unittest_librados_config ${unittest_librados_config_srcs})
target_link_libraries(unittest_librados_config librados ${CMAKE_DL_LIBS} ${TCMALLOC_LIBS} ${UNITTEST_LIBS})
set_target_properties(unittest_librados_config PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})

# unittest_daemon_config
set(unittest_daemon_config_srcs daemon_config.cc)
add_executable(unittest_daemon_config ${unittest_daemon_config_srcs})
target_link_libraries(unittest_daemon_config ${CMAKE_DL_LIBS} ${TCMALLOC_LIBS} ${UNITTEST_LIBS})
set_target_properties(unittest_daemon_config PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})

# unittest_mon_moncap
set(unittest_mon_moncap_srcs mon/moncap.cc)
add_executable(unittest_mon_moncap ${unittest_mon_moncap_srcs})
target_link_libraries(unittest_mon_moncap mon global ${CMAKE_DL_LIBS} ${TCMALLOC_LIBS} ${UNITTEST_LIBS})
set_target_properties(unittest_mon_moncap PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})

# unittest_ipaddr
add_executable(unittest_ipaddr test_ipaddr.cc)
target_link_libraries(unittest_ipaddr mon global ${CMAKE_DL_LIBS} ${TCMALLOC_LIBS} ${UNITTEST_LIBS})
set_target_properties(unittest_ipaddr PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})

# unittest_texttable
set(unittest_texttable_srcs test_texttable.cc)
add_executable(unittest_texttable ${unittest_texttable_srcs})
target_link_libraries(unittest_texttable mon global ${CMAKE_DL_LIBS} ${TCMALLOC_LIBS} ${UNITTEST_LIBS})
set_target_properties(unittest_texttable PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})

# unittest_on_exit
set(unittest_on_exit_srcs on_exit.cc)
add_executable(unittest_on_exit ${unittest_on_exit_srcs})
target_link_libraries(unittest_on_exit ${CMAKE_DL_LIBS} ${TCMALLOC_LIBS} ${UNITTEST_LIBS})
set_target_properties(unittest_on_exit PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})

if(${WITH_RADOSGW})
  # test_cors
  set(test_cors_srcs test_cors.cc)
  add_executable(test_cors ${test_cors_srcs})
  target_link_libraries(test_cors rados rgw global curl uuid expat
    ${CMAKE_DL_LIBS} ${TCMALLOC_LIBS} ${UNITTEST_LIBS})
  set_target_properties(test_cors PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})

  # test_rgw_manifest
  set(test_rgw_manifest_srcs rgw/test_rgw_manifest.cc)
  add_executable(test_rgw_manifest ${test_rgw_manifest_srcs})
  target_link_libraries(test_rgw_manifest rados rgw global curl uuid expat
    ${CMAKE_DL_LIBS} ${TCMALLOC_LIBS} ${UNITTEST_LIBS} ${CRYPTO_LIBS})
  set_target_properties(test_rgw_manifest PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})

  # test_cls_rgw_meta
  set(test_cls_rgw_meta_srcs rgw/test_rgw_admin_meta.cc)
  add_executable(test_cls_rgw_meta ${test_cls_rgw_meta_srcs})
  target_link_libraries(test_cls_rgw_meta rados rgw global curl uuid expat
    cls_version_client cls_log_client cls_statelog_client cls_refcount_client
    cls_rgw_client cls_user_client cls_lock_client
    ${CMAKE_DL_LIBS} ${TCMALLOC_LIBS} ${UNITTEST_LIBS} ${CRYPTO_LIBS})
  set_target_properties(test_cls_rgw_log PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})

  # test_cls_rgw_log
  set(test_cls_rgw_log_srcs test_rgw_admin_log.cc)
  add_executable(test_cls_rgw_log ${test_cls_rgw_log_srcs})
  target_link_libraries(test_cls_rgw_log rados rgw global curl uuid expat
    cls_version_client cls_log_client cls_statelog_client cls_refcount_client
    cls_rgw_client cls_user_client cls_lock_client
    ${CMAKE_DL_LIBS} ${TCMALLOC_LIBS} ${UNITTEST_LIBS} ${CRYPTO_LIBS})
  set_target_properties(test_cls_rgw_meta PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})

  # test_cls_rgw_opstate
  set(test_cls_rgw_opstate_srcs test_rgw_admin_opstate.cc)
  add_executable(test_cls_rgw_opstate ${test_cls_rgw_opstate_srcs})
  target_link_libraries(test_cls_rgw_opstate rados rgw global curl uuid expat
    cls_version_client cls_log_client cls_statelog_client cls_refcount_client
    cls_rgw_client cls_user_client cls_lock_client
    ${CMAKE_DL_LIBS} ${TCMALLOC_LIBS} ${UNITTEST_LIBS} ${CRYPTO_LIBS})
  set_target_properties(test_cls_rgw_opstate PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})
endif(${WITH_RADOSGW})

# radostest 
set(libradostest_srcs librados/test.cc librados/TestCase.cc)
add_library(radostest STATIC ${libradostest_srcs})
set_target_properties(radostest PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})

# multi_stress_watch
add_executable(multi_stress_watch multi_stress_watch.cc)
target_link_libraries(multi_stress_watch librados global radostest ${CMAKE_DL_LIBS} ${TCMALLOC_LIBS})

add_executable(test_librbd librbd/test_librbd.cc)
set_target_properties(test_librbd PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})
target_link_libraries(test_librbd librbd librados ${UNITTEST_LIBS} global radostest)

add_executable(test_librbd_fsx librbd/fsx.c)
target_link_libraries(test_librbd_fsx librbd librados m)

add_executable(test_cls_rbd cls_rbd/test_cls_rbd.cc)
set_target_properties(test_cls_rbd PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})
target_link_libraries(test_cls_rbd librados cls_rbd_client cls_lock_client global ${UNITTEST_LBS} radostest)

add_executable(test_cls_refcount cls_refcount/test_cls_refcount.cc)
set_target_properties(test_cls_refcount PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})
target_link_libraries(test_cls_refcount librados cls_refcount_client global ${UNITTEST_LIBS} radostest)

add_executable(test_cls_version cls_version/test_cls_version.cc)
set_target_properties(test_cls_version PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})
target_link_libraries(test_cls_version librados cls_version_client global ${UNITTEST_LIBS} radostest)

add_executable(test_cls_log cls_log/test_cls_log.cc)
set_target_properties(test_cls_log PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})
target_link_libraries(test_cls_log librados cls_log_client global ${UNITTEST_LIBS} radostest)

add_executable(test_cls_statelog cls_statelog/test_cls_statelog.cc)
set_target_properties(test_cls_statelog PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})
target_link_libraries(test_cls_statelog librados cls_statelog_client global
  ${UNITTEST_LIBS} radostest)

add_executable(test_cls_replica_log cls_replica_log/test_cls_replica_log.cc)
set_target_properties(test_cls_replica_log PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})
target_link_libraries(test_cls_replica_log librados cls_replica_log_client global
  ${UNITTEST_LIBS} radostest)

add_executable(test_cls_lock cls_lock/test_cls_lock.cc)
set_target_properties(test_cls_lock PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})
target_link_libraries(test_cls_lock librados cls_lock_client ${UNITTEST_LIBS} radostest)

add_executable(test_cls_hello cls_hello/test_cls_hello.cc)
set_target_properties(test_cls_hello PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})
target_link_libraries(test_cls_hello librados ${UNITTEST_LIBS} global radostest)

if(${WITH_RADOSGW})
  add_executable(ceph_test_cls_rgw cls_rgw/test_cls_rgw.cc)
  set_target_properties(test_cls_rgw PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})
  target_link_libraries(test_cls_rgw librados cls_rgw_client ${UNITTEST_LIBS} radostest)
endif(${WITH_RADOSGW})

add_executable(test_mon_workloadgen mon/test_mon_workloadgen.cc)
target_link_libraries(test_mon_workloadgen os osdc global)

add_executable(test_rados_api_cmd librados/cmd.cc)
set_target_properties(test_rados_api_cmd PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})
target_link_libraries(test_rados_api_cmd librados ${UNITTEST_LIBS} radostest)

add_executable(test_rados_api_io librados/io.cc)
set_target_properties(test_rados_api_io PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})
target_link_libraries(test_rados_api_io librados ${UNITTEST_LIBS} radostest)

add_executable(test_rados_api_c_write_operations librados/c_write_operations.cc)
set_target_properties(test_rados_api_c_write_operations PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})
target_link_libraries(test_rados_api_c_write_operations librados ${UNITTEST_LIBS} radostest)

add_executable(test_rados_api_c_read_operations librados/c_read_operations.cc)
set_target_properties(test_rados_api_c_read_operations PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})
target_link_libraries(test_rados_api_c_read_operations librados ${UNITTEST_LIBS} radostest)

add_executable(test_rados_api_aio librados/aio.cc)
set_target_properties(test_rados_api_aio PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})
target_link_libraries(test_rados_api_aio librados ${UNITTEST_LIBS} radostest)

add_executable(test_rados_api_list librados/list.cc)
set_target_properties(test_rados_api_list PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})
target_link_libraries(test_rados_api_list librados ${UNITTEST_LIBS} radostest)

add_executable(test_rados_api_pool librados/pool.cc)
set_target_properties(test_rados_api_pool PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})
target_link_libraries(test_rados_api_pool librados ${UNITTEST_LIBS} radostest)

add_executable(test_rados_api_stat librados/stat.cc)
set_target_properties(test_rados_api_stat PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})
target_link_libraries(test_rados_api_stat librados ${UNITTEST_LIBS} radostest)

add_executable(test_rados_api_watch_notify librados/watch_notify.cc)
set_target_properties(test_rados_api_watch_notify PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})
target_link_libraries(test_rados_api_watch_notify librados ${UNITTEST_LIBS} radostest)

add_executable(test_rados_api_cls librados/cls.cc)
set_target_properties(test_rados_api_cls PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})
target_link_libraries(test_rados_api_cls librados ${UNITTEST_LIBS} radostest)

add_executable(test_rados_api_misc librados/misc.cc)
set_target_properties(test_rados_api_misc PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})
target_link_libraries(test_rados_api_misc librados ${UNITTEST_LIBS} radostest)

add_executable(test_rados_api_lock librados/lock.cc)
set_target_properties(test_rados_api_lock PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})
target_link_libraries(test_rados_api_lock librados ${UNITTEST_LIBS} radostest)

if(${WITH_CEPHFS})
  add_executable(test_libcephfs libcephfs/test.cc
    libcephfs/readdir_r_cb.cc libcephfs/caps.cc
    libcephfs/multiclient.cc)
  set_target_properties(test_libcephfs PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})
  target_link_libraries(test_libcephfs libcephfs ${UNITTEST_LIBS})
endif(${WITH_CEPHFS})  

add_executable(test_objectstore objectstore/store_test.cc)
set_target_properties(test_objectstore PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})
target_link_libraries(test_objectstore os ${UNITTEST_LIBS} global)

add_executable(test_objectstore_workloadgen objectstore/workload_generator.cc
  objectstore/TestObjectStoreState.cc)
target_link_libraries(test_objectstore_workloadgen os global)

add_executable(test_filestore_idempotent objectstore/test_idempotent.cc
  objectstore/FileStoreTracker.cc common/ObjectContents.cc)
target_link_libraries(test_filestore_idempotent os global)

add_executable(test_filestore_idempotent_sequence
  objectstore/test_idempotent_sequence.cc
  objectstore/DeterministicOpSequence.cc
  objectstore/TestObjectStoreState.cc
  objectstore/FileStoreDiff.cc)
target_link_libraries(test_filestore_idempotent_sequence os global)

add_executable(test_xattr_bench xattr_bench.cc)
set_target_properties(test_xattr_bench PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})
target_link_libraries(test_xattr_bench os ${UNITTEST_LIBS} global)

add_executable(test_filejournal test_filejournal.cc)
set_target_properties(test_filejournal PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})
target_link_libraries(test_filejournal os ${UNITTEST_LIBS} global)

add_executable(test_stress_watch test_stress_watch.cc)
set_target_properties(test_stress_watch PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})
target_link_libraries(test_stress_watch librados ${UNITTEST_LIBS} radostest)

add_executable(test_objectcacher_stress osdc/object_cacher_stress.cc
  osdc/FakeWriteback.cc)
target_link_libraries(test_objectcacher_stress osdc global)

add_executable(test_object_map ObjectMap/test_object_map.cc
  ObjectMap/KeyValueDBMemory.cc)
set_target_properties(test_object_map PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})
target_link_libraries(test_object_map os ${UNITTEST_LIBS} global)

add_executable(test_keyvaluedb_atomicity ObjectMap/test_keyvaluedb_atomicity.cc)
set_target_properties(test_keyvaluedb_atomicity PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})
target_link_libraries(test_keyvaluedb_atomicity os ${UNITTEST_LIBS} global)

add_executable(test_keyvaluedb_iterators ObjectMap/test_keyvaluedb_iterators.cc
  ObjectMap/KeyValueDBMemory.cc)
set_target_properties(test_keyvaluedb_iterators PROPERTIES COMPILE_FLAGS ${UNITTEST_CXX_FLAGS})
target_link_libraries(test_keyvaluedb_iterators os ${UNITTEST_LIBS} global)

if(${WITH_FUSE})
  add_executable(test_cfuse_cache_invalidate test_cfuse_cache_invalidate.cc)
endif(${WITH_FUSE})

if(${WITH_CEPHFS})
  add_executable(test_c_headers test_c_headers.c)
  target_link_libraries(test_c_headers librados libcephfs)
endif(${WITH_CEPHFS})

add_executable(test_get_blkdev_size test_get_blkdev_size.cc)
target_link_libraries(test_get_blkdev_size libcommon)