set(librgw_a_srcs
	librgw.cc
	rgw_acl.cc
	rgw_acl_s3.cc
	rgw_acl_swift.cc
	rgw_auth_s3.cc
	rgw_bucket.cc
	rgw_client_io.cc 
	rgw_cache.cc
	rgw_common.cc
	rgw_cors.cc
	rgw_cors_s3.cc
	rgw_env.cc
	rgw_fcgi.cc
	rgw_formats.cc
	rgw_gc.cc
	rgw_json_enc.cc
	rgw_keystone.cc
	rgw_log.cc
	rgw_metadata.cc
	rgw_multi.cc
	rgw_multi_del.cc
	rgw_op.cc
	rgw_policy_s3.cc
	rgw_quota.cc
	rgw_rados.cc
	rgw_replica_log.cc
        rgw_tools.cc
	rgw_usage.cc
	rgw_user.cc
	rgw_xml.cc
)

add_library(librgw_a ${librgw_a_srcs})

target_link_libraries(librgw_a cls_log cls_statelog cls_refcount cls_replica_log cls_rgw cls_user cls_version)
 
set(radosgw_srcs
	rgw_http_client.cc 
	rgw_main.cc
	rgw_resolve.cc 
	rgw_rest.cc 
	rgw_rest_bucket.cc 
	rgw_rest_client.cc 
	rgw_rest_config.cc 
	rgw_rest_conn.cc 
	rgw_rest_log.cc 
	rgw_rest_metadata.cc 
	rgw_rest_opstate.cc 
	rgw_rest_replica_log.cc 
	rgw_rest_s3.cc 
	rgw_rest_swift.cc 
	rgw_rest_usage.cc 
	rgw_rest_user.cc 
	rgw_swift.cc 
	rgw_swift_auth.cc
	rgw_loadgen.cc
	rgw_civetweb.cc
	${CMAKE_SOURCE_DIR}/src/civetweb/src/civetweb.c
)

add_executable(radosgw ${radosgw_srcs})

target_link_libraries(radosgw librgw_a librados curl expat boost_regex global fcgi resolv)
install(TARGETS radosgw DESTINATION bin)

set_target_properties(radosgw 
PROPERTIES COMPILE_FLAGS "-Woverloaded-virtual -I${CMAKE_SOURCE_DIR}/src/civetweb/include")

set_target_properties(librgw_a 
PROPERTIES COMPILE_FLAGS -Woverloaded-virtual)

get_target_property(radosgw_flags radosgw COMPILE_FLAGS)

get_target_property(librgw_a_flags librgw_a COMPILE_FLAGS)

message(STATUS ${radosgw_flags})
message(STATUS ${librgw_a_flags})
