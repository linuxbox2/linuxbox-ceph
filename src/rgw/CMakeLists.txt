if(${WITH_RADOSGW})
add_definitions("-Woverloaded-virtual")

set(librgw_a_srcs
	../rgw/librgw.cc 
	../rgw/rgw_acl.cc 
	../rgw/rgw_acl_s3.cc 
	../rgw/rgw_acl_swift.cc 
	../rgw/rgw_client_io.cc 
	../rgw/rgw_fcgi.cc 
	../rgw/rgw_xml.cc 
	../rgw/rgw_usage.cc 
	../rgw/rgw_json_enc.cc 
	../rgw/rgw_user.cc 
	../rgw/rgw_bucket.cc
        ../rgw/rgw_tools.cc 
	../rgw/rgw_rados.cc 
	../rgw/rgw_op.cc 
	../rgw/rgw_common.cc 
	../rgw/rgw_cache.cc 
	../rgw/rgw_formats.cc 
	../rgw/rgw_log.cc 
	../rgw/rgw_multi.cc 
	../rgw/rgw_policy_s3.cc 
	../rgw/rgw_gc.cc 
	../rgw/rgw_multi_del.cc 
	../rgw/rgw_env.cc 
	../rgw/rgw_cors.cc 
	../rgw/rgw_cors_s3.cc
)

add_library(librgw_a ${librgw_a_srcs})

set(libcls_rgw_client_a_srcs 
	../cls/rgw/cls_rgw_client.cc 
	../cls/rgw/cls_rgw_types.cc 
	../cls/rgw/cls_rgw_ops.cc
)

add_library(libcls_rgw_client ${libcls_rgw_client_a_srcs})

set(libcls_lock_client_a_srcs
	../cls/lock/cls_lock_client.cc  
	../cls/lock/cls_lock_types.cc 
	../cls/lock/cls_lock_ops.cc
)

add_library(libcls_lock_client ${libcls_lock_client_a_srcs})

set(libcls_refcount_client_a_srcs
	../cls/refcount/cls_refcount_client.cc
)

add_library(libcls_refcount_client ${libcls_refcount_client_a_srcs})

include_directories("..")

endif(${WITH_RADOSGW})
