if(${WITH_RADOSGW})
add_definitions("-Woverloaded-virtual")

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

#set(libcls_rgw_client_a_srcs 
#	../cls/rgw/cls_rgw_client.cc 
#	../cls/rgw/cls_rgw_types.cc 
#	../cls/rgw/cls_rgw_ops.cc
#	../cls/rgw/cls_rgw.cc
#)

#add_library(libcls_rgw_client ${libcls_rgw_client_a_srcs})

#set(libcls_lock_client_a_srcs
#	../cls/lock/cls_lock_client.cc  
#	../cls/lock/cls_lock_types.cc 
#	../cls/lock/cls_lock_ops.cc
#)
#
#add_library(libcls_lock_client ${libcls_lock_client_a_srcs})

#set(libcls_refcount_client_a_srcs
#	../cls/refcount/cls_refcount_client.cc
#)

#add_library(libcls_refcount_client ${libcls_refcount_client_a_srcs})

include_directories("..")

endif(${WITH_RADOSGW})
