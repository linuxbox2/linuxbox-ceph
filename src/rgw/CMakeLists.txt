message( STATUS "rgw is here" )

set(librgw librgw.cc)

add_library(librgw STATIC ${librgw})

set(rgw_acl  rgw_acl.cc )

add_library(rgw_acl STATIC ${rgw_acl})

set(rgw_acl_s3 rgw_acl_s3.cc)

add_library(rgwacls3 STATIC ${rgw_acl_s3})


 
	  
	  
	rgw_acl_swift.cc  
	rgw_client_io.cc  
	 rgw_fcgi.cc  
	 rgw_xml.cc  
	 rgw_usage.cc  
	 rgw_json_enc.cc  
	 rgw_user.cc  
	 rgw_bucket.cc 
         rgw_tools.cc  
	 rgw_rados.cc  
	 rgw_op.cc  
	 rgw_common.cc  
	 rgw_cache.cc  
	 rgw_formats.cc  
	 rgw_log.cc  
	 rgw_multi.cc  
	 rgw_policy_s3.cc  
	 rgw_gc.cc  
	 rgw_multi_del.cc  
	 rgw_env.cc  
	 rgw_cors.cc  
	 rgw_cors_s3.cc
