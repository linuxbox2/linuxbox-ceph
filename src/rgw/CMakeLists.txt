message( STATUS "rgw is here" )

add_definitions(cxxflags ${CXXFLAGS})
add_definitions(cflags ${CFLAGS})
add_definitions(cryptoflags ${CRYPTO_CFLAGS})
add_definitions(cryptoflags -version-info 2:0:0 -export-symbols-regex '^rados_.* ${LDFLAGS})

set(rgwsrcs_a
	/home/ali/ceph-local/src/rgw/librgw.cc 
	/home/ali/ceph-local/src/rgw/rgw_acl.cc 
	/home/ali/ceph-local/src/rgw/rgw_acl_s3.cc 
	/home/ali/ceph-local/src/rgw/rgw_acl_swift.cc 
	/home/ali/ceph-local/src/rgw/rgw_client_io.cc 
	/home/ali/ceph-local/src/rgw/rgw_fcgi.cc 
	/home/ali/ceph-local/src/rgw/rgw_xml.cc 
	/home/ali/ceph-local/src/rgw/rgw_usage.cc 
	/home/ali/ceph-local/src/rgw/rgw_json_enc.cc 
	/home/ali/ceph-local/src/rgw/rgw_user.cc 
	/home/ali/ceph-local/src/rgw/rgw_bucket.cc
        /home/ali/ceph-local/src/rgw/rgw_tools.cc 
	/home/ali/ceph-local/src/rgw/rgw_rados.cc 
	/home/ali/ceph-local/src/rgw/rgw_op.cc 
	/home/ali/ceph-local/src/rgw/rgw_common.cc 
	/home/ali/ceph-local/src/rgw/rgw_cache.cc 
	/home/ali/ceph-local/src/rgw/rgw_formats.cc 
	/home/ali/ceph-local/src/rgw/rgw_log.cc 
	/home/ali/ceph-local/src/rgw/rgw_multi.cc 
	/home/ali/ceph-local/src/rgw/rgw_policy_s3.cc 
	/home/ali/ceph-local/src/rgw/rgw_gc.cc 
	/home/ali/ceph-local/src/rgw/rgw_multi_del.cc 
	/home/ali/ceph-local/src/rgw/rgw_env.cc 
	/home/ali/ceph-local/src/rgw/rgw_cors.cc 
	/home/ali/ceph-local/src/rgw/rgw_cors_s3.cc
)

add_library(rgw_srcs_a STATIC ${rgwsrcs_a})

#set (noinst_libraries
#/home/ali/ceph-local/src/librgw.a
#)

#add_library(noistlibraries STATIC ${noinst_libraries})

#i don't care about this now
#set (my_radosgw_ldadd
#/home/ali/ceph-local/src/librgw.a librados.la 
#/home/ali/ceph-local/src/libcls_rgw_client.a 
#/home/ali/ceph-local/src/libcls_lock_client.a 
#/home/ali/ceph-local/src/libcls_refcount_client.a
#)
#whatisthis -lcurl -lexpat \
#whatisthis       $(LIBGLOBAL_LDA) 


#add_library(radosgwldadd STATIC ${my_radosgw_ldadd})

set(rgwsrcs
	/home/ali/ceph-local/src/rgw/rgw_resolve.cc 
        /home/ali/ceph-local/src/rgw/rgw_rest.cc 
        /home/ali/ceph-local/src/rgw/rgw_rest_swift.cc 
        /home/ali/ceph-local/src/rgw/rgw_rest_s3.cc 
        /home/ali/ceph-local/src/rgw/rgw_rest_usage.cc 
        /home/ali/ceph-local/src/rgw/rgw_rest_user.cc 
        /home/ali/ceph-local/src/rgw/rgw_rest_bucket.cc 
        /home/ali/ceph-local/src/rgw/rgw_http_client.cc 
        /home/ali/ceph-local/src/rgw/rgw_swift.cc 
	/home/ali/ceph-local/src/rgw/rgw_swift_auth.cc 
	/home/ali/ceph-local/src/rgw/rgw_main.cc
)

add_library(rgw_srcs STATIC ${rgwsrcs})

# add_library(librados_libs 
# STATIC ${PTHREAD_LIBS}
# STATIC ${CRYPTO_LIBS}
# STATIC ${extralibs}
# )

#radosgw_LDADD = $(my_radosgw_ldadd) -lfcgi -lresolv
#radosgw_CXXFLAGS = ${AM_CXXFLAGS}
#bin_PROGRAMS += radosgw

set (radosgw_admin_SOURCES
/home/ali/ceph-local/src/rgw/rgw_admin.cc
)
#radosgw_admin_CXXFLAGS = ${AM_CXXFLAGS}
#radosgw_admin_LDADD = $(my_radosgw_ldadd)
ADD_executable(bin_PROGRAMS "${radosgw_admin_SOURCES}")


set (ceph_rgw_multiparser_SOURCES
/home/ali/ceph-local/src/rgw/rgw_multiparser.cc)
#ceph_rgw_multiparser_CXXFLAGS = ${AM_CXXFLAGS}
#ceph_rgw_multiparser_LDADD = $(my_radosgw_ldadd)
ADD_executable(bin_DEBUGPROGRAMS "${ceph_rgw_multiparser_SOURCES}")


#set (ceph_rgw_jsonparser_SOURCES
#/home/ali/ceph-local/src/rgw/rgw_jsonparser.cc
#/home/ali/ceph-local/src/rgw/rgw_common.cc 
#/home/ali/ceph-local/src/rgw/rgw_json_enc.cc
#)

ADD_executable(binary_debugprograms "${ceph_rgw_jsonparser_SOURCES}")

# ceph_rgw_jsonparser_CXXFLAGS = ${CRYPTO_CXXFLAGS} ${AM_CXXFLAGS}
#ceph_rgw_jsonparser_LDADD = $(my_radosgw_ldadd)

# make sure this is right bin_DEBUGPROGRAMS += ceph_rgw_jsonparser




