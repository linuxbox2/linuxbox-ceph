set(rgw_sts_srcs
  sts_main.cc
  rgw_rest_sts.cc
  rgw_dummy.cc)

# ugh
list(APPEND rgw_sts_srcs
  ../rgw/rgw_civetweb.cc
  ../rgw/rgw_civetweb_frontend.cc
  ../rgw/rgw_civetweb_log.cc)

if (WITH_RADOSGW_ASIO_FRONTEND)
  list(APPEND rgw_sts_srcs
    ../rgw/rgw_asio_client.cc
    ../rgw/rgw_asio_frontend.cc)
endif (WITH_RADOSGW_ASIO_FRONTEND)
# end ugh

add_executable(rgw-sts ${rgw_sts_srcs}  $<TARGET_OBJECTS:civetweb_common_objs>)
target_link_libraries(rgw-sts rgw_a
  fcgi ${LIB_RESOLV}
  ${CURL_LIBRARIES} ${EXPAT_LIBRARIES} ${SSL_LIBRARIES} ${BLKID_LIBRARIES}
  ${ALLOC_LIBS})
target_include_directories(rgw-sts PRIVATE
"${CMAKE_SOURCE_DIR}/src/rgw"
)
install(TARGETS rgw-sts DESTINATION bin)
