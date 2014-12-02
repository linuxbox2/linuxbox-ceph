# LRC plugin

set(lrc_srcs
  ErasureCodePluginLrc.cc
  ErasureCodeLrc.cc
  $<TARGET_OBJECTS:erasure_code_objs>
)

add_library(ec_lrc SHARED ${lrc_srcs})
add_dependencies(ec_lrc ${CMAKE_SOURCE_DIR}/src/ceph_ver.h)
set_target_properties(ec_lrc PROPERTIES VERSION 1.0.0 SOVERSION 1)
target_link_libraries(ec_lrc json_spirit pthread)
install(TARGETS ec_lrc DESTINATION lib/erasure-code)
