add_subdirectory(rados)

#ceph-conf
set(ceph_conf_srcs
	ceph_conf.cc
)

add_executable(ceph-conf ${ceph_conf_srcs})
target_link_libraries(ceph-conf global)

#monmaptool
set(monmaptool_srcs
	monmaptool.cc
)

add_executable(monmaptool ${monmaptool_srcs})
target_link_libraries(monmaptool global)

#crushtool
set(crushtool_srcs
	crushtool.cc
)

add_executable(crushtool ${crushtool_srcs})
target_link_libraries(crushtool global)

#osdmaptool
set(osdomaptool_srcs 
	osdmaptool.cc
)

add_executable(osdmaptool  ${osdomaptool_srcs})
target_link_libraries(osdmaptool global)

#ceph-authtool
set(ceph_authtool_srcs
	ceph_authtool.cc
)

add_executable(ceph-authtool ${ceph_authtool_srcs})
target_link_libraries(ceph-authtool global)

#ceph_mon_store_converter
set(ceph_mon_store_converter_srcs
	mon_store_converter.cc
	${CMAKE_SOURCE_DIR}/src/mon/MonitorStore.cc
)

add_executable(ceph_mon_store_converter ${ceph_mon_store_converter_srcs})
target_link_libraries(ceph_mon_store_converter os global)


# add the install targets
install(TARGETS
  ceph-conf
  monmaptool
  osdmaptool
  crushtool
  ceph-authtool
  ceph_mon_store_converter
  DESTINATION bin)

