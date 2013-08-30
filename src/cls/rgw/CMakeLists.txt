message( STATUS "rgw is here" )

set(cls_rgw cls_rgw.cc)

add_library(clsrgw STATIC ${cls_rgw})

set(cls_rgw_client cls_rgw_client.cc)

add_library(clsrgwclient STATIC ${cls_rgw_client})

set(cls_rgw_ops cls_rgw_ops.cc)

add_library(clsrgwops STATIC ${cls_rgw_ops})

set(cls_rgw_types cls_rgw_types.cc)

add_library(clsrgwtypes STATIC ${cls_rgw_types})
