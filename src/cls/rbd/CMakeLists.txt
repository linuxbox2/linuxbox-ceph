message( STATUS "rbd is here" )

set(cls_rbd cls_rbd.cc)

add_library(clsrbd STATIC ${cls_rbd})

set(cls_rbd_client cls_rbd_client.cc)

add_library(clsrbdclient STATIC ${cls_rbd_client})
