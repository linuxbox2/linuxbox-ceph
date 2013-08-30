message( STATUS "refcount is here" )

set(cls_refcount cls_refcount.cc)

add_library(clsrefcount STATIC ${cls_refcount})

set(cls_refcount_client cls_refcount_client.cc)

add_library(clsrefclient STATIC ${cls_refcount_client})
