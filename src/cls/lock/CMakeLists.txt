message( STATUS "lock is here" )

set(cls_lock cls_lock.cc)

add_library(clslock STATIC ${cls_lock})

set(cls_lock_client cls_lock_client.cc)

add_library(clslockclient STATIC ${cls_lock_client})

set(cls_lock_ops cls_lock_ops.cc)

add_library(clslockops STATIC ${cls_lock_ops})

set(cls_lock_types cls_lock_types.cc)

add_library(clslocktypes STATIC ${cls_lock_types})
