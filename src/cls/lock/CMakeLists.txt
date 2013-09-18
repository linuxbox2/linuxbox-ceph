message( STATUS "cls_lock is here" )

set(cls_lock_files
	../lock/cls_lock_client.cc  
	../lock/cls_lock_types.cc 
	../lock/cls_lock_ops.cc
)

#libcls_lock_client_a_SOURCES =  \
#	cls/lock/cls_lock_client.cc  \
#	cls/lock/cls_lock_types.cc \
#	cls/lock/cls_lock_ops.cc
#noinst_LIBRARIES += libcls_lock_client.a

include_directories("../..")

add_library(cls_lock ${cls_lock_files})


