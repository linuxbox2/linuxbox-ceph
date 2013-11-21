
set(cls_lock_files
	../lock/cls_lock_client.cc  
	../lock/cls_lock_types.cc 
	../lock/cls_lock_ops.cc
)

include_directories("../..")

add_library(cls_lock ${cls_lock_files})


