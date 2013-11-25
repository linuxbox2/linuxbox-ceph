set(cls_replica_log_files
	cls_replica_log.cc
	cls_replica_log_client.cc
	cls_replica_log_ops.cc
	cls_replica_log_types.cc
)

add_library(cls_replica_log ${cls_replica_log_files})
