// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2009-2011 New Dream Network
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_LIB_H
#define CEPH_LIB_H

#include <utime.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/statvfs.h>
#include <sys/socket.h>
#include <stdint.h>
#include <stdbool.h>

// FreeBSD compatibility
#ifdef __FreeBSD__
typedef off_t loff_t;
typedef off_t off64_t;
#endif

#ifdef __cplusplus
extern "C" {
#endif

/*
 * On FreeBSD the offset is 64 bit, but libc doesn't announce it in the way glibc does.
 */
#if !defined(__FreeBSD__) && !defined(__USE_FILE_OFFSET64)
# error libceph: must define __USE_FILE_OFFSET64 or readdir results will be corrupted
#endif

/*
 * XXXX redeclarations from ceph_fs.h, rados.h, etc.  We need more of this
 * in the interface, but shouldn't be re-typing it (and using different
 * C data types).
 */
#ifndef __cplusplus

#define CEPH_INO_ROOT  1
#define CEPH_NOSNAP  ((uint64_t)(-2))

struct ceph_file_layout {
	/* file -> object mapping */
	uint32_t fl_stripe_unit;     /* stripe unit, in bytes.  must be multiple
				      of page size. */
	uint32_t fl_stripe_count;    /* over this many objects */
	uint32_t fl_object_size;     /* until objects are this big, then move to
				      new objects */
	uint32_t fl_cas_hash;        /* 0 = none; 1 = sha256 */

	/* pg -> disk layout */
	uint32_t fl_object_stripe_unit;  /* for per-object parity, if any */

	/* object -> pg layout */
	uint32_t fl_pg_preferred; /* preferred primary for pg (-1 for none) */
	uint32_t fl_pg_pool;      /* namespace, crush ruleset, rep level */
} __attribute__ ((packed));


typedef struct _inodeno_t {
  uint64_t val;
} inodeno_t;

typedef struct _snapid_t {
  uint64_t val;
} snapid_t;

typedef struct vinodeno_t {
  inodeno_t ino;
  snapid_t snapid;
} vinodeno_t;

typedef struct Fh Fh;

#endif /* __cplusplus */

struct ceph_mount_info;
struct ceph_dir_result;
struct CephContext;

/* setattr mask bits */
#ifndef CEPH_SETATTR_MODE
# define CEPH_SETATTR_MODE   1
# define CEPH_SETATTR_UID    2
# define CEPH_SETATTR_GID    4
# define CEPH_SETATTR_MTIME  8
# define CEPH_SETATTR_ATIME 16
# define CEPH_SETATTR_SIZE  32
# define CEPH_SETATTR_CTIME 64
#endif


const char *ceph_version(int *major, int *minor, int *patch);

/* initialization */
int ceph_create(struct ceph_mount_info **cmount, const char * const id);

/* initialization with an existing configuration */
int ceph_create_with_context(struct ceph_mount_info **cmount, struct CephContext *conf);

/* Activate the mount */
int ceph_mount(struct ceph_mount_info *cmount, const char *root);

/* Destroy the ceph mount instance */
void ceph_shutdown(struct ceph_mount_info *cmount);

/* Config
 *
 * Functions for manipulating the Ceph configuration at runtime.
 */
int ceph_conf_read_file(struct ceph_mount_info *cmount, const char *path_list);

int ceph_conf_parse_argv(struct ceph_mount_info *cmount, int argc, const char **argv);

/* Sets a configuration value from a string.
 * Returns 0 on success, error code otherwise. */
int ceph_conf_set(struct ceph_mount_info *cmount, const char *option, const char *value);

/* Returns a configuration value as a string.
 * If len is positive, that is the maximum number of bytes we'll write into the
 * buffer. If len == -1, we'll call malloc() and set *buf.
 * Returns 0 on success, error code otherwise. Returns ENAMETOOLONG if the
 * buffer is too short. */
int ceph_conf_get(struct ceph_mount_info *cmount, const char *option, char *buf, size_t len);

int ceph_statfs(struct ceph_mount_info *cmount, const char *path, struct statvfs *stbuf);

/* Get the current working directory.
 *
 * The pointer you get back from this function will continue to be valid until
 * the *next* call you make to ceph_getcwd, at which point it will be invalidated.
 */
const char* ceph_getcwd(struct ceph_mount_info *cmount);

int ceph_chdir(struct ceph_mount_info *cmount, const char *s);

int ceph_opendir(struct ceph_mount_info *cmount, const char *name, struct ceph_dir_result **dirpp);
int ceph_closedir(struct ceph_mount_info *cmount, struct ceph_dir_result *dirp);
struct dirent * ceph_readdir(struct ceph_mount_info *cmount, struct ceph_dir_result *dirp);
int ceph_readdir_r(struct ceph_mount_info *cmount, struct ceph_dir_result *dirp, struct dirent *de);
int ceph_readdirplus_r(struct ceph_mount_info *cmount, struct ceph_dir_result *dirp, struct dirent *de,
		       struct stat *st, int *stmask);
int ceph_getdents(struct ceph_mount_info *cmount, struct ceph_dir_result *dirp, char *name, int buflen);
/**
 * This returns the used buffer space on success, -ERANGE if the buffer
 * is not large enough to hold a name, or -errno on other issues.
 */
int ceph_getdnames(struct ceph_mount_info *cmount, struct ceph_dir_result *dirp, char *name, int buflen);
void ceph_rewinddir(struct ceph_mount_info *cmount, struct ceph_dir_result *dirp);
loff_t ceph_telldir(struct ceph_mount_info *cmount, struct ceph_dir_result *dirp);
void ceph_seekdir(struct ceph_mount_info *cmount, struct ceph_dir_result *dirp, loff_t offset);

int ceph_link(struct ceph_mount_info *cmount, const char *existing, const char *newname);
int ceph_unlink(struct ceph_mount_info *cmount, const char *path);
int ceph_rename(struct ceph_mount_info *cmount, const char *from, const char *to);

/* dirs */
int ceph_mkdir(struct ceph_mount_info *cmount, const char *path, mode_t mode);
int ceph_mkdirs(struct ceph_mount_info *cmount, const char *path, mode_t mode);
int ceph_rmdir(struct ceph_mount_info *cmount, const char *path);

/* symlinks */
int ceph_readlink(struct ceph_mount_info *cmount, const char *path, char *buf, loff_t size);
int ceph_symlink(struct ceph_mount_info *cmount, const char *existing, const char *newname);

/* inode stuff */
int ceph_lstat(struct ceph_mount_info *cmount, const char *path, struct stat *stbuf);

int ceph_setattr(struct ceph_mount_info *cmount, const char *relpath, struct stat *attr, int mask);
int ceph_chmod(struct ceph_mount_info *cmount, const char *path, mode_t mode);
int ceph_chown(struct ceph_mount_info *cmount, const char *path, uid_t uid, gid_t gid);
int ceph_utime(struct ceph_mount_info *cmount, const char *path, struct utimbuf *buf);
int ceph_truncate(struct ceph_mount_info *cmount, const char *path, loff_t size);

/* file ops */
int ceph_mknod(struct ceph_mount_info *cmount, const char *path, mode_t mode, dev_t rdev);
int ceph_open(struct ceph_mount_info *cmount, const char *path, int flags, mode_t mode);
int ceph_close(struct ceph_mount_info *cmount, int fd);
loff_t ceph_lseek(struct ceph_mount_info *cmount, int fd, loff_t offset, int whence);
int ceph_read(struct ceph_mount_info *cmount, int fd, char *buf, loff_t size, loff_t offset);
int ceph_write(struct ceph_mount_info *cmount, int fd, const char *buf, loff_t size,
	       loff_t offset);
int ceph_ftruncate(struct ceph_mount_info *cmount, int fd, loff_t size);
int ceph_fsync(struct ceph_mount_info *cmount, int fd, int syncdataonly);
int ceph_fstat(struct ceph_mount_info *cmount, int fd, struct stat *stbuf);

int ceph_sync_fs(struct ceph_mount_info *cmount);

/* xattr support */
int ceph_getxattr(struct ceph_mount_info *cmount, const char *path, const char *name, 
	void *value, size_t size);
int ceph_lgetxattr(struct ceph_mount_info *cmount, const char *path, const char *name, 
	void *value, size_t size);
int ceph_listxattr(struct ceph_mount_info *cmount, const char *path, char *list, size_t size);
int ceph_llistxattr(struct ceph_mount_info *cmount, const char *path, char *list, size_t size);
int ceph_removexattr(struct ceph_mount_info *cmount, const char *path, const char *name);
int ceph_lremovexattr(struct ceph_mount_info *cmount, const char *path, const char *name);
int ceph_setxattr(struct ceph_mount_info *cmount, const char *path, const char *name, 
	const void *value, size_t size, int flags);
int ceph_lsetxattr(struct ceph_mount_info *cmount, const char *path, const char *name, 
	const void *value, size_t size, int flags);



/* expose file layout */
int ceph_get_file_stripe_unit(struct ceph_mount_info *cmount, int fh);
int ceph_get_file_pool(struct ceph_mount_info *cmount, int fh);
int ceph_get_file_replication(struct ceph_mount_info *cmount, int fh);
int ceph_get_file_stripe_address(struct ceph_mount_info *cmount, int fd, loff_t offset,
				 struct sockaddr_storage *addr, int naddr);

/* set default layout for new files */
int ceph_set_default_file_stripe_unit(struct ceph_mount_info *cmount, int stripe);
int ceph_set_default_file_stripe_count(struct ceph_mount_info *cmount, int count);
int ceph_set_default_object_size(struct ceph_mount_info *cmount, int size);
int ceph_set_default_preferred_pg(struct ceph_mount_info *cmount, int osd);
int ceph_set_default_file_replication(struct ceph_mount_info *cmount, int replication);

/* read from local replicas when possible */
int ceph_localize_reads(struct ceph_mount_info *cmount, int val);

/* return osd on local node, if any */
int ceph_get_local_osd(struct ceph_mount_info *cmount);

/* Get the CephContext of this mount */
struct CephContext *ceph_get_mount_context(struct ceph_mount_info *cmount);

/* Low Level */
int ceph_ll_lookup(struct ceph_mount_info *cmount, struct vinodeno_t parent,
		   const char *name, struct stat *attr, int uid, int gid);

int ceph_ll_forget(struct ceph_mount_info *cmount, struct vinodeno_t vino,
		    int count);
int ceph_ll_walk(struct ceph_mount_info *cmount, const char *name,
		 struct stat *attr);
int ceph_ll_getattr(struct ceph_mount_info *cmount, struct vinodeno_t vi,
		    struct stat *attr, int uid, int gid);
int ceph_ll_setattr(struct ceph_mount_info *cmount, struct vinodeno_t vi,
		    struct stat *st, int mask, int uid, int gid);
int ceph_ll_open(struct ceph_mount_info *cmount, struct vinodeno_t vi, int flags,
		 struct Fh **filehandle, int uid, int gid);
loff_t ceph_ll_lseek(struct ceph_mount_info *cmount, struct Fh* filehandle,
		     loff_t offset, int whence);
int ceph_ll_read(struct ceph_mount_info *cmount, struct Fh* filehandle,
		 int64_t off, uint64_t len, char* buf);
int ceph_ll_fsync(struct ceph_mount_info *cmount, struct Fh *fh,
		  int syncdataonly);
int ceph_ll_write(struct ceph_mount_info *cmount, struct Fh* filehandle,
		  int64_t off, uint64_t len, const char *data);
int ceph_ll_close(struct ceph_mount_info *cmount, struct Fh* filehandle);
int ceph_ll_getxattr(struct ceph_mount_info *cmount, struct vinodeno_t vino,
		     const char *name, void *value, size_t size, int uid,
		     int gid);
int ceph_ll_setxattr(struct ceph_mount_info *cmount, struct vinodeno_t vino,
		     const char *name, const void *value, size_t size,
		     int flags, int uid, int gid);
int ceph_ll_removexattr(struct ceph_mount_info *cmount, struct vinodeno_t vino,
			const char *name, int uid, int gid);
int ceph_ll_create(struct ceph_mount_info *cmount, struct vinodeno_t parent,
		   const char *name, mode_t mode, int flags,
		   struct Fh **filehandle, struct stat *attr, int uid, int gid);
int ceph_ll_mkdir(struct ceph_mount_info *cmount, struct vinodeno_t parent,
		  const char *name, mode_t mode, struct stat *attr, int uid,
		  int gid);
int ceph_ll_link(struct ceph_mount_info *cmount, struct vinodeno_t obj,
		 struct vinodeno_t newparrent, const char *name,
		 struct stat *attr, int uid, int gid);
int ceph_ll_truncate(struct ceph_mount_info *cmount, struct vinodeno_t obj,
		     uint64_t length, int uid, int gid);
int ceph_ll_opendir(struct ceph_mount_info *cmount, struct vinodeno_t vino,
		    struct ceph_dir_result **dirpp, int uid, int gid);
int ceph_ll_releasedir(struct ceph_mount_info *cmount,
		       struct ceph_dir_result* dir);
int ceph_ll_rename(struct ceph_mount_info *cmount, struct vinodeno_t parent,
		   const char *name, struct vinodeno_t newparent,
		   const char *newname, int uid, int gid);
int ceph_ll_unlink(struct ceph_mount_info *cmount, struct vinodeno_t vino,
		   const char *name, int uid, int gid);
int ceph_ll_statfs(struct ceph_mount_info *cmount, struct vinodeno_t vino,
		   struct statvfs *stbuf);
int ceph_ll_readlink(struct ceph_mount_info *cmount, struct vinodeno_t vino,
		     char **value, int uid, int gid);
int ceph_ll_symlink(struct ceph_mount_info *cmount, struct vinodeno_t parent,
		    const char *name, const char *value, struct stat *attr,
		    int uid, int gid);
int ceph_ll_rmdir(struct ceph_mount_info *cmount, struct vinodeno_t vino,
		  const char *name, int uid, int gid);
uint32_t ceph_ll_stripe_unit(struct ceph_mount_info *cmount,
			     struct vinodeno_t vino);
uint32_t ceph_ll_file_layout(struct ceph_mount_info *cmount,
			     struct vinodeno_t vino,
			     struct ceph_file_layout *layout);
uint64_t ceph_ll_snap_seq(struct ceph_mount_info *cmount,
			  struct vinodeno_t vino);
int ceph_ll_get_stripe_osd(struct ceph_mount_info *cmount,
			   struct vinodeno_t vino,
			   uint64_t blockno,
			   struct ceph_file_layout* layout);
int ceph_ll_num_osds(struct ceph_mount_info *cmount);
int ceph_ll_osdaddr(struct ceph_mount_info *cmount,
		    int osd, uint32_t *addr);
uint64_t ceph_ll_get_internal_offset(struct ceph_mount_info *cmount,
				     struct vinodeno_t vino, uint64_t blockno);
int ceph_ll_read_block(struct ceph_mount_info *cmount,
		       struct vinodeno_t vino, uint64_t blockid,
		       char* bl, uint64_t offset, uint64_t length,
		       struct ceph_file_layout* layout);
int ceph_ll_write_block(struct ceph_mount_info *cmount,
			vinodeno_t vino, uint64_t blockid,
			char* buf, uint64_t offset,
			uint64_t length, struct ceph_file_layout* layout,
			uint64_t snapseq, uint32_t sync);
int ceph_ll_commit_blocks(struct ceph_mount_info *cmount,
			  vinodeno_t vino, uint64_t offset, uint64_t range);
int ceph_ll_connectable_x(struct ceph_mount_info *cmount,
			  vinodeno_t vino, uint64_t* parent_ino,
			  uint32_t* parent_hash);
int ceph_ll_connectable_m(struct ceph_mount_info *cmount,
			  vinodeno_t* vino, uint64_t parent_ino,
			  uint32_t parent_hash);

uint32_t ceph_ll_hold_rw(struct ceph_mount_info *cmount,
			 vinodeno_t vino,
			 bool write,
			 void(*cb)(vinodeno_t, bool, void*),
			 void *opaque,
			 uint64_t* serial,
			 uint64_t* max_fs);

void ceph_ll_return_rw(struct ceph_mount_info *cmount,
		       vinodeno_t vino,
		       uint64_t serial);

#ifdef __cplusplus
}
#endif

#endif
