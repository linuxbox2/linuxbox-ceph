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

#include <fcntl.h>
#include <iostream>
#include <string.h>
#include <string>

#include "client/Client.h"
#include "include/cephfs/libcephfs.h"
#include "common/Mutex.h"
#include "common/ceph_argparse.h"
#include "common/common_init.h"
#include "common/config.h"
#include "common/version.h"
#include "mon/MonClient.h"
#include "include/str_list.h"
#include "messages/MMonMap.h"
#include "msg/Messenger.h"
#include "include/assert.h"

class ceph_mount_info
{
public:
  ceph_mount_info(uint64_t msgr_nonce_, CephContext *cct_)
    : msgr_nonce(msgr_nonce_),
      mounted(false),
      client(NULL),
      monclient(NULL),
      messenger(NULL),
      cct(cct_)
  {
  }

  ~ceph_mount_info()
  {
    try {
      shutdown();
      if (cct) {
	cct->put();
	cct = NULL;
      }
    }
    catch (const std::exception& e) {
      // we shouldn't get here, but if we do, we want to know about it.
      lderr(cct) << "ceph_mount_info::~ceph_mount_info: caught exception: "
	         << e.what() << dendl;
    }
    catch (...) {
      // ignore
    }
  }

  int mount(const std::string &mount_root)
  {
    int ret;
    
    if (mounted)
      return -EDOM;

    common_init_finish(cct);

    //monmap
    monclient = new MonClient(cct);
    ret = -1000;
    if (monclient->build_initial_monmap() < 0)
      goto fail;

    //network connection
    messenger = Messenger::create(cct, entity_name_t::CLIENT(), "client", msgr_nonce);

    //at last the client
    ret = -1002;
    client = new Client(messenger, monclient);
    if (!client)
      goto fail;

    ret = -1003;
    if (messenger->start() != 0)
      goto fail;

    ret = client->init();
    if (ret)
      goto fail;

    ret = client->mount(mount_root);
    if (ret)
      goto fail;

    mounted = true;
    return 0;

  fail:
    shutdown();
    return ret;
  }

  void shutdown()
  {
    if (mounted) {
      client->unmount();
      mounted = false;
    }
    if (client) {
      client->shutdown();
      delete client;
      client = NULL;
    }
    if (messenger) {
      messenger->wait();
      delete messenger;
      messenger = NULL;
    }
    if (monclient) {
      delete monclient;
      monclient = NULL;
    }
  }

  int conf_read_file(const char *path_list)
  {
    std::deque<std::string> parse_errors;
    int ret = cct->_conf->parse_config_files(path_list, &parse_errors, NULL, 0);
    if (ret)
      return ret;
    cct->_conf->apply_changes(NULL);
    complain_about_parse_errors(cct, &parse_errors);
    return 0;
  }

  int conf_parse_argv(int argc, const char **argv)
  {
    int ret;
    vector<const char*> args;
    argv_to_vec(argc, argv, args);
    ret = cct->_conf->parse_argv(args);
    if (ret)
	return ret;
    cct->_conf->apply_changes(NULL);
    return 0;
  }

  int conf_set(const char *option, const char *value)
  {
    int ret = cct->_conf->set_val(option, value);
    if (ret)
      return ret;
    cct->_conf->apply_changes(NULL);
    return 0;
  }

  int conf_get(const char *option, char *buf, size_t len)
  {
    char *tmp = buf;
    return cct->_conf->get_val(option, &tmp, len);
  }

  Client *get_client()
  {
    return client;
  }

  const char *get_cwd()
  {
    client->getcwd(cwd);
    return cwd.c_str();
  }

  CephContext *get_ceph_context() const {
    return cct;
  }

private:
  uint64_t msgr_nonce;
  bool mounted;
  Client *client;
  MonClient *monclient;
  Messenger *messenger;
  CephContext *cct;
  std::string cwd;
};

extern "C" const char *ceph_version(int *pmajor, int *pminor, int *ppatch)
{
  int major, minor, patch;
  const char *v = ceph_version_to_str();

  int n = sscanf(v, "%d.%d.%d", &major, &minor, &patch);
  if (pmajor)
    *pmajor = (n >= 1) ? major : 0;
  if (pminor)
    *pminor = (n >= 2) ? minor : 0;
  if (ppatch)
    *ppatch = (n >= 3) ? patch : 0;
  return VERSION;
}

extern "C" int ceph_create_with_context(struct ceph_mount_info **cmount, CephContext *cct)
{
  // Function-static variables are thread-safe in gcc and in the forthcoming C++ standard
  static int nonce_seed = 0;

  uint64_t nonce = (uint64_t)++nonce_seed * 1000000ull + (uint64_t)getpid();
  *cmount = new struct ceph_mount_info(nonce, cct);
  return 0;
}

extern "C" int ceph_create(struct ceph_mount_info **cmount, const char * const id)
{
  CephInitParameters iparams(CEPH_ENTITY_TYPE_CLIENT);
  if (id) {
    iparams.name.set(CEPH_ENTITY_TYPE_CLIENT, id);
  }

  CephContext *cct = common_preinit(iparams, CODE_ENVIRONMENT_LIBRARY, 0);
  cct->_conf->parse_env(); // environment variables coverride
  cct->_conf->apply_changes(NULL);
  return ceph_create_with_context(cmount, cct);
}

extern "C" void ceph_shutdown(struct ceph_mount_info *cmount)
{
  cmount->shutdown();
}

extern "C" int ceph_conf_read_file(struct ceph_mount_info *cmount, const char *path)
{
  return cmount->conf_read_file(path);
}

extern "C" int ceph_conf_parse_argv(struct ceph_mount_info *cmount, int argc,
				     const char **argv)
{
  return cmount->conf_parse_argv(argc, argv);
}

extern "C" int ceph_conf_set(struct ceph_mount_info *cmount, const char *option,
			     const char *value)
{
  return cmount->conf_set(option, value);
}

extern "C" int ceph_conf_get(struct ceph_mount_info *cmount, const char *option,
			     char *buf, size_t len)
{
  return cmount->conf_get(option, buf, len);
}

extern "C" int ceph_mount(struct ceph_mount_info *cmount, const char *root)
{
  std::string mount_root;
  if (root)
    mount_root = root;
  return cmount->mount(mount_root);
}

extern "C" int ceph_statfs(struct ceph_mount_info *cmount, const char *path,
			   struct statvfs *stbuf)
{
  return cmount->get_client()->statfs(path, stbuf);
}

extern "C" int ceph_get_local_osd(struct ceph_mount_info *cmount)
{
  return cmount->get_client()->get_local_osd();
}

extern "C" const char* ceph_getcwd(struct ceph_mount_info *cmount)
{
  return cmount->get_cwd();
}

extern "C" int ceph_chdir (struct ceph_mount_info *cmount, const char *s)
{
  return cmount->get_client()->chdir(s);
}

extern "C" int ceph_opendir(struct ceph_mount_info *cmount,
			    const char *name, struct ceph_dir_result **dirpp)
{
  return cmount->get_client()->opendir(name, (dir_result_t **)dirpp);
}

extern "C" int ceph_closedir(struct ceph_mount_info *cmount, struct ceph_dir_result *dirp)
{
  return cmount->get_client()->closedir((dir_result_t*)dirp);
}

extern "C" struct dirent * ceph_readdir(struct ceph_mount_info *cmount, struct ceph_dir_result *dirp)
{
  return cmount->get_client()->readdir((dir_result_t*)dirp);
}

extern "C" int ceph_readdir_r(struct ceph_mount_info *cmount, struct ceph_dir_result *dirp, struct dirent *de)
{
  return cmount->get_client()->readdir_r((dir_result_t*)dirp, de);
}

extern "C" int ceph_readdirplus_r(struct ceph_mount_info *cmount, struct ceph_dir_result *dirp,
				  struct dirent *de, struct stat *st, int *stmask)
{
  return cmount->get_client()->readdirplus_r((dir_result_t*)dirp, de, st, stmask);
}

extern "C" int ceph_getdents(struct ceph_mount_info *cmount, struct ceph_dir_result *dirp,
			     char *buf, int buflen)
{
  return cmount->get_client()->getdents((dir_result_t*)dirp, buf, buflen);
}

extern "C" int ceph_getdnames(struct ceph_mount_info *cmount, struct ceph_dir_result *dirp,
			      char *buf, int buflen)
{
  return cmount->get_client()->getdnames((dir_result_t*)dirp, buf, buflen);
}

extern "C" void ceph_rewinddir(struct ceph_mount_info *cmount, struct ceph_dir_result *dirp)
{
  cmount->get_client()->rewinddir((dir_result_t*)dirp);
}

extern "C" loff_t ceph_telldir(struct ceph_mount_info *cmount, struct ceph_dir_result *dirp)
{
  return cmount->get_client()->telldir((dir_result_t*)dirp);
}

extern "C" void ceph_seekdir(struct ceph_mount_info *cmount, struct ceph_dir_result *dirp, loff_t offset)
{
  cmount->get_client()->seekdir((dir_result_t*)dirp, offset);
}

extern "C" int ceph_link (struct ceph_mount_info *cmount, const char *existing,
			  const char *newname)
{
  return cmount->get_client()->link(existing, newname);
}

extern "C" int ceph_unlink(struct ceph_mount_info *cmount, const char *path)
{
  return cmount->get_client()->unlink(path);
}

extern "C" int ceph_rename(struct ceph_mount_info *cmount, const char *from,
			   const char *to)
{
  return cmount->get_client()->rename(from, to);
}

// dirs
extern "C" int ceph_mkdir(struct ceph_mount_info *cmount, const char *path, mode_t mode)
{
  return cmount->get_client()->mkdir(path, mode);
}

extern "C" int ceph_mkdirs(struct ceph_mount_info *cmount, const char *path, mode_t mode)
{
  return cmount->get_client()->mkdirs(path, mode);
}

extern "C" int ceph_rmdir(struct ceph_mount_info *cmount, const char *path)
{
  return cmount->get_client()->rmdir(path);
}

// symlinks
extern "C" int ceph_readlink(struct ceph_mount_info *cmount, const char *path,
			     char *buf, loff_t size)
{
  return cmount->get_client()->readlink(path, buf, size);
}

extern "C" int ceph_symlink(struct ceph_mount_info *cmount, const char *existing,
			    const char *newname)
{
  return cmount->get_client()->symlink(existing, newname);
}

// inode stuff
extern "C" int ceph_lstat(struct ceph_mount_info *cmount, const char *path,
			  struct stat *stbuf)
{
  return cmount->get_client()->lstat(path, stbuf);
}

extern "C" int ceph_setattr(struct ceph_mount_info *cmount, const char *relpath,
			    struct stat *attr, int mask)
{
  return cmount->get_client()->setattr(relpath, attr, mask);
}

// *xattr() calls supporting samba/vfs
extern "C" int ceph_getxattr(struct ceph_mount_info *cmount, const char *path, const char *name, void *value, size_t size)
{
  return cmount->get_client()->getxattr(path, name, value, size);
}

extern "C" int ceph_lgetxattr(struct ceph_mount_info *cmount, const char *path, const char *name, void *value, size_t size)
{
  return cmount->get_client()->lgetxattr(path, name, value, size);
}

extern "C" int ceph_listxattr(struct ceph_mount_info *cmount, const char *path, char *list, size_t size)
{
  return cmount->get_client()->listxattr(path, list, size);
}

extern "C" int ceph_llistxattr(struct ceph_mount_info *cmount, const char *path, char *list, size_t size)
{
  return cmount->get_client()->llistxattr(path, list, size);
}

extern "C" int ceph_removexattr(struct ceph_mount_info *cmount, const char *path, const char *name)
{
  return cmount->get_client()->removexattr(path, name);
}

extern "C" int ceph_lremovexattr(struct ceph_mount_info *cmount, const char *path, const char *name)
{
  return cmount->get_client()->lremovexattr(path, name);
}

extern "C" int ceph_setxattr(struct ceph_mount_info *cmount, const char *path, const char *name, const void *value, size_t size, int flags)
{
  return cmount->get_client()->setxattr(path, name, value, size, flags);
}

extern "C" int ceph_lsetxattr(struct ceph_mount_info *cmount, const char *path, const char *name, const void *value, size_t size, int flags)
{
  return cmount->get_client()->lsetxattr(path, name, value, size, flags);
}
/* end xattr support */

extern "C" int ceph_chmod(struct ceph_mount_info *cmount, const char *path, mode_t mode)
{
  return cmount->get_client()->chmod(path, mode);
}
extern "C" int ceph_chown(struct ceph_mount_info *cmount, const char *path,
			  uid_t uid, gid_t gid)
{
  return cmount->get_client()->chown(path, uid, gid);
}

extern "C" int ceph_utime(struct ceph_mount_info *cmount, const char *path,
			  struct utimbuf *buf)
{
  return cmount->get_client()->utime(path, buf);
}

extern "C" int ceph_truncate(struct ceph_mount_info *cmount, const char *path,
			     loff_t size)
{
  return cmount->get_client()->truncate(path, size);
}

// file ops
extern "C" int ceph_mknod(struct ceph_mount_info *cmount, const char *path,
			  mode_t mode, dev_t rdev)
{
  return cmount->get_client()->mknod(path, mode, rdev);
}

extern "C" int ceph_open(struct ceph_mount_info *cmount, const char *path,
			 int flags, mode_t mode)
{
  return cmount->get_client()->open(path, flags, mode);
}

extern "C" int ceph_close(struct ceph_mount_info *cmount, int fd)
{
  return cmount->get_client()->close(fd);
}

extern "C" loff_t ceph_lseek(struct ceph_mount_info *cmount, int fd,
			     loff_t offset, int whence)
{
  return cmount->get_client()->lseek(fd, offset, whence);
}

extern "C" int ceph_read(struct ceph_mount_info *cmount, int fd, char *buf,
			 loff_t size, loff_t offset)
{
  return cmount->get_client()->read(fd, buf, size, offset);
}

extern "C" int ceph_write(struct ceph_mount_info *cmount, int fd, const char *buf,
			  loff_t size, loff_t offset)
{
  return cmount->get_client()->write(fd, buf, size, offset);
}

extern "C" int ceph_ftruncate(struct ceph_mount_info *cmount, int fd, loff_t size)
{
  return cmount->get_client()->ftruncate(fd, size);
}

extern "C" int ceph_fsync(struct ceph_mount_info *cmount, int fd, int syncdataonly)
{
  return cmount->get_client()->fsync(fd, syncdataonly);
}

extern "C" int ceph_fstat(struct ceph_mount_info *cmount, int fd, struct stat *stbuf)
{
  return cmount->get_client()->fstat(fd, stbuf);
}

extern "C" int ceph_sync_fs(struct ceph_mount_info *cmount)
{
  return cmount->get_client()->sync_fs();
}


extern "C" int ceph_get_file_stripe_unit(struct ceph_mount_info *cmount, int fh)
{
  struct ceph_file_layout l;
  int r = cmount->get_client()->describe_layout(fh, &l);
  if (r < 0)
    return r;
  return l.fl_stripe_unit;
}

extern "C" int ceph_get_file_pool(struct ceph_mount_info *cmount, int fh)
{
  struct ceph_file_layout l;
  int r = cmount->get_client()->describe_layout(fh, &l);
  if (r < 0)
    return r;
  return l.fl_pg_pool;
}

extern "C" int ceph_get_file_replication(struct ceph_mount_info *cmount, int fh)
{
  struct ceph_file_layout l;
  int r = cmount->get_client()->describe_layout(fh, &l);
  if (r < 0)
    return r;
  int rep = cmount->get_client()->get_pool_replication(l.fl_pg_pool);
  return rep;
}

extern "C" int ceph_set_default_file_stripe_unit(struct ceph_mount_info *cmount,
						 int stripe)
{
  cmount->get_client()->set_default_file_stripe_unit(stripe);
  return 0;
}

extern "C" int ceph_set_default_file_stripe_count(struct ceph_mount_info *cmount,
						  int count)
{
  cmount->get_client()->set_default_file_stripe_unit(count);
  return 0;
}

extern "C" int ceph_set_default_object_size(struct ceph_mount_info *cmount, int size)
{
  cmount->get_client()->set_default_object_size(size);
  return 0;
}

extern "C" int ceph_set_default_file_replication(struct ceph_mount_info *cmount,
						 int replication)
{
  cmount->get_client()->set_default_file_replication(replication);
  return 0;
}

extern "C" int ceph_set_default_preferred_pg(struct ceph_mount_info *cmount, int osd)
{
  // this option no longer exists
  return -EOPNOTSUPP;
}

extern "C" int ceph_get_file_stripe_address(struct ceph_mount_info *cmount, int fh,
					    loff_t offset, struct sockaddr_storage *addr, int naddr)
{
  vector<entity_addr_t> address;
  unsigned i;
  int r;

  if (naddr < 0)
    return -EINVAL;

  r = cmount->get_client()->get_file_stripe_address(fh, offset, address);
  if (r < 0)
    return r;

  for (i = 0; i < (unsigned)naddr && i < address.size(); i++)
    memcpy(&addr[i], &address[i].ss_addr(), sizeof(*addr));

  /* naddr == 0: drop through and return actual size */
  if (naddr && (address.size() > (unsigned)naddr))
    return -ERANGE;

  return address.size();
}

extern "C" int ceph_localize_reads(struct ceph_mount_info *cmount, int val)
{
  if (!val)
    cmount->get_client()->clear_filer_flags(CEPH_OSD_FLAG_LOCALIZE_READS);
  else
    cmount->get_client()->set_filer_flags(CEPH_OSD_FLAG_LOCALIZE_READS);
  return 0;
}

extern "C" CephContext *ceph_get_mount_context(struct ceph_mount_info *cmount)
{
  return cmount->get_ceph_context();
}

/* Low-level exports */

extern "C" int ceph_ll_lookup(struct ceph_mount_info *cmount,
			      vinodeno_t parent, const char *name,
			      struct stat *attr, int uid, int gid)
{
  return (cmount->get_client()->ll_lookup(parent, name, attr, uid, gid));
}

int ceph_ll_forget(struct ceph_mount_info *cmount, vinodeno_t vino, int count)
{
  return (cmount->get_client()->ll_forget(vino, count));
}

extern "C" int ceph_ll_walk(struct ceph_mount_info *cmount, const char *name,
			    struct stat *attr)
{
  return(cmount->get_client()->ll_walk(name, attr));
}
  
extern "C" int ceph_ll_getattr(struct ceph_mount_info *cmount,
			       vinodeno_t vi, struct stat *attr,
			       int uid, int gid)
{
  return (cmount->get_client()->ll_getattr(vi, attr, uid, gid));
}

extern "C" int ceph_ll_setattr(struct ceph_mount_info *cmount,
			       vinodeno_t vi, struct stat *st,
			       int mask, int uid, int gid)
{
  return (cmount->get_client()->ll_setattr(vi, st, mask, uid, gid));
}

extern "C" int ceph_ll_open(struct ceph_mount_info *cmount, vinodeno_t vi,
			    int flags, Fh **filehandle, int uid, int gid)
{
  return (cmount->get_client()->ll_open(vi, flags, filehandle, uid, gid));
}

extern "C" int ceph_ll_read(struct ceph_mount_info *cmount, Fh* filehandle,
			    int64_t off, uint64_t len, char* buf)
{
  bufferlist bl;
  int r = 0;

  r = cmount->get_client()->ll_read(filehandle, off, len, &bl);
  if (r >= 0)
    {
      bl.copy(0, bl.length(), buf);
      r = bl.length();
    }
  return r;
}

extern "C" int ceph_ll_read_block(struct ceph_mount_info *cmount,
				  vinodeno_t vino, uint64_t blockid,
				  char* buf, uint64_t offset,
				  uint64_t length,
				  struct ceph_file_layout* layout)
{

  return (cmount->get_client()->ll_read_block(vino, blockid, buf,
					      offset, length, layout));
}

extern "C" int ceph_ll_write_block(struct ceph_mount_info *cmount,
				   vinodeno_t vino, uint64_t blockid,
				   char* buf, uint64_t offset,
				   uint64_t length,
				   struct ceph_file_layout* layout,
				   uint64_t snapseq, uint32_t sync)
{
  return (cmount->get_client()->ll_write_block(vino, blockid, buf, offset,
					       length, layout, snapseq, sync));
}

extern "C" int ceph_ll_commit_blocks(struct ceph_mount_info *cmount,
				     vinodeno_t vino, uint64_t offset,
				     uint64_t range)
{
  return (cmount->get_client()->ll_commit_blocks(vino, offset, range));
}

extern "C" int ceph_ll_fsync(struct ceph_mount_info *cmount,
			     Fh *fh, int syncdataonly)
{
  return (cmount->get_client()->ll_fsync(fh, syncdataonly));
}

extern "C" loff_t ceph_ll_lseek(struct ceph_mount_info *cmount,
				Fh* filehandle, loff_t offset, int whence)
{
  return (cmount->get_client()->ll_lseek(filehandle, offset, whence));
}

extern "C" int ceph_ll_write(struct ceph_mount_info *cmount,
			     Fh* filehandle, int64_t off, uint64_t len,
			     const char *data)
{
  return (cmount->get_client()->ll_write(filehandle, off, len, data));
}

extern "C" int ceph_ll_close(struct ceph_mount_info *cmount, Fh* filehandle)
{
  return (cmount->get_client()->ll_release(filehandle));
}

extern "C" int ceph_ll_create(struct ceph_mount_info *cmount,
			      vinodeno_t parent, const char* name,
			      mode_t mode, int flags,
			      Fh **filehandle,
			      struct stat *attr, int uid,
			      int gid)
{
  return (cmount->get_client()->ll_create(parent, name, mode, flags, attr,
					filehandle, uid, gid));
}

extern "C" int ceph_ll_mkdir(struct ceph_mount_info *cmount,
			     vinodeno_t parent, const char *name,
			     mode_t mode, struct stat *attr,
			     int uid, int gid)
{
  return (cmount->get_client()->ll_mkdir(parent, name, mode, attr, uid, gid));
}

extern "C" int ceph_ll_link(struct ceph_mount_info *cmount,
			    vinodeno_t obj, vinodeno_t newparrent,
			    const char *name, struct stat *attr,
			    int uid, int gid)
{
  return (cmount->get_client()->ll_link(obj, newparrent, name, attr, uid,
				      gid));
}

extern "C" int ceph_ll_truncate(struct ceph_mount_info *cmount,
				vinodeno_t obj, uint64_t length, int uid,
				int gid)
{
  struct stat st;
  st.st_size=length;

  return(cmount->get_client()->ll_setattr(obj, &st, CEPH_SETATTR_SIZE, uid,
					gid));
}

extern "C" int ceph_ll_opendir(struct ceph_mount_info *cmount,
			       vinodeno_t vino,
			       struct ceph_dir_result** dirpp,
			       int uid, int gid)
{
  return (cmount->get_client()->ll_opendir(vino, (dir_result_t **)dirpp,
					   uid, gid));
}

extern "C" int ceph_ll_releasedir(struct ceph_mount_info *cmount,
				  ceph_dir_result* dir)
{
  (void) cmount->get_client()->ll_releasedir((dir_result_t*) dir);
  return (0);
}

extern "C" int ceph_ll_rename(struct ceph_mount_info *cmount,
			      vinodeno_t parent, const char *name,
			      vinodeno_t newparent, const char *newname,
			      int uid, int gid)
{
  return (cmount->get_client()->ll_rename(parent, name, newparent, newname,
					uid, gid)); 
}

extern "C" int ceph_ll_unlink(struct ceph_mount_info *cmount,
			      vinodeno_t vino, const char *name,
			      int uid, int gid) 
{
  return (cmount->get_client()->ll_unlink(vino, name, uid, gid));
}

extern "C" int ceph_ll_statfs(struct ceph_mount_info *cmount,
			      vinodeno_t vino, struct statvfs *stbuf)
{
  return (cmount->get_client()->ll_statfs(vino, stbuf));
}

extern "C" int ceph_ll_readlink(struct ceph_mount_info *cmount,
				vinodeno_t vino, char **value, int uid,
				int gid)
{
  return (cmount->get_client()->ll_readlink(vino, (const char**) value,
					  uid, gid));
}

extern "C" int ceph_ll_symlink(struct ceph_mount_info *cmount,
			       vinodeno_t parent, const char *name,
			       const char *value, struct stat *attr,
			       int uid, int gid)
{
  return (cmount->get_client()->ll_symlink(parent, name, value, attr, uid,
					 gid));
}

extern "C" int ceph_ll_rmdir(struct ceph_mount_info *cmount,
			     vinodeno_t vino, const char *name,
			     int uid, int gid)
{
  return (cmount->get_client()->ll_rmdir(vino, name, uid, gid));
}

extern "C" int ceph_ll_getxattr(struct ceph_mount_info *cmount,
				vinodeno_t vino, const char *name, void *value,
				size_t size, int uid, int gid)
{
  return (cmount->get_client()->ll_getxattr(vino, name, value, size, uid,
					  gid));
}

extern "C" int ceph_ll_setxattr(struct ceph_mount_info *cmount,
				vinodeno_t vino, const char *name,
				const void *value, size_t size,
				int flags, int uid, int gid)
{
  return (cmount->get_client()->ll_setxattr(vino, name, value, size, flags,
					  uid, gid));
}

extern "C" int ceph_ll_removexattr(struct ceph_mount_info *cmount,
				   vinodeno_t vino, const char *name,
				   int uid, int gid)
{
  return (cmount->get_client()->ll_removexattr(vino, name, uid, gid));
}

extern "C" uint32_t ceph_ll_stripe_unit(struct ceph_mount_info *cmount,
					vinodeno_t vino)
{
  return (cmount->get_client()->ll_stripe_unit(vino));
}

extern "C" uint32_t ceph_ll_file_layout(struct ceph_mount_info *cmount,
					vinodeno_t vino,
					struct ceph_file_layout *layout)
{
  return (cmount->get_client()->ll_file_layout(vino, layout));
}

uint64_t ceph_ll_snap_seq(struct ceph_mount_info *cmount, vinodeno_t vino)
{
  return (cmount->get_client()->ll_snap_seq(vino));
}

extern "C" int ceph_ll_get_stripe_osd(struct ceph_mount_info *cmount,
				      vinodeno_t vino, uint64_t blockno,
				      struct ceph_file_layout* layout)
{
  return (cmount->get_client()->ll_get_stripe_osd(vino, blockno, layout));
}

extern "C" int ceph_ll_num_osds(struct ceph_mount_info *cmount)
{
  return (cmount->get_client()->ll_num_osds());
}

extern "C" int ceph_ll_osdaddr(struct ceph_mount_info *cmount,
			       int osd, uint32_t *addr)
{
  return (cmount->get_client()->ll_osdaddr(osd, addr));
}

extern "C" uint64_t ceph_ll_get_internal_offset(struct ceph_mount_info *cmount,
						vinodeno_t vino,
						uint64_t blockno)
{
  return (cmount->get_client()->ll_get_internal_offset(vino, blockno));
}

extern "C" int ceph_ll_connectable_x(struct ceph_mount_info *cmount,
				     vinodeno_t vino, uint64_t* parent_ino,
				     uint32_t* parent_hash)
{
  return (cmount->get_client()->ll_connectable_x(vino, parent_ino,
						 parent_hash));
}

extern "C" int ceph_ll_connectable_m(struct ceph_mount_info *cmount,
				     vinodeno_t* vino, uint64_t parent_ino,
				     uint32_t parent_hash)
{
  return (cmount->get_client()->ll_connectable_m(vino, parent_ino,
						 parent_hash));
}
