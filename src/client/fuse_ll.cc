// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#define FUSE_USE_VERSION 26

#include <fuse/fuse_lowlevel.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>

// ceph
#include "common/errno.h"
#include "common/safe_io.h"
#include "include/types.h"
#include "Client.h"
#include "common/config.h"
#include "include/assert.h"

#include "fuse_ll.h"

#define FINO_INO(x) ((x) & ((1ull<<48)-1ull))
#define FINO_STAG(x) ((x) >> 48)
#define MAKE_FINO(i,s) ((i) | ((s) << 48))

#define MINORBITS	20
#define MINORMASK	((1U << MINORBITS) - 1)

#define MAJOR(dev)	((unsigned int) ((dev) >> MINORBITS))
#define MINOR(dev)	((unsigned int) ((dev) & MINORMASK))
#define MKDEV(ma,mi)	(((ma) << MINORBITS) | (mi))

static uint32_t new_encode_dev(dev_t dev)
{
	unsigned major = MAJOR(dev);
	unsigned minor = MINOR(dev);
	return (minor & 0xff) | (major << 8) | ((minor & ~0xff) << 12);
}

static dev_t new_decode_dev(uint32_t dev)
{
	unsigned major = (dev & 0xfff00) >> 8;
	unsigned minor = (dev & 0xff) | ((dev >> 12) & 0xfff00);
	return MKDEV(major, minor);
}

class CephFuse::Handle {
public:
  Handle(Client *c, int fd);

  int init(int argc, const char *argv[]);
  int loop();
  void finalize();

  uint64_t fino_snap(uint64_t fino);
  vinodeno_t fino_vino(inodeno_t fino);
  uint64_t make_fake_ino(inodeno_t ino, snapid_t snapid);
  Inode * iget(inodeno_t fino);
  void iput(Inode *in);

  int fd_on_success;
  Client *client;

  struct fuse_chan *ch;
  struct fuse_session *se;
  char *mountpoint;

  Mutex stag_lock;
  int last_stag;

  hash_map<uint64_t,int> snap_stag_map;
  hash_map<int,uint64_t> stag_snap_map;

};

static void fuse_ll_lookup(fuse_req_t req, fuse_ino_t parent, const char *name)
{
  CephFuse::Handle *cfuse = (CephFuse::Handle *)fuse_req_userdata(req);
  const struct fuse_ctx *ctx = fuse_req_ctx(req);
  struct fuse_entry_param fe;
  Inode *i2, *i1 = cfuse->iget(parent); // see below
  int r;

  memset(&fe, 0, sizeof(fe));
  r = cfuse->client->ll_lookup(i1, name, &fe.attr, &i2, ctx->uid, ctx->gid);
  if (r >= 0) {
    fe.ino = cfuse->make_fake_ino(fe.attr.st_ino, fe.attr.st_dev);
    fe.attr.st_rdev = new_encode_dev(fe.attr.st_rdev);
    fuse_reply_entry(req, &fe);
  } else {
    fuse_reply_err(req, -r);
  }

  // XXX NB, we dont iput(i2) because FUSE will do so in a matching
  // fuse_ll_forget()
  cfuse->iput(i1);
}

static void fuse_ll_forget(fuse_req_t req, fuse_ino_t ino,
			   long unsigned nlookup)
{
  CephFuse::Handle *cfuse = (CephFuse::Handle *)fuse_req_userdata(req);
  cfuse->client->ll_forget(cfuse->iget(ino), nlookup+1);
  fuse_reply_none(req);
}

static void fuse_ll_getattr(fuse_req_t req, fuse_ino_t ino,
			    struct fuse_file_info *fi)
{
  CephFuse::Handle *cfuse = (CephFuse::Handle *)fuse_req_userdata(req);
  const struct fuse_ctx *ctx = fuse_req_ctx(req);
  Inode *in = cfuse->iget(ino);
  struct stat stbuf;
  
  (void) fi; // XXX

  if (cfuse->client->ll_getattr(in, &stbuf, ctx->uid, ctx->gid)
      == 0) {
    stbuf.st_ino = cfuse->make_fake_ino(stbuf.st_ino, stbuf.st_dev);
    stbuf.st_rdev = new_encode_dev(stbuf.st_rdev);
    fuse_reply_attr(req, &stbuf, 0);
  } else
    fuse_reply_err(req, ENOENT);

  cfuse->iput(in); // iput required
}

static void fuse_ll_setattr(fuse_req_t req, fuse_ino_t ino, struct stat *attr,
			    int to_set, struct fuse_file_info *fi)
{
  CephFuse::Handle *cfuse = (CephFuse::Handle *)fuse_req_userdata(req);
  const struct fuse_ctx *ctx = fuse_req_ctx(req);
  Inode *in = cfuse->iget(ino);

  int mask = 0;
  if (to_set & FUSE_SET_ATTR_MODE) mask |= CEPH_SETATTR_MODE;
  if (to_set & FUSE_SET_ATTR_UID) mask |= CEPH_SETATTR_UID;
  if (to_set & FUSE_SET_ATTR_GID) mask |= CEPH_SETATTR_GID;
  if (to_set & FUSE_SET_ATTR_MTIME) mask |= CEPH_SETATTR_MTIME;
  if (to_set & FUSE_SET_ATTR_ATIME) mask |= CEPH_SETATTR_ATIME;
  if (to_set & FUSE_SET_ATTR_SIZE) mask |= CEPH_SETATTR_SIZE;

  int r = cfuse->client->ll_setattr(in, attr, mask, ctx->uid, ctx->gid);
  if (r == 0)
    fuse_reply_attr(req, attr, 0);
  else
    fuse_reply_err(req, -r);

  cfuse->iput(in); // iput required
}

// XATTRS

static void fuse_ll_setxattr(fuse_req_t req, fuse_ino_t ino, const char *name,
			     const char *value, size_t size, int flags)
{
  CephFuse::Handle *cfuse = (CephFuse::Handle *)fuse_req_userdata(req);
  const struct fuse_ctx *ctx = fuse_req_ctx(req);
  Inode *in = cfuse->iget(ino);

  int r = cfuse->client->ll_setxattr(in, name, value, size, flags, ctx->uid,
				     ctx->gid);
  fuse_reply_err(req, -r);

  cfuse->iput(in); // iput required
}

static void fuse_ll_listxattr(fuse_req_t req, fuse_ino_t ino, size_t size)
{
  CephFuse::Handle *cfuse = (CephFuse::Handle *)fuse_req_userdata(req);
  const struct fuse_ctx *ctx = fuse_req_ctx(req);
  Inode *in = cfuse->iget(ino);
  char buf[size];

  int r = cfuse->client->ll_listxattr(in, buf, size, ctx->uid, ctx->gid);
  if (size == 0 && r >= 0)
    fuse_reply_xattr(req, r);
  else if (r >= 0) 
    fuse_reply_buf(req, buf, r);
  else
    fuse_reply_err(req, -r);

  cfuse->iput(in); // iput required
}

static void fuse_ll_getxattr(fuse_req_t req, fuse_ino_t ino, const char *name,
			     size_t size)
{
  CephFuse::Handle *cfuse = (CephFuse::Handle *)fuse_req_userdata(req);
  const struct fuse_ctx *ctx = fuse_req_ctx(req);
  Inode *in = cfuse->iget(ino);
  char buf[size];

  int r = cfuse->client->ll_getxattr(in, name, buf, size, ctx->uid, ctx->gid);
  if (size == 0 && r >= 0)
    fuse_reply_xattr(req, r);
  else if (r >= 0)
    fuse_reply_buf(req, buf, r);
  else
    fuse_reply_err(req, -r);

  cfuse->iput(in); // iput required
}

static void fuse_ll_removexattr(fuse_req_t req, fuse_ino_t ino,
				const char *name)
{
  CephFuse::Handle *cfuse = (CephFuse::Handle *)fuse_req_userdata(req);
  const struct fuse_ctx *ctx = fuse_req_ctx(req);
  Inode *in = cfuse->iget(ino);

  int r = cfuse->client->ll_removexattr(in, name, ctx->uid,
					ctx->gid);
  fuse_reply_err(req, -r);

  cfuse->iput(in); // iput required
}

static void fuse_ll_opendir(fuse_req_t req, fuse_ino_t ino,
			    struct fuse_file_info *fi)
{
  CephFuse::Handle *cfuse = (CephFuse::Handle *)fuse_req_userdata(req);
  const struct fuse_ctx *ctx = fuse_req_ctx(req);
  Inode *in = cfuse->iget(ino);
  void *dirp;

  int r = cfuse->client->ll_opendir(in, (dir_result_t **) &dirp, ctx->uid,
				    ctx->gid);
  if (r >= 0) {
    fi->fh = (long)dirp;
    fuse_reply_open(req, fi);
  } else {
    fuse_reply_err(req, -r);
  }

  cfuse->iput(in); // iput required
}

static void fuse_ll_readlink(fuse_req_t req, fuse_ino_t ino)
{
  CephFuse::Handle *cfuse = (CephFuse::Handle *)fuse_req_userdata(req);
  const struct fuse_ctx *ctx = fuse_req_ctx(req);
  Inode *in = cfuse->iget(ino);
  const char *value;

  int r = cfuse->client->ll_readlink(in, &value, ctx->uid, ctx->gid);
  if (r == 0) 
    fuse_reply_readlink(req, value);
  else
    fuse_reply_err(req, -r);

  cfuse->iput(in); // iput required
}

static void fuse_ll_mknod(fuse_req_t req, fuse_ino_t parent, const char *name,
			  mode_t mode, dev_t rdev)
{
  CephFuse::Handle *cfuse = (CephFuse::Handle *)fuse_req_userdata(req);
  const struct fuse_ctx *ctx = fuse_req_ctx(req);
  Inode *i2, *i1 = cfuse->iget(parent);
  struct fuse_entry_param fe;

  memset(&fe, 0, sizeof(fe));

  int r = cfuse->client->ll_mknod(i1, name, mode, new_decode_dev(rdev),
				  &fe.attr, &i2, ctx->uid, ctx->gid);
  if (r == 0) {
    fe.ino = cfuse->make_fake_ino(fe.attr.st_ino, fe.attr.st_dev);
    fe.attr.st_rdev = new_encode_dev(fe.attr.st_rdev);
    fuse_reply_entry(req, &fe);
  } else {
    fuse_reply_err(req, -r);
  }

  // XXX NB, we dont iput(i2) because FUSE will do so in a matching
  // fuse_ll_forget()
  cfuse->iput(i1); // iput required
}

static void fuse_ll_mkdir(fuse_req_t req, fuse_ino_t parent, const char *name,
			  mode_t mode)
{
  CephFuse::Handle *cfuse = (CephFuse::Handle *)fuse_req_userdata(req);
  const struct fuse_ctx *ctx = fuse_req_ctx(req);
  Inode *i2, *i1 = cfuse->iget(parent);
  struct fuse_entry_param fe;

  memset(&fe, 0, sizeof(fe));

  int r = cfuse->client->ll_mkdir(i1, name, mode, &fe.attr, &i2, ctx->uid,
				  ctx->gid);
  if (r == 0) {
    fe.ino = cfuse->make_fake_ino(fe.attr.st_ino, fe.attr.st_dev);
    fe.attr.st_rdev = new_encode_dev(fe.attr.st_rdev);
    fuse_reply_entry(req, &fe);
  } else {
    fuse_reply_err(req, -r);
  }

  // XXX NB, we dont iput(i2) because FUSE will do so in a matching
  // fuse_ll_forget()
  cfuse->iput(i1); // iput required
}

static void fuse_ll_unlink(fuse_req_t req, fuse_ino_t parent, const char *name)
{
  CephFuse::Handle *cfuse = (CephFuse::Handle *)fuse_req_userdata(req);
  const struct fuse_ctx *ctx = fuse_req_ctx(req);
  Inode *in = cfuse->iget(parent);

  int r = cfuse->client->ll_unlink(in, name, ctx->uid, ctx->gid);
  fuse_reply_err(req, -r);

  cfuse->iput(in); // iput required
}

static void fuse_ll_rmdir(fuse_req_t req, fuse_ino_t parent, const char *name)
{
  CephFuse::Handle *cfuse = (CephFuse::Handle *)fuse_req_userdata(req);
  const struct fuse_ctx *ctx = fuse_req_ctx(req);
  Inode *in = cfuse->iget(parent);

  int r = cfuse->client->ll_rmdir(in, name, ctx->uid, ctx->gid);
  fuse_reply_err(req, -r);

  cfuse->iput(in); // iput required
}

static void fuse_ll_symlink(fuse_req_t req, const char *existing,
			    fuse_ino_t parent, const char *name)
{
  CephFuse::Handle *cfuse = (CephFuse::Handle *)fuse_req_userdata(req);
  const struct fuse_ctx *ctx = fuse_req_ctx(req);
  Inode *i2, *i1 = cfuse->iget(parent);
  struct fuse_entry_param fe;

  memset(&fe, 0, sizeof(fe));

  int r = cfuse->client->ll_symlink(i1, name, existing, &fe.attr, &i2, ctx->uid,
				    ctx->gid);
  if (r == 0) {
    fe.ino = cfuse->make_fake_ino(fe.attr.st_ino, fe.attr.st_dev);
    fe.attr.st_rdev = new_encode_dev(fe.attr.st_rdev);
    fuse_reply_entry(req, &fe);
  } else {
    fuse_reply_err(req, -r);
  }

  // XXX NB, we dont iput(i2) because FUSE will do so in a matching
  // fuse_ll_forget()
  cfuse->iput(i1); // iput required
}

static void fuse_ll_rename(fuse_req_t req, fuse_ino_t parent, const char *name,
			   fuse_ino_t newparent, const char *newname)
{
  CephFuse::Handle *cfuse = (CephFuse::Handle *)fuse_req_userdata(req);
  const struct fuse_ctx *ctx = fuse_req_ctx(req);
  Inode *in = cfuse->iget(parent);
  Inode *nin = cfuse->iget(newparent);

  int r = cfuse->client->ll_rename(in, name, nin, newname, ctx->uid, ctx->gid);
  fuse_reply_err(req, -r);

  cfuse->iput(in); // iputs required
  cfuse->iput(nin);
}

static void fuse_ll_link(fuse_req_t req, fuse_ino_t ino, fuse_ino_t newparent,
			 const char *newname)
{
  CephFuse::Handle *cfuse = (CephFuse::Handle *)fuse_req_userdata(req);
  const struct fuse_ctx *ctx = fuse_req_ctx(req);
  Inode *in = cfuse->iget(ino);
  Inode *nin = cfuse->iget(newparent);
  struct fuse_entry_param fe;

  memset(&fe, 0, sizeof(fe));
  
  int r = cfuse->client->ll_link(in, nin, newname, &fe.attr, ctx->uid,
				 ctx->gid);
  if (r == 0) {
    fe.ino = cfuse->make_fake_ino(fe.attr.st_ino, fe.attr.st_dev);
    fe.attr.st_rdev = new_encode_dev(fe.attr.st_rdev);
    fuse_reply_entry(req, &fe);
  } else {
    fuse_reply_err(req, -r);
  }

  cfuse->iput(in); // iputs required
  cfuse->iput(nin);
}

static void fuse_ll_open(fuse_req_t req, fuse_ino_t ino,
			 struct fuse_file_info *fi)
{
  CephFuse::Handle *cfuse = (CephFuse::Handle *)fuse_req_userdata(req);
  const struct fuse_ctx *ctx = fuse_req_ctx(req);
  Inode *in = cfuse->iget(ino);
  Fh *fh = NULL;

  int r = cfuse->client->ll_open(in, fi->flags, &fh, ctx->uid, ctx->gid);
  if (r == 0) {
    fi->fh = (long)fh;
#if FUSE_VERSION >= FUSE_MAKE_VERSION(2, 8)
    if (g_conf->fuse_use_invalidate_cb)
      fi->keep_cache = 1;
#endif
    fuse_reply_open(req, fi);
  } else {
    fuse_reply_err(req, -r);
  }

  cfuse->iput(in); // iput required
}

static void fuse_ll_read(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off,
			 struct fuse_file_info *fi)
{
  CephFuse::Handle *cfuse = (CephFuse::Handle *)fuse_req_userdata(req);
  Fh *fh = (Fh*)fi->fh;
  bufferlist bl;
  int r = cfuse->client->ll_read(fh, off, size, &bl);
  if (r >= 0)
    fuse_reply_buf(req, bl.c_str(), bl.length());
  else
    fuse_reply_err(req, -r);
}

static void fuse_ll_write(fuse_req_t req, fuse_ino_t ino, const char *buf,
			   size_t size, off_t off, struct fuse_file_info *fi)
{
  CephFuse::Handle *cfuse = (CephFuse::Handle *)fuse_req_userdata(req);
  Fh *fh = (Fh*)fi->fh;
  int r = cfuse->client->ll_write(fh, off, size, buf);
  if (r >= 0)
    fuse_reply_write(req, r);
  else
    fuse_reply_err(req, -r);
}

static void fuse_ll_flush(fuse_req_t req, fuse_ino_t ino,
			  struct fuse_file_info *fi)
{
  // NOOP
  fuse_reply_err(req, 0);
}

static void fuse_ll_release(fuse_req_t req, fuse_ino_t ino,
			    struct fuse_file_info *fi)
{
  CephFuse::Handle *cfuse = (CephFuse::Handle *)fuse_req_userdata(req);
  Fh *fh = (Fh*)fi->fh;
  int r = cfuse->client->ll_release(fh);
  fuse_reply_err(req, -r);
}

static void fuse_ll_fsync(fuse_req_t req, fuse_ino_t ino, int datasync,
			  struct fuse_file_info *fi)
{
  CephFuse::Handle *cfuse = (CephFuse::Handle *)fuse_req_userdata(req);
  Fh *fh = (Fh*)fi->fh;
  int r = cfuse->client->ll_fsync(fh, datasync);
  fuse_reply_err(req, -r);
}

struct readdir_context {
  fuse_req_t req;
  char *buf;
  size_t size;
  size_t pos; /* in buf */
  uint64_t snap;
};

/*
 * return 0 on success, -1 if out of space
 */
static int fuse_ll_add_dirent(void *p, struct dirent *de, struct stat *st,
			      int stmask, off_t next_off)
{
  struct readdir_context *c = (struct readdir_context *)p;
  CephFuse::Handle *cfuse = (CephFuse::Handle *)fuse_req_userdata(c->req);

  st->st_ino = cfuse->make_fake_ino(de->d_ino, c->snap);
  st->st_mode = DTTOIF(de->d_type);
  st->st_rdev = new_encode_dev(st->st_rdev);

  size_t room = c->size - c->pos;
  size_t entrysize = fuse_add_direntry(c->req, c->buf + c->pos, room,
				       de->d_name, st, next_off);
  if (entrysize > room)
    return -ENOSPC;

  /* success */
  c->pos += entrysize;
  return 0;
}

static void fuse_ll_readdir(fuse_req_t req, fuse_ino_t ino, size_t size,
			    off_t off, struct fuse_file_info *fi)
{
  CephFuse::Handle *cfuse = (CephFuse::Handle *)fuse_req_userdata(req);

  dir_result_t *dirp = (dir_result_t*)fi->fh;
  cfuse->client->seekdir(dirp, off);

  struct readdir_context rc;
  rc.req = req;
  rc.buf = new char[size];
  rc.size = size;
  rc.pos = 0;
  rc.snap = cfuse->fino_snap(ino);

  int r = cfuse->client->readdir_r_cb(dirp, fuse_ll_add_dirent, &rc);
  if (r == 0 || r == -ENOSPC)  /* ignore ENOSPC from our callback */
    fuse_reply_buf(req, rc.buf, rc.pos);
  else
    fuse_reply_err(req, -r);
  delete[] rc.buf;
}

static void fuse_ll_releasedir(fuse_req_t req, fuse_ino_t ino,
			       struct fuse_file_info *fi)
{
  CephFuse::Handle *cfuse = (CephFuse::Handle *)fuse_req_userdata(req);
  dir_result_t *dirp = (dir_result_t*)fi->fh;
  cfuse->client->ll_releasedir(dirp);
  fuse_reply_err(req, 0);
}

static void fuse_ll_create(fuse_req_t req, fuse_ino_t parent, const char *name,
			   mode_t mode, struct fuse_file_info *fi)
{
  CephFuse::Handle *cfuse = (CephFuse::Handle *)fuse_req_userdata(req);
  const struct fuse_ctx *ctx = fuse_req_ctx(req);
  Inode *i1 = cfuse->iget(parent);
  struct fuse_entry_param fe;
  Fh *fh = NULL;

  memset(&fe, 0, sizeof(fe));

  int r = cfuse->client->ll_create(i1, name, mode, fi->flags, &fe.attr, NULL,
				   &fh, ctx->uid, ctx->gid);
  if (r == 0) {
    fi->fh = (long)fh;
    fe.ino = cfuse->make_fake_ino(fe.attr.st_ino, fe.attr.st_dev);
    fuse_reply_create(req, &fe, fi);
  } else
    fuse_reply_err(req, -r);
  // XXX NB, we dont iput(i2) because FUSE will do so in a matching
  // fuse_ll_forget()
  cfuse->iput(i1); // iput required
}

static void fuse_ll_statfs(fuse_req_t req, fuse_ino_t ino)
{
  struct statvfs stbuf;
  CephFuse::Handle *cfuse = (CephFuse::Handle *)fuse_req_userdata(req);
  Inode *in = cfuse->iget(ino);

  int r = cfuse->client->ll_statfs(in, &stbuf);
  if (r == 0)
    fuse_reply_statfs(req, &stbuf);
  else
    fuse_reply_err(req, -r);

  cfuse->iput(in); // iput required
}

static int getgroups_cb(void *handle, uid_t uid, gid_t **sgids)
{
#ifdef HAVE_FUSE_GETGROUPS
  assert(sgids);
  int c = fuse_getgroups(0, NULL);
  if (c < 0) {
    return c;
  }
  if (c == 0) {
    return 0;
  }

  *sgids = malloc(c*sizeof(**sgids));
  if (!*sgids) {
    return -ENOMEM;
  }
  c = fuse_getgroups(c, *sgids);
  if (c < 0) {
    free(*sgids);
    return c;
  }
  return c;
#endif
  return 0;
}

static void invalidate_cb(void *handle, vinodeno_t vino, int64_t off,
			  int64_t len)
{
  CephFuse::Handle *cfuse = (CephFuse::Handle *)handle;
  fuse_ino_t fino = cfuse->make_fake_ino(vino.ino, vino.snapid);
#if FUSE_VERSION >= FUSE_MAKE_VERSION(2, 8)
  fuse_lowlevel_notify_inval_inode(cfuse->ch, fino, off, len);
#endif
}

static void do_init(void *data, fuse_conn_info *bar)
{
  CephFuse::Handle *cfuse = (CephFuse::Handle *)data;
  if (cfuse->fd_on_success) {
    //cout << "fuse init signaling on fd " << fd_on_success << std::endl;
    uint32_t r = 0;
    int err = safe_write(cfuse->fd_on_success, &r, sizeof(r));
    if (err) {
      derr << "fuse_ll: do_init: safe_write failed with error "
	   << cpp_strerror(err) << dendl;
      ceph_abort();
    }
    //cout << "fuse init done signaling on fd " << fd_on_success << std::endl;

    // close stdout, etc.
    ::close(0);
    ::close(1);
    ::close(2);
  }
}

const static struct fuse_lowlevel_ops fuse_ll_oper = {
 init: do_init,
 destroy: 0,
 lookup: fuse_ll_lookup,
 forget: fuse_ll_forget,
 getattr: fuse_ll_getattr,
 setattr: fuse_ll_setattr,
 readlink: fuse_ll_readlink,
 mknod: fuse_ll_mknod,
 mkdir: fuse_ll_mkdir,
 unlink: fuse_ll_unlink,
 rmdir: fuse_ll_rmdir,
 symlink: fuse_ll_symlink,
 rename: fuse_ll_rename,
 link: fuse_ll_link,
 open: fuse_ll_open,
 read: fuse_ll_read,
 write: fuse_ll_write,
 flush: fuse_ll_flush,
 release: fuse_ll_release,
 fsync: fuse_ll_fsync,
 opendir: fuse_ll_opendir,
 readdir: fuse_ll_readdir,
 releasedir: fuse_ll_releasedir,
 fsyncdir: 0,
 statfs: fuse_ll_statfs,
 setxattr: fuse_ll_setxattr,
 getxattr: fuse_ll_getxattr,
 listxattr: fuse_ll_listxattr,
 removexattr: fuse_ll_removexattr,
 access: 0,
 create: fuse_ll_create,
 getlk: 0,
 setlk: 0,
 bmap: 0
};


CephFuse::Handle::Handle(Client *c, int fd) :
  fd_on_success(fd),
  client(c),
  ch(NULL),
  se(NULL),
  mountpoint(NULL),
  stag_lock("fuse_ll.cc stag_lock"),
  last_stag(0)
{
  snap_stag_map[CEPH_NOSNAP] = 0;
  stag_snap_map[0] = CEPH_NOSNAP;
}

void CephFuse::Handle::finalize()
{
  client->ll_register_ino_invalidate_cb(NULL, NULL);

  if (se)
    fuse_remove_signal_handlers(se);
  if (ch)
    fuse_session_remove_chan(ch);
  if (se)
    fuse_session_destroy(se);
  if (ch)
    fuse_unmount(mountpoint, ch);

}

int CephFuse::Handle::init(int argc, const char *argv[])
{
  // set up fuse argc/argv
  int newargc = 0;
  const char **newargv = (const char **) malloc((argc + 10) * sizeof(char *));
  if(!newargv)
    return ENOMEM;

  newargv[newargc++] = argv[0];
  newargv[newargc++] = "-f";  // stay in foreground

  if (g_conf->fuse_allow_other) {
    newargv[newargc++] = "-o";
    newargv[newargc++] = "allow_other";
  }
  if (g_conf->fuse_default_permissions) {
    newargv[newargc++] = "-o";
    newargv[newargc++] = "default_permissions";
  }
  if (g_conf->fuse_big_writes) {
    newargv[newargc++] = "-o";
    newargv[newargc++] = "big_writes";
  }
  if (g_conf->fuse_atomic_o_trunc) {
    newargv[newargc++] = "-o";
    newargv[newargc++] = "atomic_o_trunc";
  }

  if (g_conf->fuse_debug)
    newargv[newargc++] = "-d";

  for (int argctr = 1; argctr < argc; argctr++)
    newargv[newargc++] = argv[argctr];

  struct fuse_args args = FUSE_ARGS_INIT(newargc, (char**)newargv);
  int ret = 0;

  char *mountpoint;
  if (fuse_parse_cmdline(&args, &mountpoint, NULL, NULL) == -1) {
    derr << "fuse_parse_cmdline failed." << dendl;
    ret = EINVAL;
    goto done;
  }

  ch = fuse_mount(mountpoint, &args);
  if (!ch) {
    derr << "fuse_mount(mountpoint=" << mountpoint << ") failed." << dendl;
    ret = EIO;
    goto done;
  }

  se = fuse_lowlevel_new(&args, &fuse_ll_oper, sizeof(fuse_ll_oper), this);
  if (!se) {
    derr << "fuse_lowlevel_new failed" << dendl;
    ret = EDOM;
    goto done;
  }

  signal(SIGTERM, SIG_DFL);
  signal(SIGINT, SIG_DFL);
  if (fuse_set_signal_handlers(se) == -1) {
    derr << "fuse_set_signal_handlers failed" << dendl;
    ret = ENOSYS;
    goto done;
  }

  fuse_session_add_chan(se, ch);

  client->ll_register_getgroups_cb(getgroups_cb, this);

  if (g_conf->fuse_use_invalidate_cb)
    client->ll_register_ino_invalidate_cb(invalidate_cb, this);

done:
  fuse_opt_free_args(&args);
  free(newargv);
  return ret;
}

int CephFuse::Handle::loop()
{
  return fuse_session_loop(se);
}

uint64_t CephFuse::Handle::fino_snap(uint64_t fino)
{
  Mutex::Locker l(stag_lock);
  uint64_t stag = FINO_STAG(fino);
  assert(stag_snap_map.count(stag));
  return stag_snap_map[stag];
}

vinodeno_t CephFuse::Handle::fino_vino(inodeno_t fino)
{
  if (fino.val == 1) {
    fino = inodeno_t(client->get_root_ino());
  }
  vinodeno_t vino(FINO_INO(fino), fino_snap(fino));
  //cout << "fino_vino " << fino << " -> " << vino << std::endl;
  return vino;
}

Inode * CephFuse::Handle::iget(inodeno_t fino)
{
  Inode *in =
    client->ll_get_inode(fino_vino(fino));
  return in;
}

void CephFuse::Handle::iput(Inode *in)
{
    client->ll_put(in);
}

uint64_t CephFuse::Handle::make_fake_ino(inodeno_t ino, snapid_t snapid)
{
  Mutex::Locker l(stag_lock);
  uint64_t stag;
  if (snap_stag_map.count(snapid) == 0) {
    stag = ++last_stag;
    snap_stag_map[snapid] = stag;
    stag_snap_map[stag] = snapid;
  } else
    stag = snap_stag_map[snapid];
  inodeno_t fino = MAKE_FINO(ino, stag);
  //cout << "make_fake_ino " << ino << "." << snapid << " -> " << fino << std::endl;
  return fino;
}

CephFuse::CephFuse(Client *c, int fd) : _handle(new CephFuse::Handle(c, fd))
{
}

CephFuse::~CephFuse()
{
  delete _handle;
}

int CephFuse::init(int argc, const char *argv[])
{
  return _handle->init(argc, argv);
}

int CephFuse::loop()
{
  return _handle->loop();
}

void CephFuse::finalize()
{
  return _handle->finalize();
}
