// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 * Portions Copyright (C) 2013 CohortFS, LLC
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef XIO_MSG_H
#define XIO_MSG_H

#include "SimplePolicyMessenger.h"
extern "C" {
#include "libxio.h"
}
#include "XioConnection.h"

class xio_msg_cnt
{
public:
  __le32 msg_cnt;
  buffer::list bl;
public:
  xio_msg_cnt(buffer::ptr p)
    {
      bl.append(p);
      buffer::list::iterator bl_iter = bl.begin();
      ::decode(msg_cnt, bl_iter);
    }
};

class xio_msg_hdr
{
public:
  __le32 msg_cnt;
  ceph_msg_header &hdr;
  buffer::list bl;
public:
  xio_msg_hdr(ceph_msg_header& _hdr) :msg_cnt(0), hdr(_hdr)
    { }

  xio_msg_hdr(ceph_msg_header& _hdr, buffer::ptr p) : hdr(_hdr)
    {
      bl.append(p);
      buffer::list::iterator bl_iter = bl.begin();
      decode(bl_iter);
    }

  const buffer::list& get_bl() { encode(bl); return bl; };

  inline void update_lengths(buffer::list &payload, buffer::list &middle, 
			     buffer::list &data) {
    hdr.front_len = payload.buffers().size();
    hdr.middle_len = middle.buffers().size();
    hdr.data_len = data.buffers().size();
  }

  void encode(buffer::list& bl) const {
    ::encode(msg_cnt, bl);
    ::encode(hdr.seq, bl);
    ::encode(hdr.tid, bl);
    ::encode(hdr.type, bl);
    ::encode(hdr.priority, bl);
    ::encode(hdr.version, bl);
    ::encode(hdr.front_len, bl);
    ::encode(hdr.middle_len, bl);
    ::encode(hdr.data_len, bl);
    ::encode(hdr.data_off, bl);
    //::encode(hdr.src, bl);
    ::encode(hdr.compat_version, bl);
    ::encode(hdr.crc, bl);
  }

  void decode(buffer::list::iterator& bl) {
    ::decode(msg_cnt, bl);
    ::decode(hdr.seq, bl);
    ::decode(hdr.tid, bl);
    ::decode(hdr.type, bl);
    ::decode(hdr.priority, bl);
    ::decode(hdr.version, bl);
    ::decode(hdr.front_len, bl);
    ::decode(hdr.middle_len, bl);
    ::decode(hdr.data_len, bl);
    ::decode(hdr.data_off, bl);
    //::decode(hdr.src, bl);
    ::decode(hdr.compat_version, bl);
    ::decode(hdr.crc, bl);
  }

};

WRITE_CLASS_ENCODER(xio_msg_hdr);

class xio_msg_ftr
{
public:
  ceph_msg_footer ftr;
  buffer::list bl;
public:
  xio_msg_ftr(ceph_msg_footer &_ftr) : ftr(_ftr)
    { }

  xio_msg_ftr(ceph_msg_footer& _ftr, buffer::ptr p)
    {
      bl.append(p);
      buffer::list::iterator bl_iter = bl.begin();
      decode(bl_iter);
      _ftr = ftr;
    }

  const buffer::list& get_bl() { encode(bl); return bl; };

  void encode(buffer::list& bl) const {
    ::encode(ftr.front_crc, bl);
    ::encode(ftr.middle_crc, bl);
    ::encode(ftr.data_crc, bl);
    ::encode(ftr.sig, bl);
    ::encode(ftr.flags, bl);
  }

  void decode(buffer::list::iterator& bl) {
    ::decode(ftr.front_crc, bl);
    ::decode(ftr.middle_crc, bl);
    ::decode(ftr.data_crc, bl);
    ::decode(ftr.sig, bl);
    ::decode(ftr.flags, bl);
  }

};

WRITE_CLASS_ENCODER(xio_msg_ftr);

struct Xio_OMsg : public RefCountedObject
{
public:
  Message* m;
  xio_msg_hdr hdr;
  xio_msg_ftr ftr;
  struct xio_msg req_0;
  struct xio_msg *req_arr;
  int nbuffers;

public:
  Xio_OMsg(Message *_m) : m(_m),
			hdr(m->get_header()),
			ftr(m->get_footer()),
			req_arr(NULL)
    {
      m->get();
      memset(&req_0, 0, sizeof(struct xio_msg));
      req_0.user_context = this;
    }

  ~Xio_OMsg()
    {
      free(req_arr);
      m->put();
    }
};

static inline void release_xio_req(struct xio_msg *rreq)
{
  int code;
  struct xio_iovec_ex *msg_iov;
  Xio_OMsg *xmsg = static_cast<Xio_OMsg*>(rreq->user_context);

  for (unsigned int ix = 0; ix < rreq->out.data_iovlen; ++ix) {
    msg_iov = &rreq->out.data_iov[ix];
    if (msg_iov->mr) {
      code = xio_dereg_mr(&msg_iov->mr);
      if (code != 0) {
	  printf("%s xio_dereg_mr failed (%s)\n",
		 __func__, xio_strerror(code));
      }
    }
  }

  /* eventually frees xmsg */
  xmsg->put();
}

class XioReplyHook : public Message::ReplyHook
{
private:
  list <struct xio_msg *> msg_seq;
  friend class XioConnection;
  friend class XioMessenger;
public:
  XioReplyHook(Message *_m, list <struct xio_msg *>& _msg_seq) :
    ReplyHook(_m), msg_seq(_msg_seq)
    {}
  virtual int reply(Message *reply);
  virtual void finish(int r);
  virtual ~XioReplyHook() { }
};

#endif /* XIO_MSG_H */
