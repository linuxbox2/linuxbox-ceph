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
#include "msg/msg_types.h"
#include <boost/intrusive/list.hpp>

namespace bi = boost::intrusive;

class XioMsgCnt
{
public:
  __le32 msg_cnt;
  buffer::list bl;
public:
  XioMsgCnt(buffer::ptr p)
    {
      bl.append(p);
      buffer::list::iterator bl_iter = bl.begin();
      ::decode(msg_cnt, bl_iter);
    }
};

class XioMsgHdr
{
public:
  __le32 msg_cnt;
  __le32 peer_type;
  ceph_msg_header* hdr;
  ceph_msg_footer* ftr;
  buffer::list bl;
public:
  XioMsgHdr(ceph_msg_header& _hdr, ceph_msg_footer& _ftr)
    : msg_cnt(0), hdr(&_hdr), ftr(&_ftr)
    { }

  XioMsgHdr(ceph_msg_header& _hdr, ceph_msg_footer &_ftr, buffer::ptr p)
    : hdr(&_hdr), ftr(&_ftr)
    {
      bl.append(p);
      buffer::list::iterator bl_iter = bl.begin();
      decode(bl_iter);
    }

  const buffer::list& get_bl() { encode(bl); return bl; };

  inline void encode_hdr(buffer::list& bl) const {
    ::encode(msg_cnt, bl);
    ::encode(peer_type, bl);
    ::encode(hdr->seq, bl);
    ::encode(hdr->tid, bl);
    ::encode(hdr->type, bl);
    ::encode(hdr->priority, bl);
    ::encode(hdr->version, bl);
    ::encode(hdr->front_len, bl);
    ::encode(hdr->middle_len, bl);
    ::encode(hdr->data_len, bl);
    ::encode(hdr->data_off, bl);
    ::encode(hdr->src.type, bl);
    ::encode(hdr->src.num, bl);
    ::encode(hdr->compat_version, bl);
    ::encode(hdr->crc, bl);
  }

  inline void encode_ftr(buffer::list& bl) const {
    ::encode(ftr->front_crc, bl);
    ::encode(ftr->middle_crc, bl);
    ::encode(ftr->data_crc, bl);
    ::encode(ftr->sig, bl);
    ::encode(ftr->flags, bl);
  }

  inline void encode(buffer::list& bl) const {
    encode_hdr(bl);
    encode_ftr(bl);
  }

  inline void decode_hdr(buffer::list::iterator& bl) {
    ::decode(msg_cnt, bl);
    ::decode(peer_type, bl);
    ::decode(hdr->seq, bl);
    ::decode(hdr->tid, bl);
    ::decode(hdr->type, bl);
    ::decode(hdr->priority, bl);
    ::decode(hdr->version, bl);
    ::decode(hdr->front_len, bl);
    ::decode(hdr->middle_len, bl);
    ::decode(hdr->data_len, bl);
    ::decode(hdr->data_off, bl);
    ::decode(hdr->src.type, bl);
    ::decode(hdr->src.num, bl);
    ::decode(hdr->compat_version, bl);
    ::decode(hdr->crc, bl);
  }

  inline void decode_ftr(buffer::list::iterator& bl) {
    ::decode(ftr->front_crc, bl);
    ::decode(ftr->middle_crc, bl);
    ::decode(ftr->data_crc, bl);
    ::decode(ftr->sig, bl);
    ::decode(ftr->flags, bl);
  }

  inline void decode(buffer::list::iterator& bl) {
    decode_hdr(bl);
    decode_ftr(bl);
  }

  virtual ~XioMsgHdr()
    {}
};

WRITE_CLASS_ENCODER(XioMsgHdr);

struct XioMsg
{
public:
  Message* m;
  XioMsgHdr hdr;
  struct xio_msg req_0;
  struct xio_msg* req_arr;
  XioConnection *xcon;
  bi::list_member_hook<> submit_list;
  int nbuffers;
  int nref;

public:
  XioMsg(Message *_m, XioConnection *_xcon) :
    m(_m), hdr(m->get_header(), m->get_footer()),
    req_arr(NULL), xcon(_xcon)
    {
      nref = 1;
      const entity_inst_t &inst = xcon->get_messenger()->get_myinst();
      hdr.peer_type = inst.name.type();
      hdr.hdr->src.type = inst.name.type();
      hdr.hdr->src.num = inst.name.num();
      memset(&req_0, 0, sizeof(struct xio_msg));
      /* XXX needed/wanted? on the last rather than first xio_msg? */
      req_0.flags = 0 /* XIO_MSG_FLAG_REQUEST_READ_RECEIPT */;
      req_0.user_context = this;
    }

  XioMsg* get() { ++nref; return this; };

  void put() {
    --nref;
    if (nref == 0) {
      this->~XioMsg();
      free(this);
    }
  }

  ~XioMsg()
    {
      free(req_arr);
      m->put();
    }

  typedef bi::list< XioMsg,
		    bi::member_hook< XioMsg,
				     bi::list_member_hook<>,
				     &XioMsg::submit_list >
		    > Queue;

};

static inline void dereg_xio_req(struct xio_msg *rreq)
{
  int code;
  struct xio_iovec_ex *iov;

  for (unsigned int ix = 0; ix < rreq->out.data_iovlen; ++ix) {
    iov = &rreq->out.data_iov[ix];
    if (iov->mr) {
	code = xio_dereg_mr(&iov->mr);
	if (code != 0) {
	  printf("%s xio_dereg_mr failed (%s)\n",
		 __func__, xio_strerror(code));
	}
    }
  }
}

static inline void finalize_response_msg(struct xio_msg *rsp)
{
  /* currently, there is no user context */
  free(rsp);
}

class XioCompletionHook : public Message::CompletionHook
{
private:
  list <struct xio_msg *> msg_seq;
  friend class XioConnection;
  friend class XioMessenger;
public:
  XioCompletionHook(Message *_m, list <struct xio_msg *>& _msg_seq) :
    CompletionHook(_m), msg_seq(_msg_seq)
    {}
  virtual void finish(int r);
  void on_err_finalize(XioConnection *xcon);
  virtual ~XioCompletionHook() { }
};

#endif /* XIO_MSG_H */
