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

#include "XioMsg.h"
#include "XioConnection.h"
#include "XioMessenger.h"

void print_xio_msg_hdr(xio_msg_hdr &hdr)
{

  cout << "ceph header: " <<
    " front_len: " << hdr.hdr.front_len <<
    " seq: " << hdr.hdr.seq <<
    " tid: " << hdr.hdr.tid <<
    " type: " << hdr.hdr.type <<
    " prio: " << hdr.hdr.priority <<
    " version: " << hdr.hdr.version <<
    " compat_version: " << hdr.hdr.compat_version <<
    " front_len: " << hdr.hdr.front_len <<
    " middle_len: " << hdr.hdr.middle_len <<
    " data_len: " << hdr.hdr.data_len <<
    " xio header: " <<
    " msg_cnt: " << hdr.msg_cnt <<
    std::endl;
}

void print_xio_msg_ftr(xio_msg_ftr &ftr)
{

  cout << "ceph footer: " <<
    " front_crc: " << ftr.ftr.front_crc <<
    " middle_crc: " << ftr.ftr.middle_crc <<
    " data_crc: " << ftr.ftr.data_crc <<
    " sig: " << ftr.ftr.sig <<
    " flags: " << (uint32_t) ftr.ftr.flags <<
    std::endl;
}

void print_ceph_msg(Message *m)
{
  ceph_msg_header& header = m->get_header();
  cout << "header version " << header.version << 
    " compat version " << header.compat_version <<
    std::endl;
}

#define uint_to_timeval(tv, s) ((tv).tv_sec = (s), (tv).tv_usec = 0)

int XioConnection::on_msg_req(struct xio_session *session,
			      struct xio_msg *req,
			      int more_in_batch,
			      void *cb_user_context)
{
  struct xio_msg *treq;

  /* XXX this is an asymmetry Eyal plans to fix, at some point */
  switch (req->type) {
  case XIO_MSG_TYPE_RSP:
    /* XXX piggy-backed data is in req->request */
    xio_release_response(req);
    release_xio_req(req);
    return 0;
    break;
  default:
    treq = req;
    break;
  }

  /* XXX Accelio guarantees message ordering at
   * xio_session */
  if (! in_seq.p) {
    printf("req %p treq %p iov_base %p iov_len %d\n",
	   req, treq, treq->in.header.iov_base,
	   treq->in.header.iov_len);
    xio_msg_cnt msg_cnt(
      buffer::create_static(treq->in.header.iov_len,
			    (char*) treq->in.header.iov_base));
    in_seq.cnt = msg_cnt.msg_cnt;
    in_seq.p = true;
  }
  in_seq.append(req);
    if (in_seq.cnt > 0)
      return 0;
    else
      in_seq.p = false;

    XioMessenger *msgr = static_cast<XioMessenger*>(get_messenger());
    XioReplyHook *reply_hook = new XioReplyHook(NULL, in_seq.seq);
    list<struct xio_msg *>& msg_seq = reply_hook->msg_seq;
    in_seq.seq.clear();

    ceph_msg_header header;
    ceph_msg_footer footer;
    buffer::list front, middle, data;

    struct timeval t1, t2;
    uint64_t seq;

    struct xio_msg *rreq;
    list<struct xio_msg *>::iterator msg_iter = msg_seq.begin();

    treq = *msg_iter;
    xio_msg_hdr hdr(header,
		    buffer::create_static(treq->in.header.iov_len,
					  (char*) treq->in.header.iov_base));

    uint_to_timeval(t1, treq->timestamp);

    print_xio_msg_hdr(hdr);

    int ix, blen, iov_len;
    struct xio_iovec_ex *msg_iov;

    buffer::list &blist = front;
    blen = header.front_len;

    while (blen && (msg_iter != msg_seq.end())) {
      treq = *msg_iter;
      iov_len = treq->in.data_iovlen;
      for (ix = 0; blen && (ix < iov_len); ++ix, --blen) {
	msg_iov = &treq->in.data_iov[ix];
	blist.append(
	  buffer::create_static(
	    msg_iov->iov_len, (char*) msg_iov->iov_base));
      }
      msg_iter++;
    }

    blist = middle;
    blen = header.middle_len;

    while (blen && (msg_iter != msg_seq.end())) {
      treq = *msg_iter;
      iov_len = treq->in.data_iovlen;
      for (ix = 0; blen && (ix < iov_len); ++ix, --blen) {
	msg_iov = &treq->in.data_iov[ix];
	blist.append(
	  buffer::create_static(
	    msg_iov->iov_len, (char*) msg_iov->iov_base));
      }
      msg_iter++;
    }

    blist = data;
    blen = header.data_len;

    while (blen && (msg_iter != msg_seq.end())) {
      treq = *msg_iter;
      iov_len = treq->in.data_iovlen;
      for (ix = 0; blen && (ix < iov_len); ++ix, --blen) {
	msg_iov = &treq->in.data_iov[ix];
	blist.append(
	  buffer::create_static(
	    msg_iov->iov_len, (char*) msg_iov->iov_base));
      }
      msg_iter++;
    }

    /* footer */
    msg_iov = &treq->in.data_iov[(treq->in.data_iovlen - 1)];
    xio_msg_ftr ftr(footer,
		    buffer::create_static(msg_iov->iov_len,
					  (char*) msg_iov->iov_base));

    print_xio_msg_ftr(ftr);

    seq = treq->sn;
    uint_to_timeval(t2, treq->timestamp);

    /* update connection timestamp */
    recv.set(treq->timestamp);

    Message *m =
      decode_message(msgr->cct, header, footer, front, middle, data);

    cout << "m is " << m << std::endl;

    if (m) {
      /* completion */
      this->get(); /* XXX getting underrun */
      m->set_connection(this);

      /* reply hook */
      reply_hook->set_message(m);
      m->set_reply_hook(reply_hook);

      /* update timestamps */
      m->set_recv_stamp(t1);
      m->set_recv_complete_stamp(t2);
      m->set_seq(seq);

      /* dispatch it */
      msgr->ms_deliver_dispatch(m);
    } else
      delete reply_hook;

    return 0;
}

int XioConnection::on_msg_send_complete(struct xio_session *session,
					struct xio_msg *rsp,
					void *conn_user_context)
{
  release_xio_req(rsp);
  return 0;
} /* on_msg_send_complete */
