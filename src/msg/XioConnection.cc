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

void print_xio_msg_hdr(XioMsgHdr &hdr)
{

  cout << "ceph header: " <<
    " front_len: " << hdr.hdr->front_len <<
    " seq: " << hdr.hdr->seq <<
    " tid: " << hdr.hdr->tid <<
    " type: " << hdr.hdr->type <<
    " prio: " << hdr.hdr->priority <<
    " name type: " << (int) hdr.hdr->src.type <<
    " name num: " << (int) hdr.hdr->src.num <<
    " version: " << hdr.hdr->version <<
    " compat_version: " << hdr.hdr->compat_version <<
    " front_len: " << hdr.hdr->front_len <<
    " middle_len: " << hdr.hdr->middle_len <<
    " data_len: " << hdr.hdr->data_len <<
    " xio header: " <<
    " msg_cnt: " << hdr.msg_cnt <<
    std::endl;

  cout << "ceph footer: " <<
    " front_crc: " << hdr.ftr->front_crc <<
    " middle_crc: " << hdr.ftr->middle_crc <<
    " data_crc: " << hdr.ftr->data_crc <<
    " sig: " << hdr.ftr->sig <<
    " flags: " << (uint32_t) hdr.ftr->flags <<
    std::endl;
}

void print_ceph_msg(Message *m)
{
  if (m->get_magic() & (MSG_MAGIC_XIO & MSG_MAGIC_TRACE_DTOR)) {
    ceph_msg_header& header = m->get_header();
    cout << "header version " << header.version <<
      " compat version " << header.compat_version <<
      std::endl;
  }
}

XioConnection::XioConnection(XioMessenger *m, XioConnection::type _type,
			     const entity_inst_t& peer) :
  Connection(m),
  xio_conn_type(_type),
  portal(m->default_portal()),
  peer(peer),
  session(NULL),
  conn(NULL),
  in_seq()
{
  pthread_spin_init(&sp, PTHREAD_PROCESS_PRIVATE);
  peer_addr = peer.addr;
  peer_type = peer.name.type();

  /* XXXX fake features, aieee! */
  set_features(68719476735);

}

#define uint_to_timeval(tv, s) ((tv).tv_sec = (s), (tv).tv_usec = 0)

int XioConnection::on_msg_req(struct xio_session *session,
			      struct xio_msg *req,
			      int more_in_batch,
			      void *cb_user_context)
{
  struct xio_msg *treq;
  XioMsg *xmsg;

  /* XXX this is an asymmetry Eyal plans to fix, at some point */
  switch (req->type) {
  case XIO_MSG_TYPE_RSP:
    /* XXX piggy-backed data is in req->request */
    dereg_xio_req(req);
    xmsg = static_cast<XioMsg*>(req->user_context);
    xio_release_response(req);
    if (xmsg)
      xmsg->put();
    return 0;
    break;
  default:
    treq = req;
    break;
  }

  /* XXX Accelio guarantees message ordering at
   * xio_session */
  pthread_spin_lock(&sp);
  if (! in_seq.p) {
#if 0 /* XXX */
    printf("receive req %p treq %p iov_base %p iov_len %d data_iovlen %d\n",
	   req, treq, treq->in.header.iov_base,
	   (int) treq->in.header.iov_len,
	   (int) treq->in.data_iovlen);
#endif
    XioMsgCnt msg_cnt(
      buffer::create_static(treq->in.header.iov_len,
			    (char*) treq->in.header.iov_base));
    in_seq.cnt = msg_cnt.msg_cnt;
    in_seq.p = true;
  }
  in_seq.append(req);
  if (in_seq.cnt > 0) {
    pthread_spin_unlock(&sp);
    return 0;
  }
  else
    in_seq.p = false;

  pthread_spin_unlock(&sp);

  XioMessenger *msgr = static_cast<XioMessenger*>(get_messenger());
  XioCompletionHook *completion_hook =
    new XioCompletionHook(NULL, in_seq.seq);
  list<struct xio_msg *>& msg_seq = completion_hook->msg_seq;
  in_seq.seq.clear();

  ceph_msg_header header;
  ceph_msg_footer footer;
  buffer::list front, middle, data;

  struct timeval t1, t2;
  uint64_t seq;

  list<struct xio_msg *>::iterator msg_iter = msg_seq.begin();
  treq = *msg_iter;
  XioMsgHdr hdr(header, footer,
		buffer::create_static(treq->in.header.iov_len,
				      (char*) treq->in.header.iov_base));

  uint_to_timeval(t1, treq->timestamp);

#if 0 /* XXX */
  int iov_front_len;
  if (hdr.hdr->type == 41) {
    iov_front_len = 0;
    print_xio_msg_hdr(hdr);
  }
#endif

  int ix, blen, iov_len;
  struct xio_iovec_ex *msg_iov;

  blen = header.front_len;

  while (blen && (msg_iter != msg_seq.end())) {
    treq = *msg_iter;
    iov_len = treq->in.data_iovlen;
    for (ix = 0; blen && (ix < iov_len); ++ix) {
      msg_iov = &treq->in.data_iov[ix];

      /* XXX need to detect any buffer which needs to be
       * split due to coalescing of a segment (front, middle,
       * data) boundary */
#if 0 /* XXX */
      if (hdr.hdr->type == 41) {
	printf("recv req %p data off %d iov_base %p iov_len %d\n",
	       treq, ix,
	       msg_iov->iov_base,
	       (int) msg_iov->iov_len);
	iov_front_len += msg_iov->iov_len;
      }
#endif

      /* XXX need to take only MIN(blen, iov_len) */
      front.append(
	buffer::create_static(
	  msg_iov->iov_len, (char*) msg_iov->iov_base));

      blen -= msg_iov->iov_len;
    }
    /* XXX as above, if a buffer is split, then we needed to track
     * the new start (carry) and not advance */
    msg_iter++;
  }

#if 0 /* XXX */
      if (hdr.hdr->type == 41) {
	printf("iov_front_len %d\n", iov_front_len);
	cout << "front (payload) dump:" << std::endl;
	front.hexdump(cout);
      }
#endif


  blen = header.middle_len;

  while (blen && (msg_iter != msg_seq.end())) {
    treq = *msg_iter;
    iov_len = treq->in.data_iovlen;
    for (ix = 0; blen && (ix < iov_len); ++ix) {
      msg_iov = &treq->in.data_iov[ix];

      /* XXX need to take only MIN(blen, iov_len) */
      middle.append(
	buffer::create_static(
	  msg_iov->iov_len, (char*) msg_iov->iov_base));
      blen -= msg_iov->iov_len;
    }
    msg_iter++;
  }

  blen = header.data_len;

  while (blen && (msg_iter != msg_seq.end())) {
    treq = *msg_iter;
    iov_len = treq->in.data_iovlen;
    for (ix = 0; blen && (ix < iov_len); ++ix) {
      msg_iov = &treq->in.data_iov[ix];
      data.append(
	buffer::create_static(
	  msg_iov->iov_len, (char*) msg_iov->iov_base));
      blen -= msg_iov->iov_len;
    }
    msg_iter++;
  }

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
    completion_hook->set_message(m);
    m->set_completion_hook(completion_hook);

    /* trace flag */
    m->set_magic(MSG_MAGIC_XIO|MSG_MAGIC_TRACE_1);

    /* update timestamps */
    m->set_recv_stamp(t1);
    m->set_recv_complete_stamp(t2);
    m->set_seq(seq);

    /* XXXX validate peer type */
    if (peer_type == -1)
      peer_type = hdr.peer_type;
    cout << "before dispatch: peer type: " << this->get_peer_type()
	 << std::endl;

    cout << "decode m is " << m->get_type() << std::endl;

#if 0 /* XXX */
    if (m->get_type() == 4) {
      cout << "stop 4 " << std::endl;
    }

    if (m->get_type() == 18) {
      cout << "stop 18 " << std::endl;
    }

    if (m->get_type() == 15) {
      cout << "stop 15 " << std::endl;
    }
#endif

    /* dispatch it */
    msgr->ms_deliver_dispatch(m);
  } else {
    /* responds for undecoded messages and frees hook */
    cout << "decode m failed" << std::endl;
    completion_hook->on_err_finalize(this);
  }

  return 0;
}

int XioConnection::on_msg_send_complete(struct xio_session *session,
					struct xio_msg *rsp,
					void *conn_user_context)
{
  /* responder side cleanup */
  finalize_response_msg(rsp);
  return 0;
} /* on_msg_send_complete */

int XioConnection::on_msg_delivered(struct xio_session *session,
				    struct xio_msg *req,
				    int more_in_batch,
				    void *conn_user_context)
{
  /* requester delivery receipt */
  return 0;
}  /* on_msg_delivered */
