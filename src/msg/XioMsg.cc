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

#include "XioMessenger.h"
#include "XioConnection.h"
#include "XioMsg.h"


int XioReplyHook::reply(Message *reply)
{
  XioConnection *xcon =
    static_cast<XioConnection*>(m->get_connection().get());

  reply->set_seq(0); /* XIO handles seq */
  reply->encode(xcon->get_features(), !xcon->msgr->cct->_conf->ms_nocrc);

  Xio_OMsg *xmsg = new Xio_OMsg(reply);

  buffer::list blist;
  struct xio_msg *req = &xmsg->req_0;
  struct xio_iovec_ex *msg_iov = req->out.data_iov, *iov;
  int ex_cnt;

  buffer::list &payload = reply->get_payload();
  buffer::list &middle = reply->get_middle();
  buffer::list &data = reply->get_data();

  cout << "payload: " << payload.buffers().size() <<
    " middle: " << middle.buffers().size() <<
    " data: " << data.buffers().size() <<
    std::endl;

  blist.append(payload);
  blist.append(middle);
  blist.append(data);

  const std::list<buffer::ptr>& buffers = blist.buffers();
  xmsg->nbuffers = buffers.size();
  ex_cnt = ((3 + xmsg->nbuffers) / XIO_MAX_IOV);

  /* XXX adjust ex_cnt to ensure enough segments to ack all
   * request segments */
  int msg_seq_len = msg_seq.size();
  int msg_seq_off = msg_seq_len - 1;
  if (ex_cnt < msg_seq_off)
    ex_cnt = msg_seq_off;

  xmsg->hdr.msg_cnt = 1 + ex_cnt;

  if (ex_cnt > 0) {
    xmsg->req_arr =
      (struct xio_msg *) calloc(ex_cnt, sizeof(struct xio_msg));
  }

  /* do the invariant part */
  int msg_off = 0;
  int req_off = -1; /* most often, not used */

  /* attach request, if any */
  if (msg_seq_len) {
    req->request = msg_seq.front();
    msg_seq.pop_front();
  }

  list<bufferptr>::const_iterator pb; 
  for (pb = buffers.begin(); pb != buffers.end(); ++pb) {

    /* assign buffer */
    iov = &msg_iov[msg_off];
    iov->iov_base = (void *) pb->c_str(); // is this efficient?
    iov->iov_len = pb->length();

    /* advance iov(s) */
    if (++msg_off >= XIO_MAX_IOV) {
      req->out.data_iovlen = msg_off;
      if (++req_off < ex_cnt) {
	/* next record */
	req->out.data_iovlen = XIO_MAX_IOV;
	req->more_in_batch++;
	/* XXX chain it */
	req = &xmsg->req_arr[req_off];
	/* attach request, if any */
	if (msg_seq_off--) {
	  req->request = msg_seq.front();
	  msg_seq.pop_front();
	}
	req->user_context = xmsg->get();
	msg_iov = req->out.data_iov;
	msg_off = 0;
      }
    }
  }

  /* fixup last msg */
  const std::list<buffer::ptr>& footer = xmsg->ftr.get_bl().buffers();
  assert(footer.size() == 1); /* XXX */
  pb = footer.begin();
  msg_iov[msg_off].iov_base = (char*) pb->c_str();
  msg_iov[msg_off].iov_len = pb->length();
  msg_off++;
  req->out.data_iovlen = msg_off;

  void print_xio_msg_ftr(xio_msg_ftr &ftr);
  print_xio_msg_ftr(xmsg->ftr);

  /* fixup first msg */
  req = &xmsg->req_0;

  /* overload header "length" members */
  xmsg->hdr.update_lengths(payload, middle, data);

  void print_xio_msg_hdr(xio_msg_hdr &hdr);
  print_xio_msg_hdr(xmsg->hdr);

  const std::list<buffer::ptr>& header = xmsg->hdr.get_bl().buffers();
  assert(header.size() == 1); /* XXX */
  pb = header.begin();
  req->out.header.iov_base = (char*) pb->c_str();
  req->out.header.iov_len = pb->length();

  /* deliver via xio, preserve ordering */
  if (xmsg->hdr.msg_cnt == 1)
    xio_send_response(req);
  else {
    /* XXX since the xio_msg structures here originated
     * in the library (and we're using the request/response
     * model), the messages are already linked, and we
     * have to deliver them 1-by-1 (XXX or do we?) */
    pthread_spin_lock(&xcon->sp);
    xio_send_response(&xmsg->req_0);
    for (req_off = 0; req_off < ex_cnt; ++req_off) {
      req = &xmsg->req_arr[req_off];
      xio_send_response(req);
    }
    pthread_spin_unlock(&xcon->sp);
  }

  /* it's now possible to use sn and timestamp */
  xcon->send.set(req->timestamp);

  /* XXXX finish */

  return 0;
} /* reply */

void XioReplyHook::finish(int r)
{
  printf("XioReplyHook::finish called %p (%d)\n", this, r);
 
  list <struct xio_msg *>::iterator iter;
  for (iter = msg_seq.begin(); iter != msg_seq.end(); ++iter) {
    struct xio_msg *msg = *iter;
    switch (msg->type) {
    case XIO_MSG_TYPE_REQ:
      /* XXX do we have to send a response?  Maybe just
       * ack? */
      break;
    case XIO_MSG_TYPE_RSP:
      xio_release_response(msg);
      release_xio_req(msg);
      break;
    case XIO_MSG_TYPE_ONE_WAY:
      /* XXX */
      break;
    default:
      abort();
      break;
    }
  }
}
