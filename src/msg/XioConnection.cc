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
    " ver: " << hdr.hdr.version <<
    " front_len: " << hdr.hdr.front_len <<
    " middle_len: " << hdr.hdr.middle_len <<
    " data_len: " << hdr.hdr.data_len <<
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

#define uint_to_timeval(tv, s) ((tv).tv_sec = (s), (tv).tv_usec = 0)

int XioConnection::on_request(struct xio_session *session,
			      struct xio_msg *req,
			      int more_in_batch,
			      void *cb_user_context)
{
    list<struct xio_msg *> batch;

    /* XXX in future XIO will provide an accumulator */
    pthread_spin_lock(&in_batch.sp);
    if (in_batch.p) {
      if (more_in_batch) {
	in_batch.batch.push_back(req);
	pthread_spin_unlock(&in_batch.sp);
	return 0;
      }
      batch = in_batch.batch;
      in_batch.batch.clear();
      in_batch.p = false;
    }
    else {
      if (more_in_batch) {
	in_batch.batch.push_back(req);
	in_batch.p = true;
	pthread_spin_unlock(&in_batch.sp);
	return 0;
      }
      batch.push_back(req);
    }
    pthread_spin_unlock(&in_batch.sp);

    XioMessenger *msgr = static_cast<XioMessenger*>(get_messenger());

    ceph_msg_header header;
    ceph_msg_footer footer;
    bufferlist front, middle, data;

    struct timeval t1, t2;
    uint64_t seq;

    list<struct xio_msg *>::iterator msg_iter = batch.begin();
    struct xio_msg *treq = *(batch.begin());
    xio_msg_hdr hdr(header,
		    buffer::create_static(treq->in.header.iov_len,
					  (char*) treq->in.header.iov_base));
    uint_to_timeval(t1, treq->timestamp);

    print_xio_msg_hdr(hdr);

    int ix, blen, iov_len;
    struct xio_iovec_ex *msg_iov;

    bufferlist &blist = front;
    blen = header.front_len;

    while (blen && (msg_iter != batch.end())) {
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

    while (blen && (msg_iter != batch.end())) {
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

    while (blen && (msg_iter != batch.end())) {
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

    Message *m =
      decode_message(msgr->cct, header, footer, front, middle, data);

    cout << "m is " << m << std::endl;

    if (m) {
      /* update timestamps */
      m->set_recv_stamp(t1);
      m->set_recv_complete_stamp(t2);
      m->set_seq(seq);

      /* dispatch it */
      // XXXX finish
    }

    return 0;
}
