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

extern XioPool *xrp_pool;

void XioCompletionHook::finish(int r)
{
  struct xio_msg *msg, *rsp;
  list <struct xio_msg *>::iterator iter;

  nrefs.inc();

  for (iter = msg_seq.begin(); iter != msg_seq.end(); ++iter) {
    msg = *iter;
    switch (msg->type) {
    case XIO_MSG_TYPE_REQ:
    {
      ConnectionRef conn = m->get_connection();
      XioConnection *xcon = static_cast<XioConnection*>(conn.get());

      /* XXX ack it (Eyal:  we'd like an xio_ack_response) */
      rsp = (struct xio_msg *) rsp_pool.alloc(sizeof(struct xio_msg));
      memset(&rsp->out, 0, sizeof(struct xio_vmsg));
      rsp->flags = 0;
      rsp->type = XIO_MSG_TYPE_RSP;
      rsp->user_context = this->get();
      rsp->request = msg;

      /* XXX not MP correct */
      pthread_spin_lock(&xcon->sp);
      (void) xio_send_response(rsp); /* XXX can now chain */
      pthread_spin_unlock(&xcon->sp);

    }
      break;
    case XIO_MSG_TYPE_RSP:
    case XIO_MSG_TYPE_ONE_WAY:
    default:
      abort();
      break;
    }
  }

  this->put();
}

void XioCompletionHook::on_err_finalize(XioConnection *xcon)
{
  struct xio_msg *msg, *rsp;
  list <struct xio_msg *>::iterator iter;

  for (iter = msg_seq.begin(); iter != msg_seq.end(); ++iter) {
    msg = *iter;
    switch (msg->type) {
    case XIO_MSG_TYPE_REQ:
    {
      /* XXX ack it (Eyal:  we'd like an xio_ack_response) */
      rsp = (struct xio_msg *) calloc(1, sizeof(struct xio_msg));
      rsp->request = msg;
      pthread_spin_lock(&xcon->sp);
      (void) xio_send_response(rsp); /* XXX can now chain */
      pthread_spin_unlock(&xcon->sp);
    }
      break;
    case XIO_MSG_TYPE_RSP:
    case XIO_MSG_TYPE_ONE_WAY:
    default:
      abort();
      break;
    }
  }
}
