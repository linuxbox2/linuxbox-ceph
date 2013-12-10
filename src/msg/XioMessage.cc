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

#include "XioMessage.h"
#include "XioConnection.h"
#include "XioMessenger.h"
#include "XioMsg.h"


void XioCompletion::finish(int r)
{
  printf("XioCompletion::finish called %p (%d)\n", this, r);
 
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
