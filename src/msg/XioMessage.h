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

#ifndef XIO_MESSAGE_H
#define XIO_MESSAGE_H

#include "SimplePolicyMessenger.h"
extern "C" {
#include "libxio.h"
}
#include "XioConnection.h"

class XioCompletion : public Message::Completion
{
private:
  list <struct xio_msg *> msg_seq;
  friend class XioConnection;
  friend class XioMessenger;
public:
  XioCompletion(Message *_m, list <struct xio_msg *>& _msg_seq) :
    Completion(_m), msg_seq(_msg_seq)
    {}
  virtual void finish(int r);
  virtual ~XioCompletion() { }
};

#endif /* XIO_MESSAGE_H */
