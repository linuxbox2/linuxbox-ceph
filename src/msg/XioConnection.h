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

#ifndef XIO_CONNECTION_H
#define XIO_CONNECTION_H

extern "C" {
#include "libxio.h"
}
#include "Connection.h"
#include <boost/intrusive/avl_set.hpp>


class XioConnection : public Connection
{
private:
  entity_inst_t peer;
  struct xio_session *session;
  boost::intrusive::avl_set_member_hook<> conns_entity_map_hook;

  typedef boost::intrusive::member_hook<XioConnection,
					boost::intrusive::avl_set_member_hook<>,
					&XioConnection::conns_entity_map_hook> EntityOrder;
  typedef boost::intrusive::avl_set< XioConnection, EntityOrder> EntitySet;

  friend class XioMessenger;
  friend class boost::intrusive_ptr<XioConnection>;

public:
  bool is_connected() { return false; }
  const entity_inst_t& get_peer() const { return peer; }

};

// conns_entity_map comparison functor
struct XioEntityComp
{
  bool operator()(const entity_inst_t &peer, const XioConnection &c) const
    {  return peer < c.get_peer(); }

  bool operator()(const XioConnection &c, const entity_inst_t &peer) const
    {  return c.get_peer() < peer;  }
};

typedef boost::intrusive_ptr<XioConnection> XioConnectionRef;

#endif /* XIO_CONNECTION_H */
