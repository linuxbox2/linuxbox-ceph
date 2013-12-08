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
#include "Messenger.h"
#include "include/atomic.h"
#include <boost/intrusive/avl_set.hpp>

namespace bi = boost::intrusive;

class XioConnection : public Connection
{
public:
  enum type { ACTIVE, PASSIVE };
private:
  XioConnection::type xio_conn_type;
  entity_inst_t peer;
  struct xio_session *session;
  struct xio_connection	*conn;
  pthread_spinlock_t sp;
  atomic_t send;
  atomic_t recv;

  /* batching */
  struct msg_seq {
    bool p;
    int cnt;
    pthread_spinlock_t sp;
    list<struct xio_msg *> seq;
    msg_seq() : p(false) { 
      pthread_spin_init(&sp, PTHREAD_PROCESS_PRIVATE);
    }
    void append(struct xio_msg* req) { seq.push_back(req); --cnt; }
  } in_seq;

  // conns_entity_map comparison functor
  struct EntityComp
  {
    // for internal ordering
    bool operator()(const XioConnection &lhs,  const XioConnection &rhs) const
      {  return lhs.get_peer() < rhs.get_peer(); }

    // for external search by entity_inst_t(peer)
    bool operator()(const entity_inst_t &peer, const XioConnection &c) const
      {  return peer < c.get_peer(); }

    bool operator()(const XioConnection &c, const entity_inst_t &peer) const
      {  return c.get_peer() < peer;  }
  };

  bi::avl_set_member_hook<> conns_entity_map_hook;

  typedef bi::member_hook<XioConnection, bi::avl_set_member_hook<>,
			  &XioConnection::conns_entity_map_hook> EntityHook;
  typedef bi::avl_set< XioConnection, EntityHook, bi::compare<EntityComp> > EntitySet;

  friend class XioMessenger;
  friend class boost::intrusive_ptr<XioConnection>;

public:
  XioConnection(Messenger *m, XioConnection::type _type,
		const entity_inst_t& peer) :
    Connection(m),
    xio_conn_type(_type),
    peer(peer),
    session(NULL),
    conn(NULL),
    in_seq()
    {
      pthread_spin_init(&sp, PTHREAD_PROCESS_PRIVATE);
    }

  bool is_connected() { return false; }
  const entity_inst_t& get_peer() const { return peer; }

  int on_msg(struct xio_session *session, struct xio_msg *req,
	     int more_in_batch, void *cb_user_context);

};

typedef boost::intrusive_ptr<XioConnection> XioConnectionRef;

#endif /* XIO_CONNECTION_H */
