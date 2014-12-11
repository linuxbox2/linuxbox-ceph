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

#include <boost/intrusive/avl_set.hpp>
#include <boost/intrusive/list.hpp>
extern "C" {
#include "libxio.h"
}
#include "XioInSeq.h"
#include "Connection.h"
#include "Messenger.h"
#include "include/atomic.h"

#define XIO_ALL_FEATURES (CEPH_FEATURES_ALL & \
			  ~CEPH_FEATURE_MSGR_KEEPALIVE2)

namespace bi = boost::intrusive;

class XioPortal;
class XioMessenger;
class XioMsg;

class XioConnection : public Connection
{
public:
  enum type { ACTIVE, PASSIVE };

private:
  XioConnection::type xio_conn_type;
  XioPortal *portal;
  atomic_t connected;
  entity_inst_t peer;
  struct xio_session *session;
  struct xio_connection	*conn;
  pthread_spinlock_t sp;
  atomic_t send;
  atomic_t recv;
  uint32_t magic;
  uint32_t special_handling;

  struct lifecycle {
    // different from Pipe states?
    enum lf_state {
      INIT,
      LOCAL_DISCON,
      REMOTE_DISCON,
      RECONNECTING,
      UP,
      DEAD } state;

    /* XXX */
    uint32_t reconnects;
    uint32_t connect_seq, peer_global_seq;
    uint32_t in_seq, out_seq_acked; // atomic<uint64_t>, got receipt
    atomic_t out_seq; // atomic<uint32_t>

    lifecycle() : state(lifecycle::INIT), in_seq(0), out_seq(0) {}

    void set_in_seq(uint32_t seq) {
      in_seq = seq;
    }

    uint32_t next_out_seq() {
      return out_seq.inc();
    };

  } state;

  /* batching */
  XioInSeq in_seq;

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

  bi::list_member_hook<> conns_hook;
  bi::avl_set_member_hook<> conns_entity_map_hook;

  typedef bi::list< XioConnection,
		    bi::member_hook<XioConnection, bi::list_member_hook<>,
				    &XioConnection::conns_hook > > ConnList;

  typedef bi::member_hook<XioConnection, bi::avl_set_member_hook<>,
			  &XioConnection::conns_entity_map_hook> EntityHook;

  typedef bi::avl_set< XioConnection, EntityHook,
		       bi::compare<EntityComp> > EntitySet;

  friend class XioPortal;
  friend class XioMessenger;
  friend class XioCompletionHook;
  friend class XioMsg;

  int on_disconnect_event() {
    connected.set(false);
    pthread_spin_lock(&sp);
    if (!conn)
      this->put();
    pthread_spin_unlock(&sp);
    return 0;
  }

  int on_teardown_event() {
    pthread_spin_lock(&sp);
    if (conn)
      xio_connection_destroy(conn);
    conn = NULL;
    pthread_spin_unlock(&sp);
    this->put();
    return 0;
  }

public:
  XioConnection(XioMessenger *m, XioConnection::type _type,
		const entity_inst_t& peer);

  ~XioConnection() {
    if (conn)
      xio_connection_destroy(conn);
  }

  bool is_connected() { return connected.read(); }

  const entity_inst_t& get_peer() const { return peer; }

  XioConnection* get() {
#if 0
    int refs = nref.read();
    cout << "XioConnection::get " << this << " " << refs << std::endl;
#endif
    RefCountedObject::get();
    return this;
  }

  void put() {
    RefCountedObject::put();
#if 0
    int refs = nref.read();
    cout << "XioConnection::put " << this << " " << refs << std::endl;
#endif
  }

  void disconnect() {
    if (is_connected()) {
      connected.set(false);
      xio_disconnect(conn); // normal teardown will clean up conn
    }
  }

  uint32_t get_magic() { return magic; }
  void set_magic(int _magic) { magic = _magic; }
  uint32_t get_special_handling() { return special_handling; }
  void set_special_handling(int n) { special_handling = n; }

  int passive_setup(); /* XXX */

  int on_msg_req(struct xio_session *session, struct xio_msg *req,
		 int more_in_batch, void *cb_user_context);

  int on_ow_msg_send_complete(struct xio_session *session, struct xio_msg *msg,
			      void *conn_user_context);

  int on_msg_error(struct xio_session *session, enum xio_status error,
		   struct xio_msg  *msg, void *conn_user_context);

  void msg_send_fail(XioMsg *xmsg, int code);

  void msg_release_fail(struct xio_msg *msg, int code);
};

class XioLoopbackConnection : public Connection
{
private:
  atomic_t seq;
public:
  XioLoopbackConnection(Messenger *m) : Connection(m->cct, m), seq(0)
    {
      const entity_inst_t& m_inst = m->get_myinst();
      peer_addr = m_inst.addr;
      peer_type = m_inst.name.type();
      set_features(XIO_ALL_FEATURES); /* XXXX set to ours */
    }

  XioLoopbackConnection* get() {
    return static_cast<XioLoopbackConnection*>(RefCountedObject::get());
  }

  virtual bool is_connected() { return true; }

  uint32_t get_seq() {
    return seq.read();
  }

  uint32_t next_seq() {
    return seq.inc();
  }
};

typedef boost::intrusive_ptr<XioLoopbackConnection> LoopbackConnectionRef;

#endif /* XIO_CONNECTION_H */
