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

#include <arpa/inet.h>
#include <boost/lexical_cast.hpp>
#include <set>
#include <stdlib.h>

#include "XioMsg.h"
#include "XioMessenger.h"

Mutex mtx("XioMessenger Package Lock");
atomic_t initialized;

atomic_t XioMessenger::nInstances;

static struct xio_session_ops xio_msgr_ops;

/* Accelio API callouts */
static int on_session_event(struct xio_session *session,
			    struct xio_session_event_data *event_data,
			    void *cb_user_context)
{
  XioMessenger *msgr = static_cast<XioMessenger*>(cb_user_context);

  printf("session event: %s. reason: %s\n",
	 xio_session_event_str(event_data->event),
	 xio_strerror(event_data->reason));

  return msgr->session_event(session, event_data, cb_user_context);
}

static int on_new_session(struct xio_session *session,
			  struct xio_new_session_req *req,
			  void *cb_user_context)
{
  XioMessenger *msgr = static_cast<XioMessenger*>(cb_user_context);

  printf("new session %p user_context %p\n", session, cb_user_context);

  return (msgr->new_session(session, req, cb_user_context));
}

static int on_msg_send_complete(struct xio_session *session,
				struct xio_msg *rsp,
				void *conn_user_context)
{

  printf("msg send complete: session: %p rsp: %p user_context %p\n",
	 session, rsp, conn_user_context);

  XioConnection *xcon =
    static_cast<XioConnection*>(conn_user_context);

  return xcon->on_msg_send_complete(session, rsp, conn_user_context);
}

static int on_msg(struct xio_session *session,
		  struct xio_msg *req,
		  int more_in_batch,
		  void *cb_user_context)
{
  XioConnection *xcon =
    static_cast<XioConnection*>(cb_user_context);

  printf("on_msg session %p xcon %p\n", session, xcon);

  return xcon->on_msg_req(session, req, more_in_batch,
			  cb_user_context);
}

static int on_msg_delivered(struct xio_session *session,
			    struct xio_msg *msg,
			    int more_in_batch,
			    void *conn_user_context)
{
  printf("msg delivered session: %p msg: %p more: %d conn_user_context %p\n",
	   session, msg, more_in_batch, conn_user_context);

  XioConnection *xcon =
    static_cast<XioConnection*>(conn_user_context);

  return xcon->on_msg_delivered(session, msg, more_in_batch, conn_user_context);

  return 0;
}

static int on_msg_error(struct xio_session *session,
			enum xio_status error,
			struct xio_msg  *msg,
			void *conn_user_context)
{
  printf("msg error session: %p error: %s msg: %p conn_user_context %p\n",
	 session, xio_strerror(error), msg, conn_user_context);
  /* XIO promises to flush back undelivered messags */
    dereg_xio_req(msg);
    XioMsg *xmsg = static_cast<XioMsg*>(msg->user_context);
    if (xmsg)
      xmsg->put();

  return 0;
}

static int on_cancel(struct xio_session *session,
		     struct xio_msg  *msg,
		     enum xio_status result,
		     void *conn_user_context)
{
  printf("on cancel: session: %p msg: %p conn_user_context %p\n",
	 session, msg, conn_user_context);
  return 0;
}

static int on_cancel_request(struct xio_session *session,
			     struct xio_msg  *msg,
			     void *conn_user_context)
{
  printf("on cancel request: session: %p msg: %p conn_user_context %p\n",
	 session, msg, conn_user_context);
  return 0;
}

/* free functions */
static string xio_uri_from_entity(const entity_addr_t& addr, bool want_port)
{
  const char *host = NULL;
  char addr_buf[129];
  
  switch(addr.addr.ss_family) {
  case AF_INET:
    host = inet_ntop(AF_INET, &addr.addr4.sin_addr, addr_buf,
		     INET_ADDRSTRLEN);
    break;
  case AF_INET6:
    host = inet_ntop(AF_INET6, &addr.addr6.sin6_addr, addr_buf,
		     INET6_ADDRSTRLEN);
    break;
  default:
    abort();
    break;
  };

  /* The following can only succeed if the host is rdma-capable */
  string xio_uri = "rdma://";
  xio_uri += host;
  if (want_port) {
    xio_uri += ":";
    xio_uri += boost::lexical_cast<std::string>(addr.get_port());
  }

  return xio_uri;
} /* xio_uri_from_entity */

/* XioMessenger */
XioMessenger::XioMessenger(CephContext *cct, entity_name_t name,
			   string mname, uint64_t nonce, int nportals)
  : SimplePolicyMessenger(cct, name, mname, nonce),
    conns_lock("XioMessenger::conns_lock"),
    portals(this, nportals),
    port_shift(0)
{
  /* package init */
  if (! initialized.read()) {

    mtx.Lock();
    if (! initialized.read()) {

      xio_init();

      unsigned xopt;

#if 0
      xopt = XIO_LOG_LEVEL_TRACE;
      xio_set_opt(NULL, XIO_OPTLEVEL_ACCELIO, XIO_OPTNAME_LOG_LEVEL,
		  &xopt, sizeof(unsigned));
#endif
      xopt = 1;
      xio_set_opt(NULL, XIO_OPTLEVEL_ACCELIO, XIO_OPTNAME_DISABLE_HUGETBL,
		  &xopt, sizeof(unsigned));

      /* initialize ops singleton */
      xio_msgr_ops.on_session_event = on_session_event;
      xio_msgr_ops.on_new_session = on_new_session;
      xio_msgr_ops.on_session_established = NULL;
      xio_msgr_ops.on_msg_send_complete	= on_msg_send_complete;
      xio_msgr_ops.on_msg = on_msg;
      xio_msgr_ops.on_msg_delivered = on_msg_delivered;
      xio_msgr_ops.on_msg_error = on_msg_error;
      xio_msgr_ops.on_cancel = on_cancel;
      xio_msgr_ops.on_cancel_request = on_cancel_request;
      xio_msgr_ops.assign_data_in_buf = NULL;

      /* mark initialized */
      initialized.set(1);
    }
    mtx.Unlock();
  }

  /* update class instance count */
  nInstances.inc();
  
} /* ctor */

int XioMessenger::new_session(struct xio_session *session,
			      struct xio_new_session_req *req,
			      void *cb_user_context)
{
  return portals.accept(session, req, cb_user_context);
} /* new_session */

int XioMessenger::session_event(struct xio_session *session,
				struct xio_session_event_data *event_data,
				void *cb_user_context)
{
  XioConnection *xcon;

  switch (event_data->event) {
  case XIO_SESSION_NEW_CONNECTION_EVENT:
  {
    xcon = new XioConnection(this, XioConnection::PASSIVE, entity_inst_t());
    xcon->session = session;

    struct xio_connection *conn = event_data->conn;
    struct xio_context *ctx = xio_get_connection_context(conn);
    xcon->conn = conn;

    struct xio_context_params *ctx_params =
      xio_get_context_params(ctx, NULL /* XXX fixme */);
    xcon->portal = static_cast<XioPortal*>(ctx_params->user_context);

    struct xio_connection_params conn_params;
    conn_params.user_context = xcon;
    xio_set_connection_params(event_data->conn, &conn_params);

    printf("new connection session %p xcon %p\n", session, xcon);
  }
  break;
  case XIO_SESSION_CONNECTION_CLOSED_EVENT: /* orderly discon */
  case XIO_SESSION_CONNECTION_DISCONNECTED_EVENT: /* unexpected discon */
    printf("xio client disconnection %p\n", event_data->conn_user_context);
    /* clean up mapped connections */
    xcon = static_cast<XioConnection*>(event_data->conn_user_context);
    {
      XioConnection::EntitySet::iterator conn_iter =
	conns_entity_map.find(xcon->peer, XioConnection::EntityComp());
      if (conn_iter != conns_entity_map.end()) {
	XioConnection *xcon2 = &(*conn_iter);
	if (xcon == xcon2) {
	  conns_entity_map.erase(conn_iter);
	}
      }
      /* XXX it could make sense to track ephemeral connections, but
       * we don't currently, so if conn_iter points to nothing or to
       * an address other than xcon, there's nothing to clean up */
    }
    xcon->conn = NULL;
    /* XXX remove from ephemeral_conns list? */
    xcon->put();
    break;
  case XIO_SESSION_TEARDOWN_EVENT:
    printf("xio_session_teardown %p\n", session);
    xio_session_destroy(session);
    break;
  default:
    break;
  };

  return 0;
}

int XioMessenger::bind(const entity_addr_t& addr)
{
  string base_uri = xio_uri_from_entity(addr, false /* want_port */);

    printf("XioMessenger %p bind: xio_uri %s:%d\n",
	   this, base_uri.c_str(), addr.get_port());

  return portals.bind(&xio_msgr_ops, base_uri, addr.get_port());
} /* bind */

int XioMessenger::start()
{
  portals.start();
  started = true;
  return 0;
}

void XioMessenger::wait()
{
  portals.join();
} /* wait */

int XioMessenger::send_message(Message *m, const entity_inst_t& dest)
{
  ConnectionRef conn = get_connection(dest);
  if (conn)
    return send_message(m, conn.get() /* intrusive_pointer */);
  else
    return EINVAL;
} /* send_message(Message *, const entity_inst_t&) */

int XioMessenger::send_message(Message *m, Connection *con)
{
  XioConnection *xcon = static_cast<XioConnection*>(con);
  int code = 0;

  m->set_seq(0); /* XIO handles seq */
  m->encode(xcon->get_features(), !this->cct->_conf->ms_nocrc);

  /* XXX placement new */
  XioMsg *xmsg = (XioMsg*) calloc(1, sizeof(XioMsg));
  new (xmsg) XioMsg(m, xcon);

  cout << "\nsend_message " << m << " new XioMsg " << xmsg
       << " req_0 " << &xmsg->req_0 << std::endl;

  buffer::list blist;
  struct xio_msg *req = &xmsg->req_0;
  struct xio_iovec_ex *msg_iov = req->out.data_iov, *iov;
  int ex_cnt;

  buffer::list &payload = m->get_payload();
  buffer::list &middle = m->get_middle();
  buffer::list &data = m->get_data();

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
  xmsg->hdr.msg_cnt = 1 + ex_cnt;

  if (ex_cnt > 0) {
    xmsg->req_arr =
      (struct xio_msg *) calloc(ex_cnt, sizeof(struct xio_msg));
  }

  /* do the invariant part */
  int msg_off = 0;
  int req_off = -1; /* most often, not used */

  list<bufferptr>::const_iterator pb;
  for (pb = buffers.begin(); pb != buffers.end(); ++pb) {

    /* assign buffer */
    iov = &msg_iov[msg_off];
    iov->iov_base = (void *) pb->c_str(); // is this efficient?
    iov->iov_len = pb->length();

    /* track iovlen */
    req->out.data_iovlen = msg_off+1;

#if 0
    /* register it */
    iov->mr = xio_reg_mr(iov->iov_base, iov->iov_len);
    if (! iov->mr)
      abort();
#endif

    printf("send req %p data off %d iov_base %p iov_len %d\n",
	   req, msg_off,
	   iov->iov_base,
	   (int) iov->iov_len);

    /* advance iov(s) */
    if (++msg_off >= XIO_MAX_IOV) {
      if (++req_off < ex_cnt) {
	/* next record */
	req->out.data_iovlen = XIO_MAX_IOV;
	req->more_in_batch++;
	/* XXX chain it */
	req = &xmsg->req_arr[req_off];
	req->user_context = xmsg->get();
	msg_iov = req->out.data_iov;
	msg_off = 0;
      }
    }
  }

  /* fixup first msg */
  req = &xmsg->req_0;

  void print_xio_msg_hdr(XioMsgHdr &hdr);
  print_xio_msg_hdr(xmsg->hdr);

  void print_ceph_msg(Message *m);
  print_ceph_msg(m);

  const std::list<buffer::ptr>& header = xmsg->hdr.get_bl().buffers();
  assert(header.size() == 1); /* XXX */
  pb = header.begin();
  req->out.header.iov_base = (char*) pb->c_str();
  req->out.header.iov_len = pb->length();

  /* deliver via xio, preserve ordering */
  struct xio_msg *head = &xmsg->req_0;
  if (xmsg->hdr.msg_cnt > 1) {
    struct xio_msg *tail = head;
    for (req_off = 1; req_off < ex_cnt; ++req_off) {
      req = &xmsg->req_arr[req_off];
      tail->next = req;
      tail = req;
     }
  }
  xcon->portal->enqueue_for_send(xmsg);

  return code;
} /* send_message(Message *, Connection *) */

ConnectionRef XioMessenger::get_connection(const entity_inst_t& dest)
{
  entity_inst_t _dest = dest;
  if (port_shift) {
    _dest.addr.set_port(
      _dest.addr.get_port() + port_shift);
  }
  XioConnection::EntitySet::iterator conn_iter =
    conns_entity_map.find(_dest, XioConnection::EntityComp());
  if (conn_iter != conns_entity_map.end())
    return static_cast<Connection*>(&(*conn_iter));
  else {
    string xio_uri = xio_uri_from_entity(_dest.addr, true /* want_port */);

    printf("XioMessenger %p get_connection: xio_uri %s\n",
	   this, xio_uri.c_str());

    /* XXX client session attributes */
    struct xio_session_attr attr = {
      &xio_msgr_ops,
      NULL, /* XXX server private data? */
      0     /* XXX? */
    };

    XioConnection *conn = new XioConnection(this, XioConnection::ACTIVE,
					    _dest);

    conn->session = xio_session_create(XIO_SESSION_REQ, &attr, xio_uri.c_str(),
				       0, 0, this);
    if (! conn->session) {
      delete conn;
      return NULL;
    }

    /* this should cause callbacks with user context of conn, but
     * we can always set it explicitly */
    conn->conn = xio_connect(conn->session, this->portals.get_portal0()->ctx,
			     0, NULL, conn);

    /* conn has nref == 1 */
    conns_entity_map.insert(*conn);

    return conn;
  }
} /* get_connection */

ConnectionRef XioMessenger::get_loopback_connection()
{
  abort();
  return NULL;
} /* get_loopback_connection */

XioMessenger::~XioMessenger()
{
  nInstances.dec();
} /* dtor */
