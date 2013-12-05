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

#include "XioMsg.h"
#include "XioMessenger.h"
#include "common/Mutex.h"

Mutex mtx("XioMessenger Package Lock");
atomic_t initialized;

atomic_t XioMessenger::nInstances;
void* XioMessenger::ev_loop;

static struct xio_session_ops xio_msgr_ops;

extern "C" {

  static int on_session_event(struct xio_session *session,
			      struct xio_session_event_data *event_data,
			      void *cb_user_context);

  static int on_new_session(struct xio_session *session,
			    struct xio_new_session_req *req,
			    void *cb_user_context);
  
  static int on_request(struct xio_session *session,
			struct xio_msg *req,
			int more_in_batch,
			void *cb_user_context);

} /* extern "C" */

void* XioMessenger::EventThread::entry(void)
{
  /* we always need an event loop */
  xio_ev_loop_run(msgr->ev_loop);

  /* unbind only if we actually did a bind */
  if (msgr->bound) {
    xio_unbind(msgr->server);
  }

  return NULL;
}

XioMessenger::XioMessenger(CephContext *cct, entity_name_t name,
			   string mname, uint64_t nonce)
  : SimplePolicyMessenger(cct, name, mname, nonce),
    ctx(0),
    server(0),
    conns_lock("XioMessenger::conns_lock"),
    bound(false),
    event_thread(this)
{
  /* package init */
  if (! initialized.read()) {

    mtx.Lock();
    if (! initialized.read()) {

      xio_init();

      unsigned xopt;

      xopt = XIO_LOG_LEVEL_TRACE;
      xio_set_opt(NULL, XIO_OPTLEVEL_ACCELIO, XIO_OPTNAME_LOG_LEVEL,
		  &xopt, sizeof(unsigned));

      xopt = 1;
      xio_set_opt(NULL, XIO_OPTLEVEL_ACCELIO, XIO_OPTNAME_DISABLE_HUGETBL,
		  &xopt, sizeof(unsigned));

      /* initialize ops singleton */
      xio_msgr_ops.on_session_event = on_session_event;
      xio_msgr_ops.on_session_established = NULL;
      xio_msgr_ops.on_new_session = on_new_session;
      xio_msgr_ops.on_msg_send_complete	= NULL;
      xio_msgr_ops.on_msg = on_request;
      xio_msgr_ops.on_msg_error = NULL;

      ev_loop = xio_ev_loop_init();

      ctx = xio_ctx_open(NULL, ev_loop, 0);

      /* mark initialized */
      initialized.set(1);
    }
    mtx.Unlock();
  }

  /* update class instance count */
  nInstances.inc();

} /* ctor */

static string xio_uri_from_entity(const entity_addr_t& addr)
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
  xio_uri += ":";
  xio_uri += boost::lexical_cast<std::string>(addr.get_port());

  return xio_uri;
}

int XioMessenger::bind(const entity_addr_t& addr)
{
  if (bound)
    return (EINVAL);

  string xio_uri = xio_uri_from_entity(addr);

  printf("XioMessenger::bind %s %p\n",
	 xio_uri.c_str(),
	 this);

  server = xio_bind(ctx, &xio_msgr_ops, xio_uri.c_str(), NULL, 0, this);
  if (!server)
    return EINVAL;
  else
    bound = true;  

  return 0;
} /* bind */

int XioMessenger::start()
{
  started = true;
  event_thread.create();
  return 0;
}

void XioMessenger::wait()
{
  event_thread.join();
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

  m->set_connection(con);
  m->set_seq(0); /* XIO handles seq */
  m->encode(xcon->get_features(), !this->cct->_conf->ms_nocrc);

  XioMsg *xmsg = new XioMsg(m);

  bufferlist blist;
  struct xio_msg *req = &xmsg->req_0;
  struct xio_iovec_ex *msg_iov = req->out.data_iov, *iov;

  bufferlist &payload = m->get_payload();
  bufferlist &middle = m->get_middle();
  bufferlist &data = m->get_data();

  cout << "payload: " << payload.buffers().size() <<
    " middle: " << middle.buffers().size() <<
    " data: " << data.buffers().size() <<
    std::endl;

  blist.append(payload);
  blist.append(middle);
  blist.append(data);

  const std::list<buffer::ptr>& buffers = blist.buffers();
  xmsg->nbuffers = buffers.size();
  xmsg->cnt = (3 + xmsg->nbuffers) / XIO_MAX_IOV;

  if (xmsg->cnt) {
    xmsg->req_arr =
      (struct xio_msg *) calloc(xmsg->cnt, sizeof(struct xio_msg));
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

    /* advance iov(s) */
    if (++msg_off >= XIO_MAX_IOV) {
      req->out.data_iovlen = msg_off;
      if (++req_off < xmsg->cnt) {
	/* next record */
	req->out.data_iovlen = XIO_MAX_IOV;
	req->more_in_batch++;
	req = &xmsg->req_arr[req_off];
	req->user_context = xmsg;
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
  if (! xmsg->cnt)
    xio_send_request(xcon->conn, req);
  else {
    pthread_spin_lock(&xcon->sp);
    xio_send_request(xcon->conn, &xmsg->req_0);
    for (req_off = 0; req_off < xmsg->cnt; ++req_off) {
      req = &xmsg->req_arr[req_off];
      xio_send_request(xcon->conn, req);
    }
    pthread_spin_unlock(&xcon->sp);
  }

  return 0;
} /* send_message(Message *, Connection *) */

ConnectionRef XioMessenger::get_connection(const entity_inst_t& dest)
{
  XioConnection::EntitySet::iterator conn_iter =
    conns_entity_map.find(dest, XioConnection::EntityComp());
  if (conn_iter != conns_entity_map.end())
    return static_cast<Connection*>(&(*conn_iter));
  else {
    string xio_uri = xio_uri_from_entity(dest.addr);

    /* XXX client session attributes */
    struct xio_session_attr attr = {
      &xio_msgr_ops,
      NULL, /* XXX server private data? */
      0     /* XXX? */
    };

    XioConnection *conn = new XioConnection(this, XioConnection::ACTIVE,
					    dest);

    conn->session = xio_session_open(XIO_SESSION_REQ, &attr, xio_uri.c_str(),
				     0, 0, this);
    if (! conn->session) {
      delete conn;
      return NULL;
    }

    /* this should cause callbacks with user context of conn, but
     * we can always set it explicitly */
    conn->conn = xio_connect(conn->session, ctx, 0, NULL, conn);

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
  if (nInstances.dec() == 0) {
    xio_ctx_close(ctx);
    xio_ev_loop_destroy(&ev_loop);
  }
} /* dtor */

// xio hooks
extern "C" {

  int xio_conn_get_src_addr(struct xio_conn *conn,
			    struct sockaddr_storage *sa, socklen_t len);

  static int on_session_event(struct xio_session *session,
			      struct xio_session_event_data *event_data,
			      void *cb_user_context)
  {
    XioMessenger *m = static_cast<XioMessenger*>(cb_user_context);
    XioConnection *xcon;

    printf("session event: %s. reason: %s\n",
	   xio_session_event_str(event_data->event),
	   xio_strerror(event_data->reason));

    switch (event_data->event) {
    case XIO_SESSION_NEW_CONNECTION_EVENT:
    {
      struct xio_connection_params params;
      xcon = new XioConnection(m, XioConnection::PASSIVE, entity_inst_t());
      /* XXX the only member at present */
      params.user_context = xcon;
      xio_set_connection_params(event_data->conn, &params);
      printf("new connection session %p xcon %p\n", session, xcon);
    }
      break;
    case XIO_SESSION_CONNECTION_CLOSED_EVENT:
      /* XXXX need to convert session to connection, remove from
	 conn_map, and release */
      xcon = static_cast<XioConnection*>(event_data->conn_user_context);
      /* XXX remove from ephemeral_conns list? */
      xcon->put();
      break;
    case XIO_SESSION_TEARDOWN_EVENT:
      xio_session_close(session);
      break;
    default:
      break;
    };

    return 0;
  }

  static int on_new_session(struct xio_session *session,
			    struct xio_new_session_req *req,
			    void *cb_user_context)
  {
 
    printf("new session %p user_context %p\n", session, cb_user_context);

    xio_accept(session, NULL, 0, NULL, 0);

    return 0;
  }

  static int on_request(struct xio_session *session,
			struct xio_msg *req,
			int more_in_batch,
			void *cb_user_context)
  {
    XioConnection *xcon =
      static_cast<XioConnection*>(cb_user_context);

    printf("new request session %p xcon %p\n", session, xcon);

    return xcon->on_request(session, req, more_in_batch,
			    cb_user_context);
  }  

} /* extern "C" */

