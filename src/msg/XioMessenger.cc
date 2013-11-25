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

#include "XioMessenger.h"
#include "common/Mutex.h"

extern "C" {

  static int on_session_event(struct xio_session *session,
			      struct xio_session_event_data *event_data,
			      void *cb_user_context)
  {
    printf("session event: %s. reason: %s\n",
	   xio_session_event_str(event_data->event),
	   xio_strerror(event_data->reason));

    xio_session_close(session);

    return 0;
}

  static int on_new_session(struct xio_session *session,
			    struct xio_new_session_req *req,
			    void *cb_user_context)
  {
 
    printf("new session");

    xio_accept(session, NULL, 0, NULL, 0);

    return 0;
  }
  
  static int on_request(struct xio_session *session,
			struct xio_msg *req,
			int more_in_batch,
			void *cb_user_context)
  {

    printf("new request");

    return 0;
  }  

} /* extern "C" */

Mutex mtx("XioMessenger Package Lock");
atomic_t initialized;

atomic_t XioMessenger::nInstances;
void* XioMessenger::ev_loop;

static struct xio_session_ops xio_msgr_ops;

XioMessenger::XioMessenger(CephContext *cct, entity_name_t name,
			   string mname, uint64_t nonce)
  : SimplePolicyMessenger(cct, name, mname, nonce),
    ctx(0),
    server(0),
    conns_lock("XioMessenger::conns_lock"),
    bound(false)
{
  /* package init */
  if (! initialized.read()) {

    mtx.Lock();
    if (! initialized.read()) {

      xio_init();

      /* initialize ops singleton */
      xio_msgr_ops.on_session_event = on_session_event;
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

int XioMessenger::bind(const entity_addr_t& addr)
{
  const char *host = NULL;
  char addr_buf[129];

  if (bound)
    return (EINVAL);
  
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
  string xio_url = "rdma://";
  xio_url += host;
  xio_url += ":";
  xio_url += boost::lexical_cast<std::string>(addr.get_port());

  server = xio_bind(ctx, &xio_msgr_ops, xio_url.c_str(), NULL, 0, this);
  if (!server)
    return EINVAL;
  else
    bound = true;

  return 0;
} /* bind */

void XioMessenger::wait()
{
  if (bound) {
    xio_ev_loop_run(ev_loop);
    xio_unbind(server); /* XXX move? */
  }
} /* wait */

ConnectionRef XioMessenger::get_connection(const entity_inst_t& dest)
{
  XioConnection::EntitySet::iterator conn_iter = conns_entity_map.find(dest, XioEntityComp());
  if (conn_iter != conns_entity_map.end())
    return static_cast<Connection*>(&(*conn_iter));
  else
    return NULL;
}

XioMessenger::~XioMessenger()
{
  if (nInstances.dec() == 0) {
    xio_ctx_close(ctx);
    xio_ev_loop_destroy(&ev_loop);
  }
} /* dtor */



