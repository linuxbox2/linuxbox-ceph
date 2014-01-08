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

#ifndef XIO_PORTAL_H
#define XIO_PORTAL_H

#include "SimplePolicyMessenger.h"
extern "C" {
#include "libxio.h"
}
#include "XioConnection.h"
#include "XioMsg.h"
#include <boost/lexical_cast.hpp>

class XioMessenger;

class XioPortal : public Thread
{
private:
  XioMessenger *msgr;
  struct xio_context *ctx;
  struct xio_server *server;
  list<XioMsg*> submit_queue; /* XXX replace with boost::intrusive */
  pthread_spinlock_t sp;
  void *ev_loop;
  string xio_uri;
  char *portal_id;
  bool shutdown;

  friend class XioPortals;
  friend class XioMessenger;

public:
  XioPortal(XioMessenger *_msgr) :
  msgr(_msgr), ctx(NULL), server(NULL), xio_uri(""),
  portal_id(NULL)
    {
      pthread_spin_init(&sp, PTHREAD_PROCESS_PRIVATE);

      /* a portal is an xio_context and event loop */
      ev_loop = xio_ev_loop_create();
      ctx = xio_ctx_create(NULL, ev_loop, 0);

      /* associate this XioPortal object with the xio_context
       * handle */
      struct xio_context_params params =
	{
	  .user_context = this
	};
      xio_set_context_params(ctx, &params);

      printf("XioPortal %p created ev_loop %p ctx %p\n",
	     this, ev_loop, ctx);
    }

  int bind(struct xio_session_ops *ops, const string &_uri)
    {
      xio_uri = _uri;
      portal_id = strdup(xio_uri.c_str());
      server = xio_bind(ctx, ops, portal_id, NULL, 0, msgr);
      printf("xio_bind: portal %p %s returned server %p\n",
	     this, xio_uri.c_str(), server);
      return (!!server);
    }

  void enqueue_for_send(XioMsg *xmsg)
    {
      pthread_spin_lock(&sp);
      submit_queue.push_back(xmsg);
      xio_ev_loop_stop(ev_loop, false);
      pthread_spin_unlock(&sp);
    }

  void *entry()
    {
      int ix, size;
      list<XioMsg*> send_queue; /* XXX see above */
      XioConnection *xcon;
      struct xio_msg *req;
      XioMsg *xmsg;

      while (! shutdown) {
	/* barrier */
	pthread_spin_lock(&sp);
	size = submit_queue.size();
	if (size > 0) {
	  send_queue.swap(submit_queue);
	  /* submit and send may run in parallel */
	  pthread_spin_unlock(&sp);
	  /* XXX look out, no flow control */
	  for (ix = 0; ix < size; ++ix) {
	    xmsg = send_queue.front();
	    send_queue.pop_front();
	    xcon = xmsg->xcon;
	    req = &xmsg->req_0;
	    (void) xio_send_request(xcon->conn, req);
	    /* it's now possible to use sn and timestamp */
	    xcon->send.set(req->timestamp);
	  }
	} else
	  pthread_spin_unlock(&sp);
	xio_ev_loop_run_timeout(ev_loop, 300);
      }

      /* shutting down */
      if (server) {
	xio_unbind(server);
      }
      xio_ctx_destroy(ctx);
      xio_ev_loop_destroy(&ev_loop);
      return NULL;
    }

  ~XioPortal()
    {
      free(portal_id);
    }
};

class XioPortals
{
private:
  vector<XioPortal*> portals;
  char **p_vec;
  int n;

public:
  XioPortals(XioMessenger *msgr, int _n) : p_vec(NULL), n(_n)
    {
      /* n session portals, plus 1 to accept new sessions */
      int ix, np = n+1;
      for (ix = 0; ix < np; ++ix) {
	portals.push_back(new XioPortal(msgr));
      }
    }

  vector<XioPortal*>& get() { return portals; }

  const char **get_vec()
    {
      return (const char **) p_vec;
    }

  int get_portals_len()
    {
      return n;
    }

  XioPortal* get_portal0()
    {
      return portals[0];
    }

  int bind(struct xio_session_ops *ops, const string& base_uri,
	   const int base_port)
    {
      /* a server needs at least 1 portal */
      if (n < 1)
	return EINVAL;

      XioPortal *portal;
      int bind_size = portals.size();

      /* bind a consecutive range of ports */
      for (int bind_ix = 1, bind_port = base_port;
	   bind_ix < bind_size; ++bind_ix, ++bind_port) {
	string xio_uri = base_uri;
	xio_uri += ":";
	xio_uri += boost::lexical_cast<std::string>(bind_port);
	portal = portals[bind_ix];
	(void) portal->bind(ops, xio_uri);
      }

      return 0;
    }

    int accept(struct xio_session *session,
		 struct xio_new_session_req *req,
		 void *cb_user_context)
    {
      const char **portals_vec = get_vec()+1;
      int portals_len = get_portals_len()-1;

      printf("portals_vec %p len %d\n", portals_vec, portals_len);

      return xio_accept(session,
			portals_vec,
			portals_len,
			NULL, 0);
    }

  void start()
    {
      XioPortal *portal;
      int p_ix, nportals = portals.size();

      /* portal_0 is the new-session handler, portal_1+ terminate
       * active sessions */

      p_vec = new char*[(nportals-1)];
      for (p_ix = 1; p_ix < nportals; ++p_ix) {
	portal = portals[p_ix];
	/* shift left */
	p_vec[(p_ix-1)] = (char*) /* portal->xio_uri.c_str() */
	  portal->portal_id;
      }

      for (p_ix = 0; p_ix < nportals; ++p_ix) {
	portal = portals[p_ix];
	portal->create();
      }
    }

  void join()
    {
      XioPortal *portal;
      int nportals = portals.size();
      for (int p_ix = 0; p_ix < nportals; ++p_ix) {
	portal = portals[p_ix];
	portal->join();
      }
    }

  ~XioPortals()
    {
      int nportals = portals.size();
      for (int ix = 0; ix < nportals; ++ix) {
	delete(*(portals.begin()));
      }
      if (p_vec) {
	delete[] p_vec;
      }
    }
};

#endif /* XIO_PORTAL_H */
