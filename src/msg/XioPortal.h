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

#ifndef CACHE_LINE_SIZE
#define CACHE_LINE_SIZE 64 /* XXX arch-specific define */
#endif
#define CACHE_PAD(_n) char __pad ## _n [CACHE_LINE_SIZE]

class XioMessenger;

class XioPortal : public Thread
{
private:

  struct SubmitQueue
  {
    const static int nlanes = 7;

    struct Lane
    {
      uint32_t size;
      XioMsg::Queue q;
      pthread_spinlock_t sp;
      CACHE_PAD(0);
    };

    Lane qlane[nlanes];

    SubmitQueue()
      {
	int ix;
	Lane* lane;

	for (ix = 0; ix < nlanes; ++ix) {
	  lane = &qlane[ix];
	  pthread_spin_init(&lane->sp, PTHREAD_PROCESS_PRIVATE);
	  lane->size = 0;
	}
      }

    inline Lane* get_lane()
      {
	return &qlane[((uint64_t) pthread_self()) % nlanes];
      }

    void enq(XioMsg* xmsg)
      {
	Lane* lane = get_lane();
	pthread_spin_lock(&lane->sp);
	lane->q.push_back(*xmsg);
	++(lane->size);
	pthread_spin_unlock(&lane->sp);
      }

    void deq(XioMsg::Queue &send_q)
      {
	int ix;
	Lane* lane;

	for (ix = 0; ix < nlanes; ++ix) {
	  lane = &qlane[ix];
	  pthread_spin_lock(&lane->sp);
	  if (lane->size > 0) {
	    XioMsg::Queue::const_iterator i1 = send_q.end();
	    send_q.splice(i1, lane->q);
	    lane->size = 0;
	  }
	  pthread_spin_unlock(&lane->sp);
	}
      }

  };

  XioMessenger *msgr;
  struct xio_context *ctx;
  struct xio_server *server;
  SubmitQueue submit_q;
  pthread_spinlock_t sp;
  void *ev_loop;
  string xio_uri;
  char *portal_id;
  bool _shutdown;
  bool drained;
  uint32_t magic;

  friend class XioPortals;
  friend class XioMessenger;

public:
  XioPortal(XioMessenger *_msgr) :
  msgr(_msgr), ctx(NULL), server(NULL), submit_q(), xio_uri(""),
  portal_id(NULL), _shutdown(false), drained(false),
  magic(0)
    {
      pthread_spin_init(&sp, PTHREAD_PROCESS_PRIVATE);

      /* a portal is an xio_context and event loop */
      ctx = xio_context_create(NULL, 0);

      /* associate this XioPortal object with the xio_context
       * handle */
      struct xio_context_params params;
      params.user_context = this;
      xio_context_set_params(ctx, &params);

      if (magic & (MSG_MAGIC_XIO)) {
	printf("XioPortal %p created ev_loop %p ctx %p\n",
	       this, ev_loop, ctx);
      }
    }

  int bind(struct xio_session_ops *ops, const string &_uri)
    {
      xio_uri = _uri;
      portal_id = strdup(xio_uri.c_str());
      server = xio_bind(ctx, ops, portal_id, NULL, 0, msgr);
      if (magic & (MSG_MAGIC_XIO)) {
	printf("xio_bind: portal %p %s returned server %p\n",
	       this, xio_uri.c_str(), server);
      }
      return (!!server);
    }

  void enqueue_for_send(XioMsg *xmsg)
    {
      if (! _shutdown) {
	submit_q.enq(xmsg);
	xio_context_stop_loop(ctx, false);
      }
    }

  void *entry()
    {
      int ix, size;
      XioMsg::Queue send_q;
      XioMsg::Queue::iterator q_iter;
      XioConnection *xcon;
      struct xio_msg *req;
      uint64_t timestamp;
      XioMsg *xmsg;

      do {
	submit_q.deq(send_q);
	size = send_q.size();

	/* shutdown() barrier */
	pthread_spin_lock(&sp);

	if (_shutdown) {
	  drained = true;
	}

	if (size > 0) {
	  /* XXX look out, no flow control */
	  timestamp = 0;
	  for (ix = 0; ix < size; ++ix) {
	    q_iter = send_q.begin();
	    xmsg = &(*q_iter);
	    send_q.erase(q_iter);
	    xcon = xmsg->xcon;
	    req = &xmsg->req_0;

	    /* handle response traffic */
	    if (! req->request) {
	      (void) xio_send_request(xcon->conn, req);
	      timestamp = req->timestamp;
	    } else {
	      (void) xio_send_response(req);
	    }

	    if (timestamp)
	      xcon->send.set(timestamp);
	  }
	}

	pthread_spin_unlock(&sp);

	xio_context_run_loop(ctx, 3000);

      } while ((!_shutdown) || (!drained));

      /* shutting down */
      if (server) {
	xio_unbind(server);
      }
      xio_context_destroy(ctx);
      return NULL;
    }

  void shutdown()
    {
      pthread_spin_lock(&sp);
      xio_context_stop_loop(ctx, false);
      _shutdown = true;
      pthread_spin_unlock(&sp);
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

  void shutdown()
    {
      XioPortal *portal;
      int nportals = portals.size();
      for (int p_ix = 0; p_ix < nportals; ++p_ix) {
	portal = portals[p_ix];
	portal->shutdown();
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
