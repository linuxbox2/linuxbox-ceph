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

class XioMessenger;

class XioPortal : public Thread
{
private:
  XioMessenger *msgr;
  struct xio_context *ctx;
  struct xio_server *server;
  void *ev_loop;
  string xio_uri;

  friend class XioPortals;
  friend class XioMessenger;

public:
  XioPortal(XioMessenger *_msgr) : msgr(_msgr), ctx(NULL), server(NULL),
				   xio_uri("")
    {
      ev_loop = xio_ev_loop_init();
      ctx = xio_ctx_open(NULL, ev_loop, 0);
    }

  int bind(struct xio_session_ops *ops, const string &_uri)
    {
      xio_uri = _uri;
      server = xio_bind(ctx, ops, xio_uri.c_str(), NULL, 0, msgr);
      return (!!server);
    }

  void *entry()
    {
      xio_ev_loop_run(ev_loop);
      if (server) {
	xio_unbind(server);
      }
      xio_ctx_close(ctx);
      xio_ev_loop_destroy(&ev_loop);
      return NULL;
    }

  ~XioPortal()
    {}
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
	p_vec[(p_ix-1)] = (char*) portal->xio_uri.c_str();
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
