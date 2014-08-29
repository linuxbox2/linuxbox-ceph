// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 CohortFS, LLC
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "XioPortal.h"
#include <stdio.h>

#define dout_subsys ceph_subsys_xio

int XioPortal::bind(struct xio_session_ops *ops, const string &base_uri,
		    uint16_t port, uint16_t *assigned_port)
{
  // format uri
  char buf[40];
  xio_uri = base_uri;
  xio_uri += ":";
  sprintf(buf, "%d", port);
  xio_uri += buf;

  uint16_t assigned;
  server = xio_bind(ctx, ops, xio_uri.c_str(), &assigned, 0, msgr);
  if (server == NULL) {
    int err = xio_errno();
    derr << "xp::bind: portal " << xio_uri << " failed to bind: "
	 << xio_strerror(err) << dendl;
    return err;
  }

  // update uri if port changed
  if (port != assigned) {
    xio_uri = base_uri;
    xio_uri += ":";
    sprintf(buf, "%d", assigned);
    xio_uri += buf;
  }

  portal_id = const_cast<char*>(xio_uri.c_str());
  if (assigned_port)
    *assigned_port = assigned;
  dout(20) << "xio_bind: portal " << xio_uri << " returned server "
	   << server << dendl;
  return 0;
}

int XioPortals::bind(struct xio_session_ops *ops, const string& base_uri,
		     uint16_t port, uint16_t *port0)
{
  /* a server needs at least 1 portal */
  if (n < 1)
    return EINVAL;

  /* bind the portals */
  for (size_t i = 0; i < portals.size(); i++) {

    uint16_t result_port;
    int r = portals[i]->bind(ops, base_uri, port, &result_port);
    if (r != 0)
      return r;

    dout(5) << "xp::bind: portal " << i << " bind OK: "
      << portals[i]->xio_uri << dendl;

    if (i == 0 && port0 != NULL)
      *port0 = result_port;
    port = 0; // use port 0 for all subsequent portals
  }

  return 0;
}
