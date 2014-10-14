// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 * Portions Copyright (C) 2014 CohortFS, LLC
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_BUFFER_XIO_H
#define CEPH_BUFFER_XIO_H

#if defined(HAVE_XIO)

#include "buffer_ptr.h" // includes buffer_raw.h
#include "msg/XioMsg.h"

namespace ceph {

  namespace buffer {

    // pool-allocated raw objects must release their memory in operator delete
    inline void raw::operator delete(void *p) {
      raw *r = static_cast<raw*>(p);
      switch (r->get_type()) {
      case type_xio:
	xio_mempool_free(r->xio.mp);
	break;
	case
	  type_xio_msg:
	  r->xio.hook->put();
	break;
      default:
	break;
      }
    }

    inline raw* raw::create_xio(unsigned len, struct xio_mempool_obj *mp) {
      raw *r = new raw(type_xio, len, static_cast<char*>(mp->addr));
      r->xio.mp = mp;
      return r;
    }

    inline raw* raw::create_xio_msg(unsigned len, char *buf,
				    XioCompletionHook *hook) {
      XioPool& pool = hook->get_pool();
      void *p = pool.alloc(sizeof(raw));
      raw *r = new (p) raw(type_xio_msg, len, buf);
      r->xio.hook = hook;
      return r;
    }

    inline struct xio_mempool_obj* raw::get_xio_mp() const {
      if (get_type() == raw::type_xio)
	return xio.mp;
      return NULL;
    }

  } /* namespace buffer */

} /* namespace ceph */

#endif /* HAVE_XIO */
#endif /* CEPH_BUFFER_RAW_H */
