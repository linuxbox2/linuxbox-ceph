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

#include "buffer_raw.h"
#include "buffer_ptr.h" // includes buffer_raw.h

#include "include/atomic.h"
#include "msg/XioMsg.h"

namespace ceph {

  /* re-open buffer namespace  */
  namespace buffer {

    class xio_msg_buffer : public raw {
    private:
      XioCompletionHook* m_hook;
    public:
      xio_msg_buffer(XioCompletionHook* _m_hook, const char *d, unsigned l) :
	raw((char*)d, l), m_hook(_m_hook->get()) {}

      static void operator delete(void *p)
	{
	  xio_msg_buffer *buf = static_cast<xio_msg_buffer*>(p);
	  // return hook ref (counts against pool);  it appears illegal
	  // to do this in our dtor, because this fires after that
	  buf->m_hook->put();
	}
      raw* clone_empty() {
	return new raw_char(len);
      }
    };

    class xio_mempool : public raw {
    public:
      struct xio_mempool_obj *mp;
      xio_mempool(struct xio_mempool_obj *_mp, unsigned l) :
	raw((char*)mp->addr, l), mp(_mp)
	{ }
      ~xio_mempool() {}
      raw* clone_empty() {
	return new raw_char(len);
      }
    };

    inline raw* create_msg(
      unsigned len, char *buf, XioCompletionHook *m_hook) {
      XioPool& pool = m_hook->get_pool();
      raw* bp =
	static_cast<raw*>(pool.alloc(sizeof(xio_msg_buffer)));
      new (bp) xio_msg_buffer(m_hook, buf, len);
      return bp;
    }

  inline struct xio_mempool_obj* get_xio_mp(const ptr& bp)
  {
    buffer::xio_mempool *mb =
      dynamic_cast<buffer::xio_mempool*>(bp.get_raw());
    if (mb) {
      return mb->mp;
      }
    return NULL;
  }

  } /* namespace buffer */

} /* namespace ceph */

#endif /* HAVE_XIO */
#endif /* CEPH_BUFFER_RAW_H */
