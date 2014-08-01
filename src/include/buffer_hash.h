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

#ifndef CEPH_BUFFER_HASH_H
#define CEPH_BUFFER_HASH_H

#include "buffer_int.h"
#include "buffer_list.h"

namespace ceph {

  /* re-open namespace buffer */
  namespace buffer {

    /*
     * efficient hash of one or more bufferlists
     */

    class hash {
      uint32_t crc;

    public:
      hash() : crc(0) { }
      hash(uint32_t init) : crc(init) { }

      void update(buffer::list& bl) {
	crc = bl.crc32c(crc);
      }

      uint32_t digest() {
	return crc;
      }
    };

  } /* namespace buffer */

  inline bufferhash& operator<<(bufferhash& l, bufferlist &r) {
    l.update(r);
    return l;
  }

} /* namespace ceph */

#endif /* CEPH_BUFFER_HASH_H */
