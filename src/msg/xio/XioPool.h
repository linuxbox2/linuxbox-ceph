// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2014 CohortFS, LLC
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef XIO_POOL_H
#define XIO_POOL_H

extern "C" {
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include "libxio.h"
}
#include <iostream>
#include <vector>
#include "common/likely.h"


class XioPoolStats {
private:
  enum pool_sizes {
    SLAB_64 = 0,
    SLAB_256,
    SLAB_1024,
    SLAB_PAGE,
    SLAB_MAX
  };

  std::vector<uint64_t> ctr_set;

public:
  XioPoolStats() : ctr_set(5, 0) {}

  void dump(const char* tag) {
    std::cout << "\tpool objects:  "
	      << "64: " << ctr_set[SLAB_64] << "  "
	      << "256: " << ctr_set[SLAB_256] << "  "
	      << "1024: " << ctr_set[SLAB_1024] << "  "
	      << "page: " << ctr_set[SLAB_PAGE] << "  "
	      << "max: " << ctr_set[SLAB_MAX] << "  "
	      << "(" << tag << ")"
	      << std::endl;
  }

  void inc(uint64_t size) {
    if (size <= 64) {
      (ctr_set[SLAB_64])++;
      return;
    }
    if (size <= 256) {
      (ctr_set[SLAB_256])++;
      return;
    }
    if (size <= 1024) {
      (ctr_set[SLAB_1024])++;
      return;
    }
    if (size <= 8192) {
      (ctr_set[SLAB_PAGE])++;
      return;
    }
    (ctr_set[SLAB_MAX])++;
  }

  void dec(uint64_t size) {
    if (size <= 64) {
      (ctr_set[SLAB_64])--;
      return;
    }
    if (size <= 256) {
      (ctr_set[SLAB_256])--;
      return;
    }
    if (size <= 1024) {
      (ctr_set[SLAB_1024])--;
      return;
    }
    if (size <= 8192) {
      (ctr_set[SLAB_PAGE])--;
      return;
    }
    (ctr_set[SLAB_MAX])--;
  }
};

extern XioPoolStats xp_stats;

static inline int xpool_alloc(struct xio_mempool *pool, uint64_t size,
			      struct xio_mempool_obj* mp);
static inline void xpool_free(uint64_t size, struct xio_mempool_obj* mp);

class XioPool
{
private:
  struct xio_mempool *handle;

public:
  static bool trace_mempool;
  static bool trace_msgcnt;
  static const int MB = 8;

  struct xio_piece {
    struct xio_mempool_obj mp[1];
    struct xio_piece *next;
    int s;
    char payload[MB];
  } *first;

  XioPool(struct xio_mempool *_handle) :
    handle(_handle), first(0)
    {
    }
  ~XioPool()
    {
      struct xio_piece *p;
      while ((p = first)) {
	first = p->next;
	if (unlikely(trace_mempool)) {
	  memset(p->payload, 0xcf, p->s); // guard bytes
	}
	xpool_free(sizeof(struct xio_piece)+(p->s)+MB, p->mp);
      }
    }
  void *alloc(size_t _s)
    {
	void *r;
	struct xio_mempool_obj mp[1];
	struct xio_piece *x;
	int e = xpool_alloc(handle, (sizeof(struct xio_piece)-MB) + _s, mp);
	if (e) {
	  r = 0;
	} else {
	  x = reinterpret_cast<struct xio_piece *>(mp->addr);
	  *x->mp = *mp;
	  x->next = first;
	  x->s = _s;
	  first = x;
	  r = x->payload;
	}
	return r;
    }
};

static inline int xpool_alloc(struct xio_mempool *pool, uint64_t size,
			      struct xio_mempool_obj* mp)
{
  if (unlikely(XioPool::trace_mempool))
    xp_stats.inc(size);
  return xio_mempool_alloc(pool, size, mp);
}

static inline void xpool_free(uint64_t size, struct xio_mempool_obj* mp)
{
 if (unlikely(XioPool::trace_mempool))
    xp_stats.dec(size);
  xio_mempool_free(mp);
}

#endif /* XIO_POOL_H */
