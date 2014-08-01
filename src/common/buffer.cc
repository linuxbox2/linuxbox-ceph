// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "armor.h"
#include "common/environment.h"
#include "common/errno.h"
#include "common/safe_io.h"
#include "common/simple_spin.h"
#include "common/strtol.h"
#include "include/atomic.h"
#include "common/Mutex.h"
#include "include/types.h"
#include "include/compat.h"
#if defined(HAVE_XIO)
#include "msg/XioMsg.h"
#endif

#include <errno.h>
#include <fstream>
#include <sstream>
#include <sys/uio.h>
#include <limits.h>

namespace ceph {

  /* debugging XXX */
  atomic_t buffer_c_str_accesses;
  atomic_t buffer_cached_crc;
  atomic_t buffer_cached_crc_adjusted;

  bool buffer_track_alloc = get_env_bool("CEPH_BUFFER_TRACK");
  bool buffer_track_crc = get_env_bool("CEPH_BUFFER_TRACK");
  bool buffer_track_c_str = get_env_bool("CEPH_BUFFER_TRACK");

  /* re-open buffer namespace */
  namespace buffer {

    atomic_t buffer_total_alloc;

#ifdef BUFFER_DEBUG
    uint32_t simple_spinlock_t buffer_debug_lock = SIMPLE_SPINLOCK_INITIALIZER;
#endif

    void inc_total_alloc(unsigned len) {
      if (buffer_track_alloc)
	buffer_total_alloc.add(len);
    }

    void dec_total_alloc(unsigned len) {
      if (buffer_track_alloc)
	buffer_total_alloc.sub(len);
    }

    int get_total_alloc() {
      return buffer_total_alloc.read();
    }

    void track_cached_crc(bool b) {
      buffer_track_crc = b;
    }

    int get_cached_crc() {
      return buffer_cached_crc.read();
    }

    int get_cached_crc_adjusted() {
      return buffer_cached_crc_adjusted.read();
    }

    void track_c_str(bool b) {
      buffer_track_c_str = b;
    }

    int get_c_str_accesses() {
      return buffer_c_str_accesses.read();
    }

    atomic_t buffer_max_pipe_size;
    int update_max_pipe_size() {
#ifdef CEPH_HAVE_SETPIPE_SZ
      char buf[32];
      int r;
      std::string err;
      struct stat stat_result;
      if (::stat("/proc/sys/fs/pipe-max-size", &stat_result) == -1)
	return -errno;
      r = safe_read_file("/proc/sys/fs/", "pipe-max-size",
			 buf, sizeof(buf) - 1);
      if (r < 0)
	return r;
      buf[r] = '\0';
      size_t size = strict_strtol(buf, 10, &err);
      if (!err.empty())
	return -EIO;
      buffer_max_pipe_size.set(size);
#endif
      return 0;
    }

    size_t get_max_pipe_size() {
#ifdef CEPH_HAVE_SETPIPE_SZ
      size_t size = buffer_max_pipe_size.read();
      if (size)
	return size;
      if (update_max_pipe_size() == 0)
	return buffer_max_pipe_size.read();
#endif
      // this is the max size hardcoded in linux before 2.6.35
      return 65536;
    }

  } /* namespace buffer */

} /* namespace ceph */
