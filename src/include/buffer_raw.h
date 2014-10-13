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

#ifndef CEPH_BUFFER_RAW_H
#define CEPH_BUFFER_RAW_H

#include <map>

#include <errno.h>
#include <fstream>
#include <sstream>
#include <sys/uio.h>
#include <limits.h>

extern "C" {
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
}

#include "int_types.h"
#include "common/armor.h"
#include "common/environment.h"
#include "common/safe_io.h"
#include "common/simple_spin.h"
#include "common/strtol.h"
#include "common/Mutex.h"
#include "include/compat.h"
#include "include/atomic.h"

#include "buffer_int.h"

#ifdef HAVE_XIO
class XioCompletionHook;
#endif

namespace ceph {

  using std::pair;
  using std::map;

  extern bool buffer_track_alloc;
  extern atomic_t buffer_cached_crc;
  extern atomic_t buffer_cached_crc_adjusted;
  extern bool buffer_track_crc;
  extern atomic_t buffer_c_str_accesses;
  extern bool buffer_track_c_str;

  /* re-open buffer namespace  */
  namespace buffer {

    class raw {
    protected:
      // raw buffer types
      enum {
	type_malloc         = 0,
	type_aligned        = 1,
	type_pipe           = 2,
	type_char           = 3,
	type_static         = 4,
	type_xio            = 5,
	type_xio_msg        = 6,
      };
      static const int type_mask = 0x7; // low 3 bits

      static const int flag_alignment_hack = 0x8;
      static const int flag_pipe_consumed  = 0x10;

      char *data;
      unsigned len;
      atomic_t nref;
      uint8_t flags;

      mutable Mutex crc_lock;
      map<pair<size_t, size_t>, pair<uint32_t, uint32_t> > crc_map;

      int get_type() const { return flags & type_mask; }

      // type_malloc
      void init_malloc() {
	if (!data && len) {
	  data = (char *)::malloc(len);
	  if (!data)
	    throw bad_alloc();
	}
	inc_total_alloc(len);
	bdout << "raw_malloc " << this << " alloc " << (void *)data << " "
	      << len << " " << buffer::get_total_alloc() << bendl;
      }
      void cleanup_malloc() {
	::free(data);
	dec_total_alloc(len);
	bdout << "raw_malloc " << this << " free " << (void *)data << " "
	      << buffer::get_total_alloc() << bendl;
      }

      // type_aligned
#ifndef __CYGWIN__
      void init_aligned() {
#ifdef DARWIN
	data = (char *)::valloc(len);
#else
	int r = ::posix_memalign((void**)(void*)&data, CEPH_PAGE_SIZE, len);
	if (r)
	  throw bad_alloc();
#endif /* DARWIN */
	if (!data)
	  throw bad_alloc();
	inc_total_alloc(len);
	bdout << "raw_posix_aligned " << this << " alloc " << (void *)data
	      << " " << len << " " << buffer::get_total_alloc() << bendl;
      }
      void cleanup_aligned() {
	::free((void*)data);
	dec_total_alloc(len);
	bdout << "raw_posix_aligned " << this << " free " << (void *)data
	      << " " << buffer::get_total_alloc() << bendl;
      }
#else // __CYGWIN__
      void init_aligned() {
	flags |= flag_alignment_hack;
	data = new char[len+CEPH_PAGE_SIZE-1];
	inc_total_alloc(len+CEPH_PAGE_SIZE-1);
	bdout << "hack aligned " << (unsigned)get_data()
	      << " in raw " << (unsigned)data
	      << " off " << off << std::endl;
      }
      void cleanup_aligned() {
	delete[] data;
	dec_total_alloc(len+CEPH_PAGE_SIZE-1);
      }
#endif // __CYGWIN__

      // type_pipe
#ifndef CEPH_HAVE_SPLICE
      void init_pipe() { throw error_code(-ENOTSUP); }
      void cleanup_pipe() {}
      char *copy_pipe() { throw error_code(-ENOTSUP); }
#else // CEPH_HAVE_SPLICE
      int pipefds[2];

      void init_pipe() {
	assert(!data);
	size_t max = get_max_pipe_size();
	if (len > max) {
	  bdout << "raw_pipe: requested length " << len
		<< " > max length " << max << bendl;
	  throw malformed_input("length larger than max pipe size");
	}
	pipefds[0] = -1;
	pipefds[1] = -1;

	int r;
	if (::pipe(pipefds) == -1) {
	  r = -errno;
	  bdout << "raw_pipe: error creating pipe: " << cpp_strerror(r)
		<< bendl;
	  throw error_code(r);
	}

	r = set_nonblocking(pipefds);
	if (r < 0) {
	  bdout << "raw_pipe: error setting nonblocking flag on temp pipe: "
		<< cpp_strerror(r) << bendl;
	  throw error_code(r);
	}

	r = set_pipe_size(pipefds, len);
	if (r < 0) {
	  bdout << "raw_pipe: could not set pipe size" << bendl;
	  // continue, since the pipe should become large enough as needed
	}

	inc_total_alloc(len);
	bdout << "raw_pipe " << this << " alloc " << len << " "
	      << buffer::get_total_alloc() << bendl;
      }
      void cleanup_pipe() {
	if (data)
	  delete data;
	close_pipe(pipefds);
	dec_total_alloc(len);
	bdout << "raw_pipe " << this << " free " << (void *)data << " "
	      << buffer::get_total_alloc() << bendl;
      }

      int set_pipe_size(int *fds, long length) {
#ifdef CEPH_HAVE_SETPIPE_SZ
	if (::fcntl(fds[1], F_SETPIPE_SZ, length) == -1) {
	  int r = -errno;
	  if (r == -EPERM) {
	    // pipe limit must have changed - EPERM means we requested
	    // more than the maximum size as an unprivileged user
	    update_max_pipe_size();
	    throw malformed_input("length larger than new max pipe size");
	  }
	  return r;
	}
#endif // CEPH_HAVE_SETPIPE_SZ
	return 0;
      }

      int set_nonblocking(int *fds) {
	if (::fcntl(fds[0], F_SETFL, O_NONBLOCK) == -1)
	  return -errno;
	if (::fcntl(fds[1], F_SETFL, O_NONBLOCK) == -1)
	  return -errno;
	return 0;
      }

      void close_pipe(int *fds) {
	if (fds[0] >= 0)
	  VOID_TEMP_FAILURE_RETRY(::close(fds[0]));
	if (fds[1] >= 0)
	  VOID_TEMP_FAILURE_RETRY(::close(fds[1]));
      }

      char *copy_pipe() {
	/* preserve original pipe contents by copying into a temporary
	 * pipe before reading.
	 */
	int tmpfd[2];
	int r;

	assert((flags & flag_pipe_consumed) == 0);
	assert(pipefds[0] >= 0);

	if (::pipe(tmpfd) == -1) {
	  r = -errno;
	  bdout << "raw_pipe: error creating temp pipe: " << cpp_strerror(r)
		<< bendl;
	  throw error_code(r);
	}
	r = set_nonblocking(tmpfd);
	if (r < 0) {
	  bdout << "raw_pipe: error setting nonblocking flag on temp pipe: "
		<< cpp_strerror(r) << bendl;
	  throw error_code(r);
	}
	r = set_pipe_size(tmpfd, len);
	if (r < 0) {
	  bdout << "raw_pipe: error setting pipe size on temp pipe: "
		<< cpp_strerror(r) << bendl;
	}
	int flags = SPLICE_F_NONBLOCK;
	if (::tee(pipefds[0], tmpfd[1], len, flags) == -1) {
	  r = errno;
	  bdout << "raw_pipe: error tee'ing into temp pipe: " << cpp_strerror(r)
		<< bendl;
	  close_pipe(tmpfd);
	  throw error_code(r);
	}
	data = (char *)malloc(len);
	if (!data) {
	  close_pipe(tmpfd);
	  throw bad_alloc();
	}
	r = safe_read(tmpfd[0], data, len);
	if (r < (ssize_t)len) {
	  bdout << "raw_pipe: error reading from temp pipe:" << cpp_strerror(r)
		<< bendl;
	  free(data);
	  data = NULL;
	  close_pipe(tmpfd);
	  throw error_code(r);
	}
	close_pipe(tmpfd);
	return data;
      }
#endif // CEPH_HAVE_SPLICE

      // type_char
      void init_char() {
	if (!data && len)
	  data = new char[len];
	inc_total_alloc(len);
	bdout << "raw_char " << this << " alloc " << (void *)data << " " << len
	      << " " << buffer::get_total_alloc() << bendl;
      }
      void cleanup_char() {
	delete[] data;
	dec_total_alloc(len);
	bdout << "raw_char " << this << " free " << (void *)data << " "
	      << buffer::get_total_alloc() << bendl;
      }


      virtual raw* clone_empty() {
	switch (get_type()) {
	case type_pipe:
	  // cloning doesn't make sense for pipe-based buffers,
	  // and is only used by unit tests for other types of buffers
	  return NULL;
	case type_static:
	  return new raw(type_char, len);
	default:
	  return new raw(get_type(), len);
	}
      }

      // no copying.
      raw(const raw &other);
      const raw& operator=(const raw &other);

      // private constructor, use factory functions to enforce
      // type-specific invariants
      raw(uint8_t flags, unsigned len, char *data = NULL)
	: data(data), len(len), nref(0), flags(flags),
	  crc_lock("buffer::raw::crc_lock", false, false)
      {
	switch (get_type()) {
	case type_malloc:
	  init_malloc();
	  break;
	case type_aligned:
	  init_aligned();
	  break;
	case type_pipe:
	  init_pipe();
	  break;
	case type_char:
	  init_char();
	  break;
	case type_static:
	  assert(data);
	  break; // noop
	}
      }

      // private destructor, only deleted by ptr (a friend)
      virtual ~raw()
      {
	switch (get_type()) {
	case type_malloc:
	  cleanup_malloc();
	  break;
	case type_aligned:
	  cleanup_aligned();
	  break;
	case type_pipe:
	  cleanup_pipe();
	  break;
	case type_char:
	  cleanup_char();
	  break;
	case type_static:
	  break; // noop
	}
      }

    public:
      char *get_data() {
	switch (get_type()) {
	case type_aligned:
	  if (flags & flag_alignment_hack)
	    return data + CEPH_PAGE_SIZE - ((ptrdiff_t)data & ~CEPH_PAGE_MASK);
	  return data;
	case type_pipe:
	  return copy_pipe();
	default:
	  return data;
	}
      }

      raw *clone() {
	raw *c = clone_empty();
	memcpy(c->data, data, len);
	return c;
      }

#ifndef CEPH_HAVE_SPLICE
      bool can_zero_copy() const { return false; }
      int set_source(int fd, loff_t *off) { return -ENOTSUP; }
      int zero_copy_to_fd(int fd, loff_t *offset) { return -ENOTSUP; }
#else // CEPH_HAVE_SPLICE
      bool can_zero_copy() const { return get_type() == type_pipe; }

      int set_source(int fd, loff_t *off) {
	ssize_t r = safe_splice(fd, off, pipefds[1], NULL, len,
				SPLICE_F_NONBLOCK);
	if (r < 0) {
	  bdout << "raw_pipe: error splicing into pipe: " << cpp_strerror(r)
		<< bendl;
	  return r;
	}
	// update length with actual amount read
	len = r;
	return 0;
      }

      int zero_copy_to_fd(int fd, loff_t *offset) {
	assert((flags & flag_pipe_consumed) == 0);
	int flags = SPLICE_F_NONBLOCK;
	ssize_t r = safe_splice_exact(pipefds[0], NULL, fd, offset, len, flags);
	if (r < 0) {
	  bdout << "raw_pipe: error splicing from pipe to fd: "
		<< cpp_strerror(r) << bendl;
	  return r;
	}
	flags |= flag_pipe_consumed;
	return 0;
      }
#endif // CEPH_HAVE_SPLICE

      bool is_page_aligned() {
	switch (get_type()) {
	case type_aligned:
	  return true;
	case type_pipe:
	  return false;
	default:
	  return ((long)data & ~CEPH_PAGE_MASK) == 0;
	}
      }

      bool is_n_page_sized() {
	return (len & ~CEPH_PAGE_MASK) == 0;
      }

      bool get_crc(const pair<size_t, size_t> &fromto,
		   pair<uint32_t, uint32_t> *crc) const {
	Mutex::Locker l(crc_lock);
	map<pair<size_t, size_t>, pair<uint32_t, uint32_t> >::const_iterator i
	  = crc_map.find(fromto);
	if (i == crc_map.end())
	  return false;
	*crc = i->second;
	return true;
      }

      void set_crc(const pair<size_t, size_t> &fromto,
		   const pair<uint32_t, uint32_t> &crc) {
	Mutex::Locker l(crc_lock);
	crc_map[fromto] = crc;
      }

      void invalidate_crc() {
	Mutex::Locker l(crc_lock);
	crc_map.clear();
      }


      static raw* create(unsigned len) {
	return new raw(type_char, len);
      }
      static raw* claim_char(unsigned len, char *buf) {
	return new raw(type_char, len, buf);
      }
      static raw* create_malloc(unsigned len) {
	return new raw(type_malloc, len);
      }
      static raw* claim_malloc(unsigned len, char *buf) {
	return new raw(type_malloc, len, buf);
      }
      static raw* create_static(unsigned len, char *buf) {
	return new raw(type_static, len, buf);
      }
      static raw* create_page_aligned(unsigned len) {
	return new raw(type_aligned, len);
      }
      static raw* create_zero_copy(unsigned len, int fd, int64_t *offset) {
	raw* buf = new raw(type_pipe, len);
	int r = buf->set_source(fd, (loff_t*)offset);
	if (r < 0) {
	  delete buf;
	  throw error_code(r);
	}
	return buf;
      }
#ifdef HAVE_XIO
      static raw* create_xio_msg(unsigned len, char *buf,
				 XioCompletionHook *hook);
#endif

      friend class ptr;
      friend std::ostream& operator<<(std::ostream& out, const raw &r);
    };


    inline raw* copy(const char *c, unsigned len) {
      raw* r = raw::create(len);
      memcpy(r->get_data(), c, len);
      return r;
    }

    inline std::ostream& operator<<(std::ostream& out, const raw &r) {
      return out << "buffer::raw(" << (void*)r.data << " len " << r.len
		 << " nref " << r.nref.read() << ")";
    }

  } /* namespace buffer */

} /* namespace ceph */

#endif /* CEPH_BUFFER_RAW_H */
