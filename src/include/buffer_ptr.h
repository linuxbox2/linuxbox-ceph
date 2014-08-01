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

#ifndef CEPH_BUFFER_PTR_H
#define CEPH_BUFFER_PTR_H

#include "buffer_int.h"
#include "buffer_raw.h"

namespace ceph {

  /* re-open buffer namespace */
  namespace buffer {

    /*
     * a buffer pointer.  references (a subsequence of) a raw buffer.
     */
    class ptr {
    private:
      buffer::raw *_raw;
      unsigned _off, _len;

      void release() {
	if (_raw) {
	  bdout << "ptr " << this << " release " << _raw << bendl;
	  if (_raw->nref.dec() == 0) {
	    /* cout << "hosing raw " << (void*)_raw << " len " << _raw->len
	       << std::endl; */
	    delete _raw;  // dealloc old (if any)
	  }
	  _raw = 0;
	}
      }


    public:
      ptr() : _raw(0), _off(0), _len(0) {}

      ptr(raw *r) : _raw(r), _off(0), _len(r->len) {
	// no lock needed; this is an unref raw.
	r->nref.inc();
	bdout << "ptr " << this << " get " << _raw << bendl;
      }

      ptr(unsigned l) : _off(0), _len(l) {
	_raw = create(l);
	_raw->nref.inc();
	bdout << "ptr " << this << " get " << _raw << bendl;
      }

      ptr(const char *d, unsigned l) : _off(0), _len(l) {
	_raw = copy(d, l);
	_raw->nref.inc();
	bdout << "ptr " << this << " get " << _raw << bendl;
      }

      ptr(const ptr& p) : _raw(p._raw), _off(p._off), _len(p._len) {
	if (_raw) {
	  _raw->nref.inc();
	  bdout << "ptr " << this << " get " << _raw << bendl;
	}
      }

      ptr(const ptr& p, unsigned o, unsigned l)
	: _raw(p._raw), _off(p._off + o), _len(l) {
	assert(o+l <= p._len);
	assert(_raw);
	_raw->nref.inc();
	bdout << "ptr " << this << " get " << _raw << bendl;
      }

      ptr& operator= (const ptr& p) {
	if (p._raw) {
	  p._raw->nref.inc();
	  bdout << "ptr " << this << " get " << _raw << bendl;
	}
	buffer::raw *raw = p._raw;
	release();
	if (raw) {
	  _raw = raw;
	  _off = p._off;
	  _len = p._len;
	} else {
	  _off = _len = 0;
	}
	return *this;
      }

      ~ptr() {
	release();
      }

      bool have_raw() const { return _raw ? true:false; } // !!raw

      // misc
      bool at_buffer_head() const { return _off == 0; }
      bool at_buffer_tail() const { return _off + _len == _raw->len; }
      bool is_page_aligned() const {
	return ((long)c_str() & ~CEPH_PAGE_MASK) == 0; }
      bool is_n_page_sized() const { return (length() & ~CEPH_PAGE_MASK) == 0; }

      // accessors
      raw *get_raw() const { return _raw; }

      const char *c_str() const {
	assert(_raw);
	if (unlikely(buffer_track_c_str))
	  buffer_c_str_accesses.inc();
	return _raw->get_data() + _off;
      }

      char *c_str() {
	assert(_raw);
	if (unlikely(buffer_track_c_str))
	  buffer_c_str_accesses.inc();
	return _raw->get_data() + _off;
      }

      unsigned length() const { return _len; }
      unsigned offset() const { return _off; }
      unsigned start() const { return _off; }
      unsigned end() const { return _off + _len; }

      unsigned unused_tail_length() const {
	if (_raw)
	  return _raw->len - (_off+_len);
	else
	  return 0;
      }

      const char& operator[](unsigned n) const {
	assert(_raw);
	assert(n < _len);
	return _raw->get_data()[_off + n];
      }

      char& operator[](unsigned n) {
	assert(_raw);
	assert(n < _len);
	return _raw->get_data()[_off + n];
      }

      const char *raw_c_str() const {
	assert(_raw);
	return _raw->data;
      }

      unsigned raw_length() const { assert(_raw); return _raw->len; }
      int raw_nref() const { assert(_raw); return _raw->nref.read(); }

      void copy_out(unsigned o, unsigned l, char *dest) const {
	assert(_raw);
	if (!((o <= _len) && (o+l <= _len)))
	  throw end_of_buffer();
	memcpy(dest, c_str()+o, l);
      }

      bool can_zero_copy() const {
	return _raw->can_zero_copy();
      }

      int zero_copy_to_fd(int fd, int64_t *offset) const {
	return _raw->zero_copy_to_fd(fd, (loff_t*)offset);
      }

      unsigned wasted() {
	assert(_raw);
	return _raw->len - _len;
      }

      int cmp(const ptr& o) const {
	int l = _len < o._len ? _len : o._len;
	if (l) {
	  int r = memcmp(c_str(), o.c_str(), l);
	  if (r)
	    return r;
	}
	if (_len < o._len)
	  return -1;
	if (_len > o._len)
	  return 1;
	return 0;
      }

      bool is_zero() const {
	const char *data = c_str();
	for (size_t p = 0; p < _len; p++) {
	  if (data[p] != 0) {
	    return false;
	  }
	}
	return true;
      }

      // modifiers
      void set_offset(unsigned o) { _off = o; }
      void set_length(unsigned l) { _len = l; }

      void append(char c) {
	assert(_raw);
	assert(1 <= unused_tail_length());
	(c_str())[_len] = c;
	_len++;
      }

      void append(const char *p, unsigned l) {
	assert(_raw);
	assert(l <= unused_tail_length());
	memcpy(c_str() + _len, p, l);
	_len += l;
      }

      void copy_in(unsigned o, unsigned l, const char *src) {
	assert(_raw);
	assert(o <= _len);
	assert(o+l <= _len);
	_raw->invalidate_crc();
	memcpy(c_str()+o, src, l);
      }

      void zero() {
	_raw->invalidate_crc();
	memset(c_str(), 0, _len);
      }

      void zero(unsigned o, unsigned l) {
	assert(o+l <= _len);
	_raw->invalidate_crc();
	memset(c_str()+o, 0, l);
      }

      buffer::raw *clone() {
	return _raw->clone();
      }

      void swap(ptr& other) {
	buffer::raw *r = _raw;
	unsigned o = _off;
	unsigned l = _len;
	_raw = other._raw;
	_off = other._off;
	_len = other._len;
	other._raw = r;
	other._off = o;
	other._len = l;
      }

    }; /* class buffer::ptr */

    inline std::ostream& operator<<(std::ostream& out, const ptr& bp) {
      if (bp.have_raw())
	out << "ptr(" << bp.offset() << "~" << bp.length()
	    << " " << (void*)bp.c_str()
	    << " in raw " << (void*)bp.raw_c_str()
	    << " len " << bp.raw_length()
	    << " nref " << bp.raw_nref() << ")";
      else
	out << "buffer:ptr(" << bp.offset() << "~" << bp.length()
	    << " no raw)";
      return out;
    }

  } /* namespace buffer */

} /* namespace ceph */

#endif /* CEPH_BUFFER_PTR_H */
