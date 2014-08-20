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
#ifndef CEPH_BUFFER_H
#define CEPH_BUFFER_H

#if defined(__linux__) || defined(__FreeBSD__)
#include <stdlib.h>
#endif

#ifndef _XOPEN_SOURCE
# define _XOPEN_SOURCE 600
#endif

#include <stdio.h>

#if defined(__linux__)	// For malloc(2).
#include <malloc.h>
#endif

#include <inttypes.h>
#include <stdint.h>
#include <string.h>

#ifndef __CYGWIN__
# include <sys/mman.h>
#endif

#include <iostream>
#include <istream>
#include <iomanip>
#include <list>
#include <string>
#include <exception>

#include "page.h"
#include "crc32c.h"

#if defined(HAVE_XIO)
extern "C" {
#include "libxio.h"
}
#endif /* HAVE_XIO */

#ifdef __CEPH__
# include "include/assert.h"
#else
# include <assert.h>
#endif

class XioCompletionHook;

namespace ceph {

class buffer {
  /*
   * exceptions
   */

public:
  struct error : public std::exception{
    const char *what() const throw () {
      return "buffer::exception";
    }
  };
  struct bad_alloc : public error {
    const char *what() const throw () {
      return "buffer::bad_alloc";
    }
  };
  struct end_of_buffer : public error {
    const char *what() const throw () {
      return "buffer::end_of_buffer";
    }
  };
  struct malformed_input : public error {
    explicit malformed_input(const char *w) {
      snprintf(buf, sizeof(buf), "buffer::malformed_input: %s", w);
    }
    const char *what() const throw () {
      return buf;
    }
  private:
    char buf[256];
  };
  struct error_code : public malformed_input {
    explicit error_code(int error);
    int code;
  };


  /// total bytes allocated
  static int get_total_alloc();

  /// enable/disable alloc tracking
  static void track_alloc(bool b);

  /// count of cached crc hits (matching input)
  static int get_cached_crc();
  /// count of cached crc hits (mismatching input, required adjustment)
  static int get_cached_crc_adjusted();
  /// enable/disable tracking of cached crcs
  static void track_cached_crc(bool b);

  /// count of calls to buffer::ptr::c_str()
  static int get_c_str_accesses();
  /// enable/disable tracking of buffer::ptr::c_str() calls
  static void track_c_str(bool b);

private:
 
  /* hack for memory utilization debugging. */
  static void inc_total_alloc(unsigned len);
  static void dec_total_alloc(unsigned len);

  /*
   * an abstract raw buffer.  with a reference count.
   */
  class raw;
  class raw_crc;
  class raw_malloc;
  class raw_static;
  class raw_mmap_pages;
  class raw_posix_aligned;
  class raw_hack_aligned;
  class raw_char;
  class raw_pipe;

public:
  class xio_mempool;
  class xio_msg_buffer;

  friend std::ostream& operator<<(std::ostream& out, const raw &r);

public:

  /*
   * named constructors 
   */
  static raw* copy(const char *c, unsigned len);
  static raw* create(unsigned len);
  static raw* claim_char(unsigned len, char *buf);
  static raw* create_malloc(unsigned len);
  static raw* claim_malloc(unsigned len, char *buf);
  static raw* create_static(unsigned len, char *buf);
  static raw* create_page_aligned(unsigned len);
  static raw* create_zero_copy(unsigned len, int fd, int64_t *offset);

#if defined(HAVE_XIO)
 static raw* create_msg(unsigned len, char *buf, XioCompletionHook *m_hook);
#endif

  /*
   * a buffer pointer.  references (a subsequence of) a raw buffer.
   */
  class ptr {
    raw *_raw;
    unsigned _off, _len;

    void release();

  public:
    ptr() : _raw(0), _off(0), _len(0) {}
    ptr(raw *r);
    ptr(unsigned l);
    ptr(const char *d, unsigned l);
    ptr(const ptr& p);
    ptr(const ptr& p, unsigned o, unsigned l);
    ptr& operator= (const ptr& p);
    ~ptr() {
      release();
    }
    
    bool have_raw() const { return _raw ? true:false; }

    raw *clone();
    void swap(ptr& other);

    // misc
    bool at_buffer_head() const { return _off == 0; }
    bool at_buffer_tail() const;

    bool is_page_aligned() const { return ((long)c_str() & ~CEPH_PAGE_MASK) == 0; }
    bool is_n_page_sized() const { return (length() & ~CEPH_PAGE_MASK) == 0; }

    // accessors
    raw *get_raw() const { return _raw; }
    const char *c_str() const;
    char *c_str();
    unsigned length() const { return _len; }
    unsigned offset() const { return _off; }
    unsigned start() const { return _off; }
    unsigned end() const { return _off + _len; }
    unsigned unused_tail_length() const;
    const char& operator[](unsigned n) const;
    char& operator[](unsigned n);

    const char *raw_c_str() const;
    unsigned raw_length() const;
    int raw_nref() const;

    void copy_out(unsigned o, unsigned l, char *dest) const {
      assert(_raw);
      if (!((o <= _len) && (o+l <= _len)))
	throw end_of_buffer();
      memcpy(dest, c_str()+o, l);
    }

    bool can_zero_copy() const;
    int zero_copy_to_fd(int fd, int64_t *offset) const;

    unsigned wasted();

    int cmp(const ptr& o) const;
    bool is_zero() const;

    // modifiers
    void set_offset(unsigned o) { _off = o; }
    void set_length(unsigned l) { _len = l; }

    void append(char c);
    void append(const char *p, unsigned l);
    void copy_in(unsigned o, unsigned l, const char *src);
    void zero();
    void zero(unsigned o, unsigned l);

  };

  friend std::ostream& operator<<(std::ostream& out, const buffer::ptr& bp);

  /*
   * list - the useful bit!
   */

  class list {
    // my private bits
    std::list<ptr> _buffers;
    unsigned _len;

    ptr append_buffer;  // where i put small appends.

  public:
    class iterator {
      list *bl;
      std::list<ptr> *ls; // meh.. just here to avoid an extra pointer dereference..
      unsigned off;  // in bl
      std::list<ptr>::iterator p;
      unsigned p_off; // in *p
    public:
      // constructor.  position.
      iterator() :
	bl(0), ls(0), off(0), p_off(0) {}
      iterator(list *l, unsigned o=0) : 
	bl(l), ls(&bl->_buffers), off(0), p(ls->begin()), p_off(0) {
	advance(o);
      }
      iterator(list *l, unsigned o, std::list<ptr>::iterator ip, unsigned po) : 
	bl(l), ls(&bl->_buffers), off(o), p(ip), p_off(po) { }

      iterator(const iterator& other) : bl(other.bl),
					ls(other.ls),
					off(other.off),
					p(other.p),
					p_off(other.p_off) {}

      iterator& operator=(const iterator& other) {
	if (this != &other) {
	  bl = other.bl;
	  ls = other.ls;
	  off = other.off;
	  p = other.p;
	  p_off = other.p_off;
	}
	return *this;
      }

      /// get current iterator offset in buffer::list
      unsigned get_off() { return off; }

      /// get number of bytes remaining from iterator position to the end of the buffer::list
      unsigned get_remaining() { return bl->length() - off; }

      /// true if iterator is at the end of the buffer::list
      bool end() {
	return p == ls->end();
	//return off == bl->length();
      }

      // the inline keyword is optional, but here to remind folks not
      // to move these
      inline void advance(int o) {
	  //cout << this << " advance " << o << " from " << off << " (p_off " << p_off << " in " << p->length() << ")" << std::endl;
	  if (o > 0) {
	    p_off += o;
	    while (p_off > 0) {
	      if (p == ls->end())
		throw end_of_buffer();
	      if (p_off >= p->length()) {
		// skip this buffer
		p_off -= p->length();
		p++;
	      } else {
		// somewhere in this buffer!
		break;
	      }
	    }
	    off += o;
	    return;
	  }
	  while (o < 0) {
	    if (p_off) {
	      unsigned d = -o;
	      if (d > p_off)
		d = p_off;
	      p_off -= d;
	      off -= d;
	      o += d;
	    } else if (off > 0) {
	      assert(p != ls->begin());
	      p--;
	      p_off = p->length();
	    } else {
	      throw end_of_buffer();
	    }
	  }
	}

      inline void seek(unsigned o) {
	  //cout << this << " seek " << o << std::endl;
	  p = ls->begin();
	  off = p_off = 0;
	  advance(o);
	}

      inline char operator*() {
	  if (p == ls->end())
	    throw end_of_buffer();
	  return (*p)[p_off];
	}

      inline buffer::list::iterator& operator++() {
	  if (p == ls->end())
	    throw end_of_buffer();
	  advance(1);
	  return *this;
	}

      inline buffer::ptr get_current_ptr() {
	  if (p == ls->end())
	    throw end_of_buffer();
	  return ptr(*p, p_off, p->length() - p_off);
	}

      // copy data out.
      // note that these all _append_ to dest!
      inline void copy(unsigned len, char *dest) {
	  if (p == ls->end()) seek(off);
	  while (len > 0) {
	    if (p == ls->end())
	      throw end_of_buffer();
	    assert(p->length() > 0);

	    unsigned howmuch = p->length() - p_off;
	    if (len < howmuch) howmuch = len;
	    p->copy_out(p_off, howmuch, dest);
	    dest += howmuch;

	    len -= howmuch;
	    advance(howmuch);
	  }
	}

      inline void copy(unsigned len, ptr &dest) {
	  dest = create(len);
	  copy(len, dest.c_str());
	}

      inline void copy(unsigned len, list &dest) {
	  if (p == ls->end())
	    seek(off);
	  while (len > 0) {
	    if (p == ls->end())
	      throw end_of_buffer();

	    unsigned howmuch = p->length() - p_off;
	    if (len < howmuch)
	      howmuch = len;
	    dest.append(*p, p_off, howmuch);

	    len -= howmuch;
	    advance(howmuch);
	  }
	}

      inline void copy(unsigned len, std::string &dest) {
	  if (p == ls->end())
	    seek(off);
	  while (len > 0) {
	    if (p == ls->end())
	      throw end_of_buffer();

	    unsigned howmuch = p->length() - p_off;
	    const char *c_str = p->c_str();
	    if (len < howmuch)
	      howmuch = len;
	    dest.append(c_str + p_off, howmuch);

	    len -= howmuch;
	    advance(howmuch);
	  }
	}

      inline void copy_all(list &dest) {
	  if (p == ls->end())
	    seek(off);
	  while (1) {
	    if (p == ls->end())
	      return;
	    assert(p->length() > 0);

	    unsigned howmuch = p->length() - p_off;
	    const char *c_str = p->c_str();
	    dest.append(c_str + p_off, howmuch);

	    advance(howmuch);
	  }
	}

      // copy data in
      inline void copy_in(unsigned len, const char *src) {
	  // copy
	  if (p == ls->end())
	    seek(off);
	  while (len > 0) {
	    if (p == ls->end())
	      throw end_of_buffer();

	    unsigned howmuch = p->length() - p_off;
	    if (len < howmuch)
	      howmuch = len;
	    p->copy_in(p_off, howmuch, src);

	    src += howmuch;
	    len -= howmuch;
	    advance(howmuch);
	  }
	}

      inline void copy_in(unsigned len, const list& otherl) {
	  if (p == ls->end())
	    seek(off);
	  unsigned left = len;
	  for (std::list<ptr>::const_iterator i = otherl._buffers.begin();
	       i != otherl._buffers.end();
	       ++i) {
	    unsigned l = (*i).length();
	    if (left < l)
	      l = left;
	    copy_in(l, i->c_str());
	    left -= l;
	    if (left == 0)
	      break;
	  }
	}
    };

  private:
    mutable iterator last_p;
    int zero_copy_to_fd(int fd) const;

  public:
    // cons/des
    list() : _len(0), last_p(this) {}
    list(unsigned prealloc) : _len(0), last_p(this) {
      append_buffer = buffer::create(prealloc);
      append_buffer.set_length(0);   // unused, so far.
    }
    ~list() {}

    list(const list& other) : _buffers(other._buffers), _len(other._len), last_p(this) { }
    list& operator= (const list& other) {
      if (this != &other) {
        _buffers = other._buffers;
        _len = other._len;
      }
      return *this;
    }

    const std::list<ptr>& buffers() const { return _buffers; }

    void swap(list& other);
    unsigned length() const {
#if 0
      // DEBUG: verify _len
      unsigned len = 0;
      for (std::list<ptr>::const_iterator it = _buffers.begin();
	   it != _buffers.end();
	   it++) {
	len += (*it).length();
      }
      assert(len == _len);
#endif
      return _len;
    }
    bool contents_equal(buffer::list& other);

    bool can_zero_copy() const;
    bool is_page_aligned() const;
    bool is_n_page_sized() const;

    bool is_zero() const;

    // modifiers
    void clear() {
      _buffers.clear();
      _len = 0;
      last_p = begin();
    }
    void push_front(ptr& bp) {
      if (bp.length() == 0)
	return;
      _buffers.push_front(bp);
      _len += bp.length();
    }
    void push_front(raw *r) {
      ptr bp(r);
      push_front(bp);
    }
    void push_back(const ptr& bp) {
      if (bp.length() == 0)
	return;
      _buffers.push_back(bp);
      _len += bp.length();
    }
    void push_back(raw *r) {
      ptr bp(r);
      push_back(bp);
    }

    void zero();
    void zero(unsigned o, unsigned l);

    bool is_contiguous();
    void rebuild();
    void rebuild(ptr& nb);
    void rebuild_page_aligned();

    // sort-of-like-assignment-op
    void claim(list& bl);
    void claim_append(list& bl);
    void claim_prepend(list& bl);

    iterator begin() {
      return iterator(this, 0);
    }
    iterator end() {
      return iterator(this, _len, _buffers.end(), 0);
    }

    // crope lookalikes.
    // **** WARNING: this are horribly inefficient for large bufferlists. ****
    void copy(unsigned off, unsigned len, char *dest) const;
    void copy(unsigned off, unsigned len, list &dest) const;
    void copy(unsigned off, unsigned len, std::string& dest) const;
    void copy_in(unsigned off, unsigned len, const char *src);
    void copy_in(unsigned off, unsigned len, const list& src);

    void append(char c);
    void append(const char *data, unsigned len);
    void append(const std::string& s) {
      append(s.data(), s.length());
    }
    void append(const ptr& bp);
    void append(const ptr& bp, unsigned off, unsigned len);
    void append(const list& bl);
    void append(std::istream& in);
    void append_zero(unsigned len);

    /*
     * get a char
     */
    const char& operator[](unsigned n) const;
    char *c_str();
    void substr_of(const list& other, unsigned off, unsigned len);

    // funky modifer
    void splice(unsigned off, unsigned len, list *claim_by=0 /*, bufferlist& replace_with */);
    void write(int off, int len, std::ostream& out) const;

    void encode_base64(list& o);
    void decode_base64(list& o);

    void hexdump(std::ostream &out) const;
    int read_file(const char *fn, std::string *error);
    ssize_t read_fd(int fd, size_t len);
    int read_fd_zero_copy(int fd, size_t len);
    int write_file(const char *fn, int mode=0644);
    int write_fd(int fd) const;
    int write_fd_zero_copy(int fd) const;
    uint32_t crc32c(uint32_t crc) const;
  };

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
};

#if defined(HAVE_XIO)
  struct xio_mempool_obj* get_xio_mp(const buffer::ptr& bp);
#endif

typedef buffer::ptr bufferptr;
typedef buffer::list bufferlist;
typedef buffer::hash bufferhash;

inline bool operator>(bufferlist& l, bufferlist& r) {
  for (unsigned p = 0; ; p++) {
    if (l.length() > p && r.length() == p) return true;
    if (l.length() == p) return false;
    if (l[p] > r[p]) return true;
    if (l[p] < r[p]) return false;
  }
}
inline bool operator>=(bufferlist& l, bufferlist& r) {
  for (unsigned p = 0; ; p++) {
    if (l.length() > p && r.length() == p) return true;
    if (r.length() == p && l.length() == p) return true;
    if (l.length() == p && r.length() > p) return false;
    if (l[p] > r[p]) return true;
    if (l[p] < r[p]) return false;
  }
}

inline bool operator==(bufferlist &l, bufferlist &r) {
  if (l.length() != r.length())
    return false;
  for (unsigned p = 0; p < l.length(); p++) {
    if (l[p] != r[p])
      return false;
  }
  return true;
}
inline bool operator<(bufferlist& l, bufferlist& r) {
  return r > l;
}
inline bool operator<=(bufferlist& l, bufferlist& r) {
  return r >= l;
}


inline std::ostream& operator<<(std::ostream& out, const buffer::ptr& bp) {
  if (bp.have_raw())
    out << "buffer::ptr(" << bp.offset() << "~" << bp.length()
	<< " " << (void*)bp.c_str()
	<< " in raw " << (void*)bp.raw_c_str()
	<< " len " << bp.raw_length()
	<< " nref " << bp.raw_nref() << ")";
  else
    out << "buffer:ptr(" << bp.offset() << "~" << bp.length() << " no raw)";
  return out;
}

inline std::ostream& operator<<(std::ostream& out, const buffer::list& bl) {
  out << "buffer::list(len=" << bl.length() << "," << std::endl;

  std::list<buffer::ptr>::const_iterator it = bl.buffers().begin();
  while (it != bl.buffers().end()) {
    out << "\t" << *it;
    if (++it == bl.buffers().end()) break;
    out << "," << std::endl;
  }
  out << std::endl << ")";
  return out;
}

inline std::ostream& operator<<(std::ostream& out, buffer::error& e)
{
  return out << e.what();
}

inline bufferhash& operator<<(bufferhash& l, bufferlist &r) {
  l.update(r);
  return l;
}

}

#endif
