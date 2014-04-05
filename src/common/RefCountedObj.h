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

#ifndef CEPH_REFCOUNTEDOBJ_H
#define CEPH_REFCOUNTEDOBJ_H
 
#include "common/Mutex.h"
#include "common/Cond.h"
#include "include/atomic.h"


struct RefCountedObject {
  atomic_t nref;
  RefCountedObject() : nref(1) {}
  virtual ~RefCountedObject() {}
  
  RefCountedObject *get() {
    //generic_dout(0) << "RefCountedObject::get " << this << " " << nref.read() << " -> " << (nref.read() + 1) << dendl;
    nref.inc();
    return this;
  }
  RefCountedObject *add(int n) {
    //generic_dout(0) << "RefCountedObject::add " << this << " " << nref.read() << " -> " << (nref.read() + n) << dendl;
    nref.add(n);
    return this;
  }
  void put() {
    //generic_dout(0) << "RefCountedObject::put " << this << " " << nref.read() << " -> " << (nref.read() - 1) << dendl;
    if (nref.dec() == 0)
      delete this;
  }
  bool safe_put() {
    //generic_dout(0) << "RefCountedObject::safe_put " << this << " " << nref.read() << " -> " << (nref.read() - 1) << dendl;
    if (nref.dec() == 0) {
      delete this;
      return true;
    }
    return false;
  }
};

/**
 * RefCountedCond
 *
 *  a refcounted condition, will be removed when all references are dropped
 */

struct RefCountedCond : public RefCountedObject {
  bool complete;
  Mutex lock;
  Cond cond;
  int rval;

  RefCountedCond() : complete(false), lock("RefCountedCond"), rval(0) {}

  int wait() {
    Mutex::Locker l(lock);
    while (!complete) {
      cond.Wait(lock);
    }
    return rval;
  }

  void done(int r) {
    Mutex::Locker l(lock);
    rval = r;
    complete = true;
    cond.SignalAll();
  }

  void done() {
    done(0);
  }
};

/**
 * RefCountedWaitObject
 *
 * refcounted object that allows waiting for the object's last reference.
 * Any referrer can either put or put_wait(). A simple put() will return
 * immediately, a put_wait() will return only when the object is destroyed.
 * e.g., useful when we want to wait for a specific event completion. We
 * use RefCountedCond, as the condition can be referenced after the object
 * destruction. 
 *    
 */
struct RefCountedWaitObject {
  atomic_t nref;
  RefCountedCond *c;

  RefCountedWaitObject() : nref(1) {
    c = new RefCountedCond;
  }
  virtual ~RefCountedWaitObject() {
    c->put();
  }

  RefCountedWaitObject *get() {
    nref.inc();
    return this;
  }

  bool put() {
    bool ret = false;
    RefCountedCond *cond = c;
    cond->get();
    if (nref.dec() == 0) {
      cond->done();
      delete this;
      ret = true;
    }
    cond->put();
    return ret;
  }

  void put_wait() {
    RefCountedCond *cond = c;

    cond->get();
    if (nref.dec() == 0) {
      cond->done();
      delete this;
    } else {
      cond->wait();
    }
    cond->put();
  }
};

void intrusive_ptr_add_ref(RefCountedObject *p);
void intrusive_ptr_release(RefCountedObject *p);

#endif
