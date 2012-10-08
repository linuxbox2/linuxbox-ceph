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

#ifndef CEPH_OBJECTER_H
#define CEPH_OBJECTER_H

#include "include/types.h"
#include "include/buffer.h"
#include "include/xlist.h"

#include "osd/OSDMap.h"
#include "messages/MOSDOp.h"

#include "common/admin_socket.h"
#include "common/Timer.h"

#include <list>
#include <map>
#include <memory>
#include <ext/hash_map>
using namespace std;
using namespace __gnu_cxx;

class Context;
class Messenger;
class OSDMap;
class MonClient;
class Message;

class MPoolOpReply;

class MGetPoolStatsReply;
class MStatfsReply;

class PerfCounters;

// -----------------------------------------

struct ObjectOperation {
  vector<OSDOp> ops;
  int flags;
  int priority;

  vector<bufferlist*> out_bl;
  vector<Context*> out_handler;
  vector<int*> out_rval;

  ObjectOperation() : flags(0), priority(0) {}

  size_t size() {
    return ops.size();
  }

  void set_last_op_flags(int flags) {
    assert(!ops.empty());
    ops.rbegin()->op.flags = flags;
  }

  OSDOp& add_op(int op) {
    int s = ops.size();
    ops.resize(s+1);
    ops[s].op.op = op;
    out_bl.resize(s+1);
    out_bl[s] = NULL;
    out_handler.resize(s+1);
    out_handler[s] = NULL;
    out_rval.resize(s+1);
    out_rval[s] = NULL;
    return ops[s];
  }
  void add_data(int op, uint64_t off, uint64_t len, bufferlist& bl) {
    OSDOp& osd_op = add_op(op);
    osd_op.op.op = op;
    osd_op.op.extent.offset = off;
    osd_op.op.extent.length = len;
    osd_op.indata.claim_append(bl);
  }
  void add_clone_range(int op, uint64_t off, uint64_t len, const object_t& srcoid, uint64_t srcoff, snapid_t srcsnapid) {
    OSDOp& osd_op = add_op(op);
    osd_op.op.op = op;
    osd_op.op.clonerange.offset = off;
    osd_op.op.clonerange.length = len;
    osd_op.op.clonerange.src_offset = srcoff;
    osd_op.soid = sobject_t(srcoid, srcsnapid);
  }
  void add_xattr(int op, const char *name, const bufferlist& data) {
    OSDOp& osd_op = add_op(op);
    osd_op.op.op = op;
    osd_op.op.xattr.name_len = (name ? strlen(name) : 0);
    osd_op.op.xattr.value_len = data.length();
    if (name)
      osd_op.indata.append(name);
    osd_op.indata.append(data);
  }
  void add_xattr_cmp(int op, const char *name, uint8_t cmp_op, uint8_t cmp_mode, const bufferlist& data) {
    OSDOp& osd_op = add_op(op);
    osd_op.op.op = op;
    osd_op.op.xattr.name_len = (name ? strlen(name) : 0);
    osd_op.op.xattr.value_len = data.length();
    osd_op.op.xattr.cmp_op = cmp_op;
    osd_op.op.xattr.cmp_mode = cmp_mode;
    if (name)
      osd_op.indata.append(name);
    osd_op.indata.append(data);
  }
  void add_call(int op, const char *cname, const char *method, bufferlist &indata) {
    OSDOp& osd_op = add_op(op);
    osd_op.op.op = op;
    osd_op.op.cls.class_len = strlen(cname);
    osd_op.op.cls.method_len = strlen(method);
    osd_op.op.cls.indata_len = indata.length();
    osd_op.indata.append(cname, osd_op.op.cls.class_len);
    osd_op.indata.append(method, osd_op.op.cls.method_len);
    osd_op.indata.append(indata);
  }
  void add_watch(int op, uint64_t cookie, uint64_t ver, uint8_t flag, bufferlist& inbl) {
    OSDOp& osd_op = add_op(op);
    osd_op.op.op = op;
    osd_op.op.watch.cookie = cookie;
    osd_op.op.watch.ver = ver;
    osd_op.op.watch.flag = flag;
    osd_op.indata.append(inbl);
  }
  void add_pgls(int op, uint64_t count, collection_list_handle_t cookie, epoch_t start_epoch) {
    OSDOp& osd_op = add_op(op);
    osd_op.op.op = op;
    osd_op.op.pgls.count = count;
    osd_op.op.pgls.start_epoch = start_epoch;
    ::encode(cookie, osd_op.indata);
  }
  void add_pgls_filter(int op, uint64_t count, bufferlist& filter, collection_list_handle_t cookie, epoch_t start_epoch) {
    OSDOp& osd_op = add_op(op);
    osd_op.op.op = op;
    osd_op.op.pgls.count = count;
    osd_op.op.pgls.start_epoch = start_epoch;
    string cname = "pg";
    string mname = "filter";
    ::encode(cname, osd_op.indata);
    ::encode(mname, osd_op.indata);
    ::encode(cookie, osd_op.indata);
    osd_op.indata.append(filter);
  }

  // ------

  // pg
  void pg_ls(uint64_t count, bufferlist& filter, collection_list_handle_t cookie, epoch_t start_epoch) {
    if (filter.length() == 0)
      add_pgls(CEPH_OSD_OP_PGLS, count, cookie, start_epoch);
    else
      add_pgls_filter(CEPH_OSD_OP_PGLS_FILTER, count, filter, cookie, start_epoch);
    flags |= CEPH_OSD_FLAG_PGOP;
  }

  void create(bool excl) {
    OSDOp& o = add_op(CEPH_OSD_OP_CREATE);
    o.op.flags = (excl ? CEPH_OSD_OP_FLAG_EXCL : 0);
  }
  void create(bool excl, const string& category) {
    OSDOp& o = add_op(CEPH_OSD_OP_CREATE);
    o.op.flags = (excl ? CEPH_OSD_OP_FLAG_EXCL : 0);
    ::encode(category, o.indata);
  }

  struct C_ObjectOperation_stat : public Context {
    bufferlist bl;
    uint64_t *psize;
    utime_t *pmtime;
    time_t *ptime;
    C_ObjectOperation_stat(uint64_t *ps, utime_t *pm, time_t *pt) : psize(ps), pmtime(pm), ptime(pt) {}
    void finish(int r) {
      if (r >= 0) {
	bufferlist::iterator p = bl.begin();
	try {
	  uint64_t size;
	  utime_t mtime;
	  ::decode(size, p);
	  ::decode(mtime, p);
	  if (psize)
	    *psize = size;
	  if (pmtime)
	    *pmtime = mtime;
	  if (ptime)
	    *ptime = mtime.sec();
	}
	catch (buffer::error& e) {
	  r = -EIO;
	}
      }
    }
  };
  void stat(uint64_t *psize, utime_t *pmtime, int *prval) {
    add_op(CEPH_OSD_OP_STAT);
    unsigned p = ops.size() - 1;
    C_ObjectOperation_stat *h = new C_ObjectOperation_stat(psize, pmtime, NULL);
    out_bl[p] = &h->bl;
    out_handler[p] = h;
    out_rval[p] = prval;
  }
  void stat(uint64_t *psize, time_t *ptime, int *prval) {
    add_op(CEPH_OSD_OP_STAT);
    unsigned p = ops.size() - 1;
    C_ObjectOperation_stat *h = new C_ObjectOperation_stat(psize, NULL, ptime);
    out_bl[p] = &h->bl;
    out_handler[p] = h;
    out_rval[p] = prval;
  }

  // object data
  void read(uint64_t off, uint64_t len, bufferlist *pbl, int *prval) {
    bufferlist bl;
    add_data(CEPH_OSD_OP_READ, off, len, bl);
    unsigned p = ops.size() - 1;
    out_bl[p] = pbl;
    out_rval[p] = prval;
  }
  void write(uint64_t off, bufferlist& bl) {
    add_data(CEPH_OSD_OP_WRITE, off, bl.length(), bl);
  }
  void write_full(bufferlist& bl) {
    add_data(CEPH_OSD_OP_WRITEFULL, 0, bl.length(), bl);
  }
  void append(bufferlist& bl) {
    add_data(CEPH_OSD_OP_APPEND, 0, bl.length(), bl);
  }
  void zero(uint64_t off, uint64_t len) {
    bufferlist bl;
    add_data(CEPH_OSD_OP_ZERO, off, len, bl);
  }
  void truncate(uint64_t off) {
    bufferlist bl;
    add_data(CEPH_OSD_OP_TRUNCATE, off, 0, bl);
  }
  void remove() {
    bufferlist bl;
    add_data(CEPH_OSD_OP_DELETE, 0, 0, bl);
  }
  void mapext(uint64_t off, uint64_t len) {
    bufferlist bl;
    add_data(CEPH_OSD_OP_MAPEXT, off, len, bl);
  }
  void sparse_read(uint64_t off, uint64_t len) {
    bufferlist bl;
    add_data(CEPH_OSD_OP_SPARSE_READ, off, len, bl);
  }

  void clone_range(const object_t& src_oid, uint64_t src_offset, uint64_t len, uint64_t dst_offset) {
    add_clone_range(CEPH_OSD_OP_CLONERANGE, dst_offset, len, src_oid, src_offset, CEPH_NOSNAP);
  }

  // object attrs
  void getxattr(const char *name, bufferlist *pbl, int *prval) {
    bufferlist bl;
    add_xattr(CEPH_OSD_OP_GETXATTR, name, bl);
    unsigned p = ops.size() - 1;
    out_bl[p] = pbl;
    out_rval[p] = prval;
  }
  struct C_ObjectOperation_decodevals : public Context {
    bufferlist bl;
    std::map<std::string,bufferlist> *pattrs;
    int *prval;
    C_ObjectOperation_decodevals(std::map<std::string,bufferlist> *pa, int *pr)
      : pattrs(pa), prval(pr) {}
    void finish(int r) {
      if (r >= 0) {
	bufferlist::iterator p = bl.begin();
	try {
	  if (pattrs)
	    ::decode(*pattrs, p);
	}
	catch (buffer::error& e) {
	  if (prval)
	    *prval = -EIO;
	}
      }
    }
  };
  struct C_ObjectOperation_decodekeys : public Context {
    bufferlist bl;
    std::set<std::string> *pattrs;
    int *prval;
    C_ObjectOperation_decodekeys(std::set<std::string> *pa, int *pr)
      : pattrs(pa), prval(pr) {}
    void finish(int r) {
      if (r >= 0) {
	bufferlist::iterator p = bl.begin();
	try {
	  if (pattrs)
	    ::decode(*pattrs, p);
	}
	catch (buffer::error& e) {
	  if (prval)
	    *prval = -EIO;
	}
      }	
    }
  };
  void getxattrs(std::map<std::string,bufferlist> *pattrs, int *prval) {
    add_op(CEPH_OSD_OP_GETXATTRS);
    if (pattrs || prval) {
      unsigned p = ops.size() - 1;
      C_ObjectOperation_decodevals *h = new C_ObjectOperation_decodevals(pattrs, prval);
      out_handler[p] = h;
      out_bl[p] = &h->bl;
      out_rval[p] = prval;
    }
  }
  void setxattr(const char *name, const bufferlist& bl) {
    add_xattr(CEPH_OSD_OP_SETXATTR, name, bl);
  }
  void setxattr(const char *name, const string& s) {
    bufferlist bl;
    bl.append(s);
    add_xattr(CEPH_OSD_OP_SETXATTR, name, bl);
  }
  void cmpxattr(const char *name, uint8_t cmp_op, uint8_t cmp_mode, const bufferlist& bl) {
    add_xattr_cmp(CEPH_OSD_OP_CMPXATTR, name, cmp_op, cmp_mode, bl);
  }
  void rmxattr(const char *name) {
    bufferlist bl;
    add_xattr(CEPH_OSD_OP_RMXATTR, name, bl);
  }
  void setxattrs(map<string, bufferlist>& attrs) {
    bufferlist bl;
    ::encode(attrs, bl);
    add_xattr(CEPH_OSD_OP_RESETXATTRS, 0, bl.length());
  }
  void resetxattrs(const char *prefix, map<string, bufferlist>& attrs) {
    bufferlist bl;
    ::encode(attrs, bl);
    add_xattr(CEPH_OSD_OP_RESETXATTRS, prefix, bl);
  }
  
  // trivialmap
  void tmap_update(bufferlist& bl) {
    add_data(CEPH_OSD_OP_TMAPUP, 0, 0, bl);
  }
  void tmap_put(bufferlist& bl) {
    add_data(CEPH_OSD_OP_TMAPPUT, 0, bl.length(), bl);
  }
  void tmap_get(bufferlist *pbl, int *prval) {
    add_op(CEPH_OSD_OP_TMAPGET);
    unsigned p = ops.size() - 1;
    out_bl[p] = pbl;
    out_rval[p] = prval;
  }
  void tmap_get() {
    add_op(CEPH_OSD_OP_TMAPGET);
  }

  // objectmap
  void omap_get_keys(const string &start_after,
		     uint64_t max_to_get,
		     std::set<std::string> *out_set,
		     int *prval) {
    OSDOp &op = add_op(CEPH_OSD_OP_OMAPGETKEYS);
    bufferlist bl;
    ::encode(start_after, bl);
    ::encode(max_to_get, bl);
    op.op.extent.offset = 0;
    op.op.extent.length = bl.length();
    op.indata.claim_append(bl);
    if (prval || out_set) {
      unsigned p = ops.size() - 1;
      C_ObjectOperation_decodekeys *h =
	new C_ObjectOperation_decodekeys(out_set, prval);
      out_handler[p] = h;
      out_bl[p] = &h->bl;
      out_rval[p] = prval;
    }
  }

  void omap_get_vals(const string &start_after,
		     const string &filter_prefix,
		     uint64_t max_to_get,
		     std::map<std::string, bufferlist> *out_set,
		     int *prval) {
    OSDOp &op = add_op(CEPH_OSD_OP_OMAPGETVALS);
    bufferlist bl;
    ::encode(start_after, bl);
    ::encode(max_to_get, bl);
    ::encode(filter_prefix, bl);
    op.op.extent.offset = 0;
    op.op.extent.length = bl.length();
    op.indata.claim_append(bl);
    if (prval || out_set) {
      unsigned p = ops.size() - 1;
      C_ObjectOperation_decodevals *h =
	new C_ObjectOperation_decodevals(out_set, prval);
      out_handler[p] = h;
      out_bl[p] = &h->bl;
      out_rval[p] = prval;
    }
  }

  void omap_get_vals_by_keys(const std::set<std::string> &to_get,
			    std::map<std::string, bufferlist> *out_set,
			    int *prval) {
    OSDOp &op = add_op(CEPH_OSD_OP_OMAPGETVALSBYKEYS);
    bufferlist bl;
    ::encode(to_get, bl);
    op.op.extent.offset = 0;
    op.op.extent.length = bl.length();
    op.indata.claim_append(bl);
    if (prval || out_set) {
      unsigned p = ops.size() - 1;
      C_ObjectOperation_decodevals *h =
	new C_ObjectOperation_decodevals(out_set, prval);
      out_handler[p] = h;
      out_bl[p] = &h->bl;
      out_rval[p] = prval;
    }
  }

  void omap_get_header(bufferlist *bl, int *prval) {
    add_op(CEPH_OSD_OP_OMAPGETHEADER);
    unsigned p = ops.size() - 1;
    out_bl[p] = bl;
    out_rval[p] = prval;
  }

  void omap_set(const map<string, bufferlist> &map) {
    bufferlist bl;
    ::encode(map, bl);
    add_data(CEPH_OSD_OP_OMAPSETVALS, 0, bl.length(), bl);
  }

  void omap_set_header(bufferlist &bl) {
    add_data(CEPH_OSD_OP_OMAPSETHEADER, 0, bl.length(), bl);
  }

  void omap_clear() {
    add_op(CEPH_OSD_OP_OMAPCLEAR);
  }

  void omap_rm_keys(const std::set<std::string> &to_remove) {
    bufferlist bl;
    ::encode(to_remove, bl);
    add_data(CEPH_OSD_OP_OMAPRMKEYS, 0, bl.length(), bl);
  }

  // object classes
  void call(const char *cname, const char *method, bufferlist &indata) {
    add_call(CEPH_OSD_OP_CALL, cname, method, indata);
  }

  // watch/notify
  void watch(uint64_t cookie, uint64_t ver, bool set) {
    bufferlist inbl;
    add_watch(CEPH_OSD_OP_WATCH, cookie, ver, (set ? 1 : 0), inbl);
  }

  void notify(uint64_t cookie, uint64_t ver, bufferlist& inbl) {
    add_watch(CEPH_OSD_OP_NOTIFY, cookie, ver, 1, inbl); 
  }

  void notify_ack(uint64_t notify_id, uint64_t ver) {
    bufferlist bl;
    add_watch(CEPH_OSD_OP_NOTIFY_ACK, notify_id, ver, 0, bl);
  }

  void assert_version(uint64_t ver) {
    bufferlist bl;
    add_watch(CEPH_OSD_OP_ASSERT_VER, 0, ver, 0, bl);
  }
  void assert_src_version(const object_t& srcoid, snapid_t srcsnapid, uint64_t ver) {
    bufferlist bl;
    add_watch(CEPH_OSD_OP_ASSERT_SRC_VERSION, 0, ver, 0, bl);
    ops.rbegin()->soid = sobject_t(srcoid, srcsnapid);
  }

  void cmpxattr(const char *name, const bufferlist& val,
		int op, int mode) {
    add_xattr(CEPH_OSD_OP_CMPXATTR, name, val);
    OSDOp& o = *ops.rbegin();
    o.op.xattr.cmp_op = op;
    o.op.xattr.cmp_mode = mode;
  }
  void src_cmpxattr(const object_t& srcoid, snapid_t srcsnapid,
		    const char *name, const bufferlist& val,
		    int op, int mode) {
    add_xattr(CEPH_OSD_OP_SRC_CMPXATTR, name, val);
    OSDOp& o = *ops.rbegin();
    o.soid = sobject_t(srcoid, srcsnapid);
    o.op.xattr.cmp_op = op;
    o.op.xattr.cmp_mode = mode;
  }

};


// ----------------


class Objecter {
 public:
  Messenger *messenger;
  MonClient *monc;
  OSDMap    *osdmap;
  CephContext *cct;

  bool initialized;

private:
  tid_t last_tid;
  int client_inc;
  uint64_t max_linger_id;
  int num_unacked;
  int num_uncommitted;
  int global_op_flags; // flags which are applied to each IO op
  bool keep_balanced_budget;
  bool honor_osdmap_full;

  void maybe_request_map(epoch_t epoch=0);

  version_t last_seen_osdmap_version;
  version_t last_seen_pgmap_version;

  Mutex &client_lock;
  SafeTimer &timer;

  PerfCounters *logger;
  
  class C_Tick : public Context {
    Objecter *ob;
  public:
    C_Tick(Objecter *o) : ob(o) {}
    void finish(int r) { ob->tick(); }
  } *tick_event;

  void schedule_tick();
  void tick();

  class RequestStateHook : public AdminSocketHook {
    Objecter *m_objecter;
  public:
    RequestStateHook(Objecter *objecter);
    bool call(std::string command, std::string args, bufferlist& out);
  };

  RequestStateHook *m_request_state_hook;

public:
  /*** track pending operations ***/
  // read
 public:

  class OSDSession;

  struct Op {
    OSDSession *session;
    xlist<Op*>::item session_item;
    int incarnation;
    
    object_t oid;
    object_locator_t oloc;

    pg_t pgid;
    vector<int> acting;
    bool used_replica;

    Connection *con;  // for rx buffer only

    vector<OSDOp> ops;

    snapid_t snapid;
    SnapContext snapc;
    utime_t mtime;

    bufferlist *outbl;
    vector<bufferlist*> out_bl;
    vector<Context*> out_handler;
    vector<int*> out_rval;

    int flags, priority;
    Context *onack, *oncommit;

    tid_t tid;
    eversion_t version;        // for op replay
    int attempts;

    bool paused;

    eversion_t *objver;
    epoch_t *reply_epoch;

    utime_t stamp;

    bool precalc_pgid;

    bool budgeted;

    /// true if we should resend this message on failure
    bool should_resend;

    Op(const object_t& o, const object_locator_t& ol, vector<OSDOp>& op,
       int f, Context *ac, Context *co, eversion_t *ov) :
      session(NULL), session_item(this), incarnation(0),
      oid(o), oloc(ol),
      used_replica(false), con(NULL),
      snapid(CEPH_NOSNAP),
      outbl(NULL),
      flags(f), priority(0), onack(ac), oncommit(co),
      tid(0), attempts(0),
      paused(false), objver(ov), reply_epoch(NULL), precalc_pgid(false),
      budgeted(false),
      should_resend(true) {
      ops.swap(op);
      
      /* initialize out_* to match op vector */
      out_bl.resize(ops.size());
      out_rval.resize(ops.size());
      out_handler.resize(ops.size());
      for (unsigned i = 0; i < ops.size(); i++) {
	out_bl[i] = NULL;
	out_handler[i] = NULL;
	out_rval[i] = NULL;
      }

      if (oloc.key == o)
	oloc.key.clear();
    }

    bool operator<(const Op& other) const {
      return tid < other.tid;
    }
  };

  struct C_Op_Map_Latest : public Context {
    Objecter *objecter;
    tid_t tid;
    C_Op_Map_Latest(Objecter *o, tid_t t) : objecter(o), tid(t) {}
    void finish(int r);
  };

  struct C_Stat : public Context {
    bufferlist bl;
    uint64_t *psize;
    utime_t *pmtime;
    Context *fin;
    C_Stat(uint64_t *ps, utime_t *pm, Context *c) :
      psize(ps), pmtime(pm), fin(c) {}
    void finish(int r) {
      if (r >= 0) {
	bufferlist::iterator p = bl.begin();
	uint64_t s;
	utime_t m;
	::decode(s, p);
	::decode(m, p);
	if (psize)
	  *psize = s;
	if (pmtime)
	  *pmtime = m;
      }
      fin->finish(r);
      delete fin;
    }
  };

  struct C_GetAttrs : public Context {
    bufferlist bl;
    map<string,bufferlist>& attrset;
    Context *fin;
    C_GetAttrs(map<string, bufferlist>& set, Context *c) : attrset(set), fin(c) {}
    void finish(int r) {
      if (r >= 0) {
	bufferlist::iterator p = bl.begin();
	::decode(attrset, p);
      }
      fin->finish(r);
      delete fin;
    }
  };


  // Pools and statistics 
  struct ListContext {
    int current_pg;
    collection_list_handle_t cookie;
    epoch_t current_pg_epoch;
    int starting_pg_num;
    bool at_end;

    int64_t pool_id;
    int pool_snap_seq;
    int max_entries;
    std::list<pair<object_t, string> > list;

    bufferlist filter;

    bufferlist extra_info;

    ListContext() : current_pg(0), current_pg_epoch(0), starting_pg_num(0),
		    at_end(false), pool_id(0),
		    pool_snap_seq(0), max_entries(0) {}
  };

  struct C_List : public Context {
    ListContext *list_context;
    Context *final_finish;
    bufferlist *bl;
    Objecter *objecter;
    epoch_t epoch;
    C_List(ListContext *lc, Context * finish, bufferlist *b, Objecter *ob) :
      list_context(lc), final_finish(finish), bl(b), objecter(ob), epoch(0) {}
    void finish(int r) {
      if (r >= 0) {
        objecter->_list_reply(list_context, r, bl, final_finish, epoch);
      } else {
        final_finish->finish(r);
        delete final_finish;
      }
    }
  };
  
  struct PoolStatOp {
    tid_t tid;
    list<string> pools;

    map<string,pool_stat_t> *pool_stats;
    Context *onfinish;

    utime_t last_submit;
  };

  struct StatfsOp {
    tid_t tid;
    struct ceph_statfs *stats;
    Context *onfinish;

    utime_t last_submit;
  };

  struct PoolOp {
    tid_t tid;
    int64_t pool;
    string name;
    Context *onfinish;
    int pool_op;
    uint64_t auid;
    __u8 crush_rule;
    snapid_t snapid;
    bufferlist *blp;

    utime_t last_submit;
    PoolOp() : tid(0), pool(0), onfinish(0), pool_op(0),
	       auid(0), crush_rule(0), snapid(0), blp(NULL) {}
  };


  // -- lingering ops --

  struct LingerOp : public RefCountedObject {
    uint64_t linger_id;
    object_t oid;
    object_locator_t oloc;

    pg_t pgid;
    vector<int> acting;

    snapid_t snap;
    int flags;
    vector<OSDOp> ops;
    bufferlist inbl;
    bufferlist *poutbl;
    eversion_t *pobjver;

    bool registering, registered;
    Context *on_reg_ack, *on_reg_commit;

    OSDSession *session;
    xlist<LingerOp*>::item session_item;

    tid_t register_tid;

    LingerOp() : linger_id(0), flags(0), poutbl(NULL), pobjver(NULL),
		 registering(false), registered(false),
		 on_reg_ack(NULL), on_reg_commit(NULL),
		 session(NULL), session_item(this),
		 register_tid(0) {}

    // no copy!
    const LingerOp &operator=(const LingerOp& r);
    LingerOp(const LingerOp& o);
  private:
    ~LingerOp() {}
  };

  struct C_Linger_Ack : public Context {
    Objecter *objecter;
    LingerOp *info;
    C_Linger_Ack(Objecter *o, LingerOp *l) : objecter(o), info(l) {
      info->get();
    }
    ~C_Linger_Ack() {
      info->put();
    }
    void finish(int r) {
      objecter->_linger_ack(info, r);
    }
  };
  
  struct C_Linger_Commit : public Context {
    Objecter *objecter;
    LingerOp *info;
    C_Linger_Commit(Objecter *o, LingerOp *l) : objecter(o), info(l) {
      info->get();
    }
    ~C_Linger_Commit() {
      info->put();
    }
    void finish(int r) {
      objecter->_linger_commit(info, r);
    }
  };

  struct C_Linger_Map_Latest : public Context {
    Objecter *objecter;
    uint64_t linger_id;
    C_Linger_Map_Latest(Objecter *o, uint64_t id) :
      objecter(o), linger_id(id) {}
    void finish(int r);
  };

  // -- osd sessions --
  struct OSDSession {
    xlist<Op*> ops;
    xlist<LingerOp*> linger_ops;
    int osd;
    int incarnation;
    Connection *con;

    OSDSession(int o) : osd(o), incarnation(0), con(NULL) {}
  };
  map<int,OSDSession*> osd_sessions;


 private:
  // pending ops
  hash_map<tid_t,Op*>       ops;
  int                       num_homeless_ops;
  map<uint64_t, LingerOp*>  linger_ops;
  map<tid_t,PoolStatOp*>    poolstat_ops;
  map<tid_t,StatfsOp*>      statfs_ops;
  map<tid_t,PoolOp*>        pool_ops;

  // ops waiting for an osdmap with a new pool or confirmation that
  // the pool does not exist (may be expanded to other uses later)
  map<uint64_t, LingerOp*>  check_latest_map_lingers;
  map<tid_t, Op*>           check_latest_map_ops;

  map<epoch_t,list< pair<Context*, int> > > waiting_for_map;

  void send_op(Op *op);
  void cancel_op(Op *op);
  void finish_op(Op *op);
  bool is_pg_changed(vector<int>& a, vector<int>& b, bool any_change=false);
  enum recalc_op_target_result {
    RECALC_OP_TARGET_NO_ACTION = 0,
    RECALC_OP_TARGET_NEED_RESEND,
    RECALC_OP_TARGET_POOL_DNE,
  };
  int recalc_op_target(Op *op);
  bool recalc_linger_op_target(LingerOp *op);

  void send_linger(LingerOp *info);
  void _linger_ack(LingerOp *info, int r);
  void _linger_commit(LingerOp *info, int r);

  void op_check_for_latest_map(Op *op);
  void op_cancel_map_check(Op *op);
  void linger_check_for_latest_map(LingerOp *op);
  void linger_cancel_map_check(LingerOp *op);

  void kick_requests(OSDSession *session);

  OSDSession *get_session(int osd);
  void reopen_session(OSDSession *session);
  void close_session(OSDSession *session);
  
  void _list_reply(ListContext *list_context, int r, bufferlist *bl, Context *final_finish,
		   epoch_t reply_epoch);

  void resend_mon_ops();

  /**
   * handle a budget for in-flight ops
   * budget is taken whenever an op goes into the ops map
   * and returned whenever an op is removed from the map
   * If throttle_op needs to throttle it will unlock client_lock.
   */
  int calc_op_budget(Op *op);
  void throttle_op(Op *op, int op_size=0);
  void take_op_budget(Op *op) {
    int op_budget = calc_op_budget(op);
    if (keep_balanced_budget) {
      throttle_op(op, op_budget);
    } else {
      op_throttle_bytes.take(op_budget);
      op_throttle_ops.take(1);
    }
    op->budgeted = true;
  }
  void put_op_budget(Op *op) {
    assert(op->budgeted);
    int op_budget = calc_op_budget(op);
    op_throttle_bytes.put(op_budget);
    op_throttle_ops.put(1);
  }
  Throttle op_throttle_bytes, op_throttle_ops;

 public:
  Objecter(CephContext *cct_, Messenger *m, MonClient *mc,
	   OSDMap *om, Mutex& l, SafeTimer& t) : 
    messenger(m), monc(mc), osdmap(om), cct(cct_),
    initialized(false),
    last_tid(0), client_inc(-1), max_linger_id(0),
    num_unacked(0), num_uncommitted(0),
    global_op_flags(0),
    keep_balanced_budget(false), honor_osdmap_full(true),
    last_seen_osdmap_version(0),
    last_seen_pgmap_version(0),
    client_lock(l), timer(t),
    logger(NULL), tick_event(NULL),
    m_request_state_hook(NULL),
    num_homeless_ops(0),
    op_throttle_bytes(cct, "objecter_bytes", cct->_conf->objecter_inflight_op_bytes),
    op_throttle_ops(cct, "objecter_ops", cct->_conf->objecter_inflight_ops)
  { }
  ~Objecter() {
    assert(!tick_event);
    assert(!m_request_state_hook);
    assert(!logger);
  }

  void init();
  void shutdown();

  /**
   * Tell the objecter to throttle outgoing ops according to its
   * budget (in _conf). If you do this, ops can block, in
   * which case it will unlock client_lock and sleep until
   * incoming messages reduce the used budget low enough for
   * the ops to continue going; then it will lock client_lock again.
   */
  void set_balanced_budget() { keep_balanced_budget = true; }
  void unset_balanced_budget() { keep_balanced_budget = false; }

  void set_honor_osdmap_full() { honor_osdmap_full = true; }
  void unset_honor_osdmap_full() { honor_osdmap_full = false; }

  void scan_requests(bool skipped_map,
		     map<tid_t, Op*>& need_resend,
		     list<LingerOp*>& need_resend_linger);

  // messages
 public:
  void dispatch(Message *m);
  void handle_osd_op_reply(class MOSDOpReply *m);
  void handle_osd_map(class MOSDMap *m);
  void wait_for_osd_map();

private:
  // low-level
  tid_t op_submit(Op *op);
  tid_t _op_submit(Op *op);

  // public interface
 public:
  bool is_active() {
    return !(ops.empty() && linger_ops.empty() && poolstat_ops.empty() && statfs_ops.empty());
  }

  /**
   * Output in-flight requests
   */
  void dump_active();
  void dump_requests(Formatter& fmt) const;
  void dump_ops(Formatter& fmt) const;
  void dump_linger_ops(Formatter& fmt) const;
  void dump_pool_ops(Formatter& fmt) const;
  void dump_pool_stat_ops(Formatter& fmt) const;
  void dump_statfs_ops(Formatter& fmt) const;

  int get_client_incarnation() const { return client_inc; }
  void set_client_incarnation(int inc) { client_inc = inc; }

  void wait_for_new_map(Context *c, epoch_t epoch, int replyCode=0) {
    maybe_request_map(epoch);
    waiting_for_map[epoch].push_back(pair<Context *, int>(c, replyCode));
  }

  /** Get the current set of global op flags */
  int get_global_op_flags() { return global_op_flags; }
  /** Add a flag to the global op flags */
  void add_global_op_flags(int flag) { global_op_flags |= flag; }
  /** Clear the passed flags from the global op flag set */
  void clear_global_op_flag(int flags) { global_op_flags &= ~flags; }

  // mid-level helpers
  tid_t mutate(const object_t& oid, const object_locator_t& oloc, 
	       ObjectOperation& op,
	       const SnapContext& snapc, utime_t mtime, int flags,
	       Context *onack, Context *oncommit, eversion_t *objver = NULL) {
    Op *o = new Op(oid, oloc, op.ops, flags | global_op_flags | CEPH_OSD_FLAG_WRITE, onack, oncommit, objver);
    o->priority = op.priority;
    o->mtime = mtime;
    o->snapc = snapc;
    return op_submit(o);
  }
  tid_t read(const object_t& oid, const object_locator_t& oloc,
	     ObjectOperation& op,
	     snapid_t snapid, bufferlist *pbl, int flags,
	     Context *onack, eversion_t *objver = NULL) {
    Op *o = new Op(oid, oloc, op.ops, flags | global_op_flags | CEPH_OSD_FLAG_READ, onack, NULL, objver);
    o->priority = op.priority;
    o->snapid = snapid;
    o->outbl = pbl;
    o->out_bl.swap(op.out_bl);
    o->out_handler.swap(op.out_handler);
    o->out_rval.swap(op.out_rval);
    return op_submit(o);
  }
  tid_t linger(const object_t& oid, const object_locator_t& oloc,
	       ObjectOperation& op,
	       snapid_t snap, bufferlist& inbl, bufferlist *poutbl, int flags,
	       Context *onack, Context *onfinish,
	       eversion_t *objver);
  void unregister_linger(uint64_t linger_id);

  /**
   * set up initial ops in the op vector, and allocate a final op slot.
   *
   * The caller is responsible for filling in the final op.
   *
   * @param ops op vector
   * @param ops_count number of initial ops
   * @param extra_ops pointer to [array of] initial op[s]
   * @return index of final op (for caller to fill in)
   */
  int init_ops(vector<OSDOp>& ops, int ops_count, ObjectOperation *extra_ops) {
    int i;

    if (extra_ops)
      ops_count += extra_ops->ops.size();

    ops.resize(ops_count);

    for (i=0; i<ops_count - 1; i++) {
      ops[i] = extra_ops->ops[i];
    }

    return i;
  }


  // high-level helpers
  tid_t stat(const object_t& oid, const object_locator_t& oloc, snapid_t snap,
	     uint64_t *psize, utime_t *pmtime, int flags, 
	     Context *onfinish,
	     eversion_t *objver = NULL, ObjectOperation *extra_ops = NULL) {
    vector<OSDOp> ops;
    int i = init_ops(ops, 1, extra_ops);
    ops[i].op.op = CEPH_OSD_OP_STAT;
    C_Stat *fin = new C_Stat(psize, pmtime, onfinish);
    Op *o = new Op(oid, oloc, ops, flags | global_op_flags | CEPH_OSD_FLAG_READ, fin, 0, objver);
    o->snapid = snap;
    o->outbl = &fin->bl;
    return op_submit(o);
  }

  tid_t read(const object_t& oid, const object_locator_t& oloc, 
	     uint64_t off, uint64_t len, snapid_t snap, bufferlist *pbl, int flags,
	     Context *onfinish,
	     eversion_t *objver = NULL, ObjectOperation *extra_ops = NULL) {
    vector<OSDOp> ops;
    int i = init_ops(ops, 1, extra_ops);
    ops[i].op.op = CEPH_OSD_OP_READ;
    ops[i].op.extent.offset = off;
    ops[i].op.extent.length = len;
    ops[i].op.extent.truncate_size = 0;
    ops[i].op.extent.truncate_seq = 0;
    Op *o = new Op(oid, oloc, ops, flags | global_op_flags | CEPH_OSD_FLAG_READ, onfinish, 0, objver);
    o->snapid = snap;
    o->outbl = pbl;
    return op_submit(o);
  }

  tid_t read_trunc(const object_t& oid, const object_locator_t& oloc, 
	     uint64_t off, uint64_t len, snapid_t snap, bufferlist *pbl, int flags,
	     uint64_t trunc_size, __u32 trunc_seq,
	     Context *onfinish,
	     eversion_t *objver = NULL, ObjectOperation *extra_ops = NULL) {
    vector<OSDOp> ops;
    int i = init_ops(ops, 1, extra_ops);
    ops[i].op.op = CEPH_OSD_OP_READ;
    ops[i].op.extent.offset = off;
    ops[i].op.extent.length = len;
    ops[i].op.extent.truncate_size = trunc_size;
    ops[i].op.extent.truncate_seq = trunc_seq;
    Op *o = new Op(oid, oloc, ops, flags | global_op_flags | CEPH_OSD_FLAG_READ, onfinish, 0, objver);
    o->snapid = snap;
    o->outbl = pbl;
    return op_submit(o);
  }
  tid_t mapext(const object_t& oid, const object_locator_t& oloc,
	     uint64_t off, uint64_t len, snapid_t snap, bufferlist *pbl, int flags,
	     Context *onfinish,
	     eversion_t *objver = NULL, ObjectOperation *extra_ops = NULL) {
    vector<OSDOp> ops;
    int i = init_ops(ops, 1, extra_ops);
    ops[i].op.op = CEPH_OSD_OP_MAPEXT;
    ops[i].op.extent.offset = off;
    ops[i].op.extent.length = len;
    ops[i].op.extent.truncate_size = 0;
    ops[i].op.extent.truncate_seq = 0;
    Op *o = new Op(oid, oloc, ops, flags | global_op_flags | CEPH_OSD_FLAG_READ, onfinish, 0, objver);
    o->snapid = snap;
    o->outbl = pbl;
    return op_submit(o);
  }
  tid_t sparse_read(const object_t& oid, const object_locator_t& oloc,
	     uint64_t off, uint64_t len, snapid_t snap, bufferlist *pbl, int flags,
	     Context *onfinish,
	     eversion_t *objver = NULL, ObjectOperation *extra_ops = NULL) {
    vector<OSDOp> ops;
    int i = init_ops(ops, 1, extra_ops);
    ops[i].op.op = CEPH_OSD_OP_SPARSE_READ;
    ops[i].op.extent.offset = off;
    ops[i].op.extent.length = len;
    ops[i].op.extent.truncate_size = 0;
    ops[i].op.extent.truncate_seq = 0;
    Op *o = new Op(oid, oloc, ops, flags | global_op_flags | CEPH_OSD_FLAG_READ, onfinish, 0, objver);
    o->snapid = snap;
    o->outbl = pbl;
    return op_submit(o);
  }

  tid_t getxattr(const object_t& oid, const object_locator_t& oloc,
	     const char *name, snapid_t snap, bufferlist *pbl, int flags,
	     Context *onfinish,
	     eversion_t *objver = NULL, ObjectOperation *extra_ops = NULL) {
    vector<OSDOp> ops;
    int i = init_ops(ops, 1, extra_ops);
    ops[i].op.op = CEPH_OSD_OP_GETXATTR;
    ops[i].op.xattr.name_len = (name ? strlen(name) : 0);
    ops[i].op.xattr.value_len = 0;
    if (name)
      ops[i].indata.append(name);
    Op *o = new Op(oid, oloc, ops, flags | global_op_flags | CEPH_OSD_FLAG_READ, onfinish, 0, objver);
    o->snapid = snap;
    o->outbl = pbl;
    return op_submit(o);
  }

  tid_t getxattrs(const object_t& oid, const object_locator_t& oloc, snapid_t snap,
		  map<string,bufferlist>& attrset,
		  int flags, Context *onfinish,
		  eversion_t *objver = NULL, ObjectOperation *extra_ops = NULL) {
    vector<OSDOp> ops;
    int i = init_ops(ops, 1, extra_ops);
    ops[i].op.op = CEPH_OSD_OP_GETXATTRS;
    C_GetAttrs *fin = new C_GetAttrs(attrset, onfinish);
    Op *o = new Op(oid, oloc, ops, flags | global_op_flags | CEPH_OSD_FLAG_READ, fin, 0, objver);
    o->snapid = snap;
    o->outbl = &fin->bl;
    return op_submit(o);
  }

  tid_t read_full(const object_t& oid, const object_locator_t& oloc,
		  snapid_t snap, bufferlist *pbl, int flags,
		  Context *onfinish,
		  eversion_t *objver = NULL, ObjectOperation *extra_ops = NULL) {
    return read(oid, oloc, 0, 0, snap, pbl, flags | CEPH_OSD_FLAG_READ, onfinish, objver);
  }

  // writes
  tid_t _modify(const object_t& oid, const object_locator_t& oloc,
		vector<OSDOp>& ops, utime_t mtime,
		const SnapContext& snapc, int flags,
		Context *onack, Context *oncommit,
		eversion_t *objver = NULL) {
    Op *o = new Op(oid, oloc, ops, flags | CEPH_OSD_FLAG_WRITE, onack, oncommit, objver);
    o->mtime = mtime;
    o->snapc = snapc;
    return op_submit(o);
  }
  tid_t write(const object_t& oid, const object_locator_t& oloc,
	      uint64_t off, uint64_t len, const SnapContext& snapc, const bufferlist &bl,
	      utime_t mtime, int flags,
	      Context *onack, Context *oncommit,
	      eversion_t *objver = NULL, ObjectOperation *extra_ops = NULL) {
    vector<OSDOp> ops;
    int i = init_ops(ops, 1, extra_ops);
    ops[i].op.op = CEPH_OSD_OP_WRITE;
    ops[i].op.extent.offset = off;
    ops[i].op.extent.length = len;
    ops[i].op.extent.truncate_size = 0;
    ops[i].op.extent.truncate_seq = 0;
    ops[i].indata = bl;
    Op *o = new Op(oid, oloc, ops, flags | global_op_flags | CEPH_OSD_FLAG_WRITE, onack, oncommit, objver);
    o->mtime = mtime;
    o->snapc = snapc;
    return op_submit(o);
  }
  tid_t append(const object_t& oid, const object_locator_t& oloc,
	       uint64_t len, const SnapContext& snapc, const bufferlist &bl,
	       utime_t mtime, int flags,
	       Context *onack, Context *oncommit,
	       eversion_t *objver = NULL, ObjectOperation *extra_ops = NULL) {
    vector<OSDOp> ops;
    int i = init_ops(ops, 1, extra_ops);
    ops[i].op.op = CEPH_OSD_OP_APPEND;
    ops[i].op.extent.offset = 0;
    ops[i].op.extent.length = len;
    ops[i].op.extent.truncate_size = 0;
    ops[i].op.extent.truncate_seq = 0;
    ops[i].indata = bl;
    Op *o = new Op(oid, oloc, ops, flags | global_op_flags | CEPH_OSD_FLAG_WRITE, onack, oncommit, objver);
    o->mtime = mtime;
    o->snapc = snapc;
    return op_submit(o);
  }
  tid_t write_trunc(const object_t& oid, const object_locator_t& oloc,
	      uint64_t off, uint64_t len, const SnapContext& snapc, const bufferlist &bl,
	      utime_t mtime, int flags,
	      uint64_t trunc_size, __u32 trunc_seq,
	      Context *onack, Context *oncommit,
	      eversion_t *objver = NULL, ObjectOperation *extra_ops = NULL) {
    vector<OSDOp> ops;
    int i = init_ops(ops, 1, extra_ops);
    ops[i].op.op = CEPH_OSD_OP_WRITE;
    ops[i].op.extent.offset = off;
    ops[i].op.extent.length = len;
    ops[i].op.extent.truncate_size = trunc_size;
    ops[i].op.extent.truncate_seq = trunc_seq;
    ops[i].indata = bl;
    Op *o = new Op(oid, oloc, ops, flags | global_op_flags | CEPH_OSD_FLAG_WRITE, onack, oncommit, objver);
    o->mtime = mtime;
    o->snapc = snapc;
    return op_submit(o);
  }
  tid_t write_full(const object_t& oid, const object_locator_t& oloc,
		   const SnapContext& snapc, const bufferlist &bl, utime_t mtime, int flags,
		   Context *onack, Context *oncommit,
		   eversion_t *objver = NULL, ObjectOperation *extra_ops = NULL) {
    vector<OSDOp> ops;
    int i = init_ops(ops, 1, extra_ops);
    ops[i].op.op = CEPH_OSD_OP_WRITEFULL;
    ops[i].op.extent.offset = 0;
    ops[i].op.extent.length = bl.length();
    ops[i].indata = bl;
    Op *o = new Op(oid, oloc, ops, flags | global_op_flags | CEPH_OSD_FLAG_WRITE, onack, oncommit, objver);
    o->mtime = mtime;
    o->snapc = snapc;
    return op_submit(o);
  }
  tid_t trunc(const object_t& oid, const object_locator_t& oloc,
	      const SnapContext& snapc,
	      utime_t mtime, int flags,
	      uint64_t trunc_size, __u32 trunc_seq,
	      Context *onack, Context *oncommit,
	      eversion_t *objver = NULL, ObjectOperation *extra_ops = NULL) {
    vector<OSDOp> ops;
    int i = init_ops(ops, 1, extra_ops);
    ops[i].op.op = CEPH_OSD_OP_TRUNCATE;
    ops[i].op.extent.offset = trunc_size;
    ops[i].op.extent.truncate_size = trunc_size;
    ops[i].op.extent.truncate_seq = trunc_seq;
    Op *o = new Op(oid, oloc, ops, flags | global_op_flags | CEPH_OSD_FLAG_WRITE, onack, oncommit, objver);
    o->mtime = mtime;
    o->snapc = snapc;
    return op_submit(o);
  }
  tid_t zero(const object_t& oid, const object_locator_t& oloc, 
	     uint64_t off, uint64_t len, const SnapContext& snapc, utime_t mtime, int flags,
             Context *onack, Context *oncommit,
	     eversion_t *objver = NULL, ObjectOperation *extra_ops = NULL) {
    vector<OSDOp> ops;
    int i = init_ops(ops, 1, extra_ops);
    ops[i].op.op = CEPH_OSD_OP_ZERO;
    ops[i].op.extent.offset = off;
    ops[i].op.extent.length = len;
    Op *o = new Op(oid, oloc, ops, flags | global_op_flags | CEPH_OSD_FLAG_WRITE, onack, oncommit, objver);
    o->mtime = mtime;
    o->snapc = snapc;
    return op_submit(o);
  }
  tid_t rollback_object(const object_t& oid, const object_locator_t& oloc,
		 const SnapContext& snapc, snapid_t snapid,
		 utime_t mtime, Context *onack, Context *oncommit,
		 eversion_t *objver = NULL, ObjectOperation *extra_ops = NULL) {
    vector<OSDOp> ops;
    int i = init_ops(ops, 1, extra_ops);
    ops[i].op.op = CEPH_OSD_OP_ROLLBACK;
    ops[i].op.snap.snapid = snapid;
    Op *o = new Op(oid, oloc, ops, CEPH_OSD_FLAG_WRITE, onack, oncommit, objver);
    o->mtime = mtime;
    o->snapc = snapc;
    return op_submit(o);
  }
  tid_t create(const object_t& oid, const object_locator_t& oloc, 
	     const SnapContext& snapc, utime_t mtime,
             int global_flags, int create_flags,
             Context *onack, Context *oncommit,
             eversion_t *objver = NULL, ObjectOperation *extra_ops = NULL) {
    vector<OSDOp> ops;
    int i = init_ops(ops, 1, extra_ops);
    ops[i].op.op = CEPH_OSD_OP_CREATE;
    ops[i].op.flags = create_flags;
    Op *o = new Op(oid, oloc, ops, global_flags | global_op_flags | CEPH_OSD_FLAG_WRITE, onack, oncommit, objver);
    o->mtime = mtime;
    o->snapc = snapc;
    return op_submit(o);
  }
  tid_t remove(const object_t& oid, const object_locator_t& oloc, 
	       const SnapContext& snapc, utime_t mtime, int flags,
	       Context *onack, Context *oncommit,
	       eversion_t *objver = NULL, ObjectOperation *extra_ops = NULL) {
    vector<OSDOp> ops;
    int i = init_ops(ops, 1, extra_ops);
    ops[i].op.op = CEPH_OSD_OP_DELETE;
    Op *o = new Op(oid, oloc, ops, flags | global_op_flags | CEPH_OSD_FLAG_WRITE, onack, oncommit, objver);
    o->mtime = mtime;
    o->snapc = snapc;
    return op_submit(o);
  }

  tid_t lock(const object_t& oid, const object_locator_t& oloc, int op, int flags,
	     Context *onack, Context *oncommit, eversion_t *objver = NULL, ObjectOperation *extra_ops = NULL) {
    SnapContext snapc;  // no snapc for lock ops
    vector<OSDOp> ops;
    int i = init_ops(ops, 1, extra_ops);
    ops[i].op.op = op;
    Op *o = new Op(oid, oloc, ops, flags | global_op_flags | CEPH_OSD_FLAG_WRITE, onack, oncommit, objver);
    o->snapc = snapc;
    return op_submit(o);
  }
  tid_t setxattr(const object_t& oid, const object_locator_t& oloc,
	      const char *name, const SnapContext& snapc, const bufferlist &bl,
	      utime_t mtime, int flags,
	      Context *onack, Context *oncommit,
	      eversion_t *objver = NULL, ObjectOperation *extra_ops = NULL) {
    vector<OSDOp> ops;
    int i = init_ops(ops, 1, extra_ops);
    ops[i].op.op = CEPH_OSD_OP_SETXATTR;
    ops[i].op.xattr.name_len = (name ? strlen(name) : 0);
    ops[i].op.xattr.value_len = bl.length();
    if (name)
      ops[i].indata.append(name);
    ops[i].indata.append(bl);
    Op *o = new Op(oid, oloc, ops, flags | global_op_flags | CEPH_OSD_FLAG_WRITE, onack, oncommit, objver);
    o->mtime = mtime;
    o->snapc = snapc;
    return op_submit(o);
  }
  tid_t removexattr(const object_t& oid, const object_locator_t& oloc,
	      const char *name, const SnapContext& snapc,
	      utime_t mtime, int flags,
	      Context *onack, Context *oncommit,
	      eversion_t *objver = NULL, ObjectOperation *extra_ops = NULL) {
    vector<OSDOp> ops;
    int i = init_ops(ops, 1, extra_ops);
    ops[i].op.op = CEPH_OSD_OP_RMXATTR;
    ops[i].op.xattr.name_len = (name ? strlen(name) : 0);
    ops[i].op.xattr.value_len = 0;
    if (name)
      ops[i].indata.append(name);
    Op *o = new Op(oid, oloc, ops, flags | global_op_flags | CEPH_OSD_FLAG_WRITE, onack, oncommit, objver);
    o->mtime = mtime;
    o->snapc = snapc;
    return op_submit(o);
  }

  void list_objects(ListContext *p, Context *onfinish);

  // -------------------------
  // pool ops
private:
  void pool_op_submit(PoolOp *op);
public:
  int create_pool_snap(int64_t pool, string& snapName, Context *onfinish);
  int allocate_selfmanaged_snap(int64_t pool, snapid_t *psnapid, Context *onfinish);
  int delete_pool_snap(int64_t pool, string& snapName, Context *onfinish);
  int delete_selfmanaged_snap(int64_t pool, snapid_t snap, Context *onfinish);

  int create_pool(string& name, Context *onfinish, uint64_t auid=0,
		  int crush_rule=-1);
  int delete_pool(int64_t pool, Context *onfinish);
  int change_pool_auid(int64_t pool, Context *onfinish, uint64_t auid);

  void handle_pool_op_reply(MPoolOpReply *m);

  // --------------------------
  // pool stats
private:
  void poolstat_submit(PoolStatOp *op);
public:
  void handle_get_pool_stats_reply(MGetPoolStatsReply *m);
  void get_pool_stats(list<string>& pools, map<string,pool_stat_t> *result,
		      Context *onfinish);

  // ---------------------------
  // df stats
private:
  void fs_stats_submit(StatfsOp *op);
public:
  void handle_fs_stats_reply(MStatfsReply *m);
  void get_fs_stats(struct ceph_statfs& result, Context *onfinish);

  // ---------------------------
  // some scatter/gather hackery

  void _sg_read_finish(vector<ObjectExtent>& extents, vector<bufferlist>& resultbl, 
		       bufferlist *bl, Context *onfinish);

  struct C_SGRead : public Context {
    Objecter *objecter;
    vector<ObjectExtent> extents;
    vector<bufferlist> resultbl;
    bufferlist *bl;
    Context *onfinish;
    C_SGRead(Objecter *ob, 
	     vector<ObjectExtent>& e, vector<bufferlist>& r, bufferlist *b, Context *c) :
      objecter(ob), bl(b), onfinish(c) {
      extents.swap(e);
      resultbl.swap(r);
    }
    void finish(int r) {
      objecter->_sg_read_finish(extents, resultbl, bl, onfinish);
    }      
  };

  void sg_read_trunc(vector<ObjectExtent>& extents, snapid_t snap, bufferlist *bl, int flags,
		uint64_t trunc_size, __u32 trunc_seq, Context *onfinish) {
    if (extents.size() == 1) {
      read_trunc(extents[0].oid, extents[0].oloc, extents[0].offset, extents[0].length,
	   snap, bl, flags, trunc_size, trunc_seq, onfinish);
    } else {
      C_GatherBuilder gather(cct);
      vector<bufferlist> resultbl(extents.size());
      int i=0;
      for (vector<ObjectExtent>::iterator p = extents.begin(); p != extents.end(); p++) {
	read_trunc(p->oid, p->oloc, p->offset, p->length,
	     snap, &resultbl[i++], flags, trunc_size, trunc_seq, gather.new_sub());
      }
      gather.set_finisher(new C_SGRead(this, extents, resultbl, bl, onfinish));
      gather.activate();
    }
  }

  void sg_read(vector<ObjectExtent>& extents, snapid_t snap, bufferlist *bl, int flags, Context *onfinish) {
    sg_read_trunc(extents, snap, bl, flags, 0, 0, onfinish);
  }

  void sg_write_trunc(vector<ObjectExtent>& extents, const SnapContext& snapc, const bufferlist& bl, utime_t mtime,
		int flags, uint64_t trunc_size, __u32 trunc_seq,
		Context *onack, Context *oncommit) {
    if (extents.size() == 1) {
      write_trunc(extents[0].oid, extents[0].oloc, extents[0].offset, extents[0].length,
	    snapc, bl, mtime, flags, trunc_size, trunc_seq, onack, oncommit);
    } else {
      C_GatherBuilder gack(cct, onack);
      C_GatherBuilder gcom(cct, oncommit);
      for (vector<ObjectExtent>::iterator p = extents.begin(); p != extents.end(); p++) {
	bufferlist cur;
	for (map<uint64_t, uint64_t>::iterator bit = p->buffer_extents.begin();
	     bit != p->buffer_extents.end();
	     bit++)
	  bl.copy(bit->first, bit->second, cur);
	assert(cur.length() == p->length);
	write_trunc(p->oid, p->oloc, p->offset, p->length, 
	      snapc, cur, mtime, flags, trunc_size, trunc_seq,
	      onack ? gack.new_sub():0,
	      oncommit ? gcom.new_sub():0);
      }
      gack.activate();
      gcom.activate();
    }
  }

  void sg_write(vector<ObjectExtent>& extents, const SnapContext& snapc, const bufferlist& bl, utime_t mtime,
		int flags, Context *onack, Context *oncommit) {
    sg_write_trunc(extents, snapc, bl, mtime, flags, 0, 0, onack, oncommit);
  }

  void ms_handle_connect(Connection *con);
  void ms_handle_reset(Connection *con);
  void ms_handle_remote_reset(Connection *con);
};

#endif
