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

/* Object Store Device (OSD) Monitor
 */

#ifndef CEPH_OSDMONITOR_H
#define CEPH_OSDMONITOR_H

#include <map>
#include <set>
using namespace std;

#include "include/types.h"
#include "msg/Messenger.h"

#include "osd/OSDMap.h"

#include "PaxosService.h"
#include "Session.h"

class Monitor;
#include "messages/MOSDBoot.h"
#include "messages/MMonCommand.h"
#include "messages/MOSDMap.h"
#include "messages/MOSDFailure.h"
#include "messages/MPoolOp.h"

/// information about a particular peer's failure reports for one osd
struct failure_reporter_t {
  int num_reports;          ///< reports from this reporter
  utime_t failed_since;     ///< when they think it failed
  MOSDFailure *msg;         ///< most recent failure message

  failure_reporter_t() : num_reports(0), msg(NULL) {}
  failure_reporter_t(utime_t s) : num_reports(1), failed_since(s), msg(NULL) {}
  ~failure_reporter_t() {
    // caller should have taken this message before removing the entry.
    assert(!msg);
  }
};

/// information about all failure reports for one osd
struct failure_info_t {
  map<int, failure_reporter_t> reporters;  ///< reporter -> # reports
  utime_t max_failed_since;                ///< most recent failed_since
  int num_reports;

  failure_info_t() : num_reports(0) {}

  utime_t get_failed_since() {
    if (max_failed_since == utime_t() && !reporters.empty()) {
      // the old max must have canceled; recalculate.
      for (map<int, failure_reporter_t>::iterator p = reporters.begin();
	   p != reporters.end();
	   ++p)
	if (p->second.failed_since > max_failed_since)
	  max_failed_since = p->second.failed_since;
    }
    return max_failed_since;
  }

  // set the message for the latest report.  return any old message we had,
  // if any, so we can discard it.
  MOSDFailure *add_report(int who, utime_t failed_since, MOSDFailure *msg) {
    map<int, failure_reporter_t>::iterator p = reporters.find(who);
    if (p == reporters.end()) {
      if (max_failed_since == utime_t())
	max_failed_since = failed_since;
      else if (max_failed_since < failed_since)
	max_failed_since = failed_since;
      p = reporters.insert(map<int, failure_reporter_t>::value_type(who, failure_reporter_t(failed_since))).first;
    } else {
      p->second.num_reports++;
    }
    num_reports++;

    MOSDFailure *ret = p->second.msg;
    p->second.msg = msg;
    return ret;
  }

  void take_report_messages(list<MOSDFailure*>& ls) {
    for (map<int, failure_reporter_t>::iterator p = reporters.begin();
	 p != reporters.end();
	 ++p) {
      if (p->second.msg) {
	ls.push_back(p->second.msg);
	p->second.msg = NULL;
      }
    }
  }

  void cancel_report(int who) {
    map<int, failure_reporter_t>::iterator p = reporters.find(who);
    if (p == reporters.end())
      return;
    num_reports -= p->second.num_reports;
    reporters.erase(p);
    if (reporters.empty())
      max_failed_since = utime_t();
  }
};

class OSDMonitor : public PaxosService {
public:
  OSDMap osdmap;

private:
  // [leader]
  OSDMap::Incremental pending_inc;
  map<int, failure_info_t> failure_info;
  map<int,utime_t>    down_pending_out;  // osd down -> out

  map<int,double> osd_weight;

  /*
   * cache what epochs we think osds have.  this is purely
   * optimization to try to avoid sending the same inc maps twice.
   */
  map<int,epoch_t> osd_epoch;

  void check_failures(utime_t now);
  bool check_failure(utime_t now, int target_osd, failure_info_t& fi);

  // map thrashing
  int thrash_map;
  int thrash_last_up_osd;
  bool thrash();

  bool _have_pending_crush();
  CrushWrapper &_get_stable_crush();
  void _get_pending_crush(CrushWrapper& newcrush);

  // svc
public:  
  void create_initial();
private:
  void update_from_paxos(bool *need_bootstrap);
  void create_pending();  // prepare a new pending
  void encode_pending(MonitorDBStore::Transaction *t);
  void on_active();
  void on_shutdown();

  /**
   * we haven't delegated full version stashing to paxosservice for some time
   * now, making this function useless in current context.
   */
  virtual void encode_full(MonitorDBStore::Transaction *t) { }
  /**
   * do not let paxosservice periodically stash full osdmaps, or we will break our
   * locally-managed full maps.  (update_from_paxos loads the latest and writes them
   * out going forward from there, but if we just synced that may mean we skip some.)
   */
  virtual bool should_stash_full() {
    return false;
  }

  /**
   * hook into trim to include the oldest full map in the trim transaction
   *
   * This ensures that anyone post-sync will have enough to rebuild their
   * full osdmaps.
   */
  void encode_trim_extra(MonitorDBStore::Transaction *tx, version_t first);

  void update_msgr_features();

  void share_map_with_random_osd();

  void update_logger();

  void handle_query(PaxosServiceMessage *m);
  bool preprocess_query(PaxosServiceMessage *m);  // true if processed.
  bool prepare_update(PaxosServiceMessage *m);
  bool should_propose(double &delay);

  version_t get_trim_to();

  bool can_mark_down(int o);
  bool can_mark_up(int o);
  bool can_mark_out(int o);
  bool can_mark_in(int o);

  // ...
  MOSDMap *build_latest_full();
  MOSDMap *build_incremental(epoch_t first, epoch_t last);
  void send_full(PaxosServiceMessage *m);
  void send_incremental(PaxosServiceMessage *m, epoch_t first);
  void send_incremental(epoch_t first, entity_inst_t& dest, bool onetime);
  void send_incremental(epoch_t first, ConnectionRef &con, bool onetime);

  void remove_redundant_pg_temp();
  void remove_down_pg_temp();
  int reweight_by_utilization(int oload, std::string& out_str);

  bool check_source(PaxosServiceMessage *m, uuid_d fsid);
 
  bool preprocess_mark_me_down(class MOSDMarkMeDown *m);

  friend class C_AckMarkedDown;
  bool preprocess_failure(class MOSDFailure *m);
  bool prepare_failure(class MOSDFailure *m);
  bool prepare_mark_me_down(class MOSDMarkMeDown *m);
  void process_failures();
  void take_all_failures(list<MOSDFailure*>& ls);

  bool preprocess_boot(class MOSDBoot *m);
  bool prepare_boot(class MOSDBoot *m);
  void _booted(MOSDBoot *m, bool logit);

  bool preprocess_alive(class MOSDAlive *m);
  bool prepare_alive(class MOSDAlive *m);
  void _reply_map(PaxosServiceMessage *m, epoch_t e);

  bool preprocess_pgtemp(class MOSDPGTemp *m);
  bool prepare_pgtemp(class MOSDPGTemp *m);

  int _prepare_remove_pool(uint64_t pool);
  int _prepare_rename_pool(int64_t pool, string newname);

  bool preprocess_pool_op ( class MPoolOp *m);
  bool preprocess_pool_op_create ( class MPoolOp *m);
  bool prepare_pool_op (MPoolOp *m);
  bool prepare_pool_op_create (MPoolOp *m);
  bool prepare_pool_op_delete(MPoolOp *m);
  int prepare_new_pool(string& name, uint64_t auid, int crush_rule,
                       unsigned pg_num, unsigned pgp_num,
		       const vector<string> &properties);
  int prepare_new_pool(MPoolOp *m);

  void update_pool_flags(int64_t pool_id, uint64_t flags);
  bool update_pools_status();
  void get_pools_health(list<pair<health_status_t,string> >& summary,
                        list<pair<health_status_t,string> > *detail) const;

  bool prepare_set_flag(MMonCommand *m, int flag);
  bool prepare_unset_flag(MMonCommand *m, int flag);

  void _pool_op_reply(MPoolOp *m, int ret, epoch_t epoch, bufferlist *blp=NULL);

  struct C_Booted : public Context {
    OSDMonitor *cmon;
    MOSDBoot *m;
    bool logit;
    C_Booted(OSDMonitor *cm, MOSDBoot *m_, bool l=true) : 
      cmon(cm), m(m_), logit(l) {}
    void finish(int r) {
      if (r >= 0)
	cmon->_booted(m, logit);
      else if (r == -ECANCELED)
	m->put();
      else if (r == -EAGAIN)
	cmon->dispatch((PaxosServiceMessage*)m);
      else
	assert(0 == "bad C_Booted return value");
    }
  };

  struct C_ReplyMap : public Context {
    OSDMonitor *osdmon;
    PaxosServiceMessage *m;
    epoch_t e;
    C_ReplyMap(OSDMonitor *o, PaxosServiceMessage *mm, epoch_t ee) : osdmon(o), m(mm), e(ee) {}
    void finish(int r) {
      if (r >= 0)
	osdmon->_reply_map(m, e);
      else if (r == -ECANCELED)
	m->put();
      else if (r == -EAGAIN)
	osdmon->dispatch(m);
      else
	assert(0 == "bad C_ReplyMap return value");
    }
  };
  struct C_PoolOp : public Context {
    OSDMonitor *osdmon;
    MPoolOp *m;
    int replyCode;
    int epoch;
    bufferlist reply_data;
    C_PoolOp(OSDMonitor * osd, MPoolOp *m_, int rc, int e, bufferlist *rd=NULL) :
      osdmon(osd), m(m_), replyCode(rc), epoch(e) {
      if (rd)
	reply_data = *rd;
    }
    void finish(int r) {
      if (r >= 0)
	osdmon->_pool_op_reply(m, replyCode, epoch, &reply_data);
      else if (r == -ECANCELED)
	m->put();
      else if (r == -EAGAIN)
	osdmon->dispatch(m);
      else
	assert(0 == "bad C_PoolOp return value");
    }
  };

  bool preprocess_remove_snaps(struct MRemoveSnaps *m);
  bool prepare_remove_snaps(struct MRemoveSnaps *m);

 public:
  OSDMonitor(Monitor *mn, Paxos *p, string service_name)
  : PaxosService(mn, p, service_name),
    thrash_map(0), thrash_last_up_osd(-1) { }

  void tick();  // check state, take actions

  int parse_osd_id(const char *s, stringstream *pss);
  void parse_loc_map(const vector<string>& args, map<string,string> *ploc);

  void get_health(list<pair<health_status_t,string> >& summary,
		  list<pair<health_status_t,string> > *detail) const;
  bool preprocess_command(MMonCommand *m);
  bool prepare_command(MMonCommand *m);

  int prepare_command_pool_set(map<string,cmd_vartype> &cmdmap,
                               stringstream& ss);

  void handle_osd_timeouts(const utime_t &now,
			   std::map<int,utime_t> &last_osd_report);
  void mark_all_down();

  void send_latest(PaxosServiceMessage *m, epoch_t start=0);
  void send_latest_now_nodelete(PaxosServiceMessage *m, epoch_t start=0) {
    send_incremental(m, start);
  }

  epoch_t blacklist(const entity_addr_t& a, utime_t until);

  void dump_info(Formatter *f);

  void check_subs();
  void check_sub(Subscription *sub);

  void add_flag(int flag) {
    if (!(osdmap.flags & flag)) {
      if (pending_inc.new_flags < 0)
	pending_inc.new_flags = osdmap.flags;
      pending_inc.new_flags |= flag;
    }
  }

  void remove_flag(int flag) {
    if(osdmap.flags & flag) {
      if (pending_inc.new_flags < 0)
	pending_inc.new_flags = osdmap.flags;
      pending_inc.new_flags &= ~flag;
    }
  }
};

#endif
