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

#include <unistd.h>

#include "global/signal_handler.h"

#include "include/types.h"
#include "common/entity_name.h"
#include "common/Clock.h"
#include "common/signal.h"
#include "common/ceph_argparse.h"
#include "common/errno.h"

#include "msg/Messenger.h"
#include "mon/MonClient.h"

#include "osdc/Objecter.h"
#include "osdc/Filer.h"
#include "osdc/Journaler.h"

#include "MDSMap.h"

#include "MDS.h"
#include "Server.h"
#include "Locker.h"
#include "MDCache.h"
#include "MDLog.h"
#include "MDBalancer.h"
#include "Migrator.h"

#include "SnapServer.h"
#include "SnapClient.h"

#include "InoTable.h"

#include "common/HeartbeatMap.h"

#include "common/perf_counters.h"

#include "common/Timer.h"

#include "events/ESession.h"

#include "messages/MMDSMap.h"
#include "messages/MMDSBeacon.h"

#include "messages/MGenericMessage.h"

#include "messages/MClientRequest.h"
#include "messages/MClientRequestForward.h"

#include "messages/MMDSTableRequest.h"

#include "messages/MMonCommand.h"

#include "auth/AuthAuthorizeHandler.h"
#include "auth/KeyRing.h"

#include "common/config.h"

#include "perfglue/cpu_profiler.h"
#include "perfglue/heap_profiler.h"


#define dout_subsys ceph_subsys_mds
#undef dout_prefix
#define dout_prefix *_dout << "mds." << whoami << '.' << incarnation << ' '


// cons/des
MDS::MDS(const std::string &n, Messenger *m, MonClient *mc) : 
  Dispatcher(m->cct),
  mds_lock("MDS::mds_lock"),
  timer(m->cct, mds_lock),
  hb(NULL),
  beacon(m->cct, mc, n),
  authorize_handler_cluster_registry(new AuthAuthorizeHandlerRegistry(m->cct,
								      m->cct->_conf->auth_supported.length() ?
								      m->cct->_conf->auth_supported :
								      m->cct->_conf->auth_cluster_required)),
  authorize_handler_service_registry(new AuthAuthorizeHandlerRegistry(m->cct,
								      m->cct->_conf->auth_supported.length() ?
								      m->cct->_conf->auth_supported :
								      m->cct->_conf->auth_service_required)),
  name(n),
  whoami(-1), incarnation(0),
  standby_for_rank(MDSMap::MDS_NO_STANDBY_PREF),
  standby_type(MDSMap::STATE_NULL),
  standby_replaying(false),
  messenger(m),
  monc(mc),
  log_client(m->cct, messenger, &mc->monmap, LogClient::NO_FLAGS),
  op_tracker(cct, m->cct->_conf->mds_enable_op_tracker, 
                     m->cct->_conf->osd_num_op_tracker_shard),
  finisher(cct),
  sessionmap(this),
  progress_thread(this),
  asok_hook(NULL)
{

  hb = cct->get_heartbeat_map()->add_worker("MDS");

  orig_argc = 0;
  orig_argv = NULL;

  last_tid = 0;

  clog = log_client.create_channel();

  monc->set_messenger(messenger);

  mdsmap = new MDSMap;

  objecter = new Objecter(m->cct, messenger, monc, 0, 0);
  objecter->unset_honor_osdmap_full();

  filer = new Filer(objecter);

  mdcache = new MDCache(this);
  mdlog = new MDLog(this);
  balancer = new MDBalancer(this);

  inotable = new InoTable(this);
  snapserver = new SnapServer(this);
  snapclient = new SnapClient(this);

  server = new Server(this);
  locker = new Locker(this, mdcache);

  dispatch_depth = 0;

  // clients
  last_client_mdsmap_bcast = 0;
  
  // tick
  tick_event = 0;

  req_rate = 0;

  last_state = want_state = state = MDSMap::STATE_BOOT;

  logger = 0;
  mlogger = 0;
  op_tracker.set_complaint_and_threshold(m->cct->_conf->mds_op_complaint_time,
                                         m->cct->_conf->mds_op_log_threshold);
  op_tracker.set_history_size_and_duration(m->cct->_conf->mds_op_history_size,
                                           m->cct->_conf->mds_op_history_duration);
}

MDS::~MDS() {
  Mutex::Locker lock(mds_lock);

  delete authorize_handler_service_registry;
  delete authorize_handler_cluster_registry;

  if (mdcache) { delete mdcache; mdcache = NULL; }
  if (mdlog) { delete mdlog; mdlog = NULL; }
  if (balancer) { delete balancer; balancer = NULL; }
  if (inotable) { delete inotable; inotable = NULL; }
  if (snapserver) { delete snapserver; snapserver = NULL; }
  if (snapclient) { delete snapclient; snapclient = NULL; }
  if (mdsmap) { delete mdsmap; mdsmap = 0; }

  if (server) { delete server; server = 0; }
  if (locker) { delete locker; locker = 0; }

  if (filer) { delete filer; filer = 0; }
  if (objecter) { delete objecter; objecter = 0; }

  if (logger) {
    g_ceph_context->get_perfcounters_collection()->remove(logger);
    delete logger;
    logger = 0;
  }
  if (mlogger) {
    g_ceph_context->get_perfcounters_collection()->remove(mlogger);
    delete mlogger;
    mlogger = 0;
  }
  
  if (messenger)
    delete messenger;

  if (hb) {
    cct->get_heartbeat_map()->remove_worker(hb);
  }
}

class MDSSocketHook : public AdminSocketHook {
  MDS *mds;
public:
  MDSSocketHook(MDS *m) : mds(m) {}
  bool call(std::string command, cmdmap_t& cmdmap, std::string format,
	    bufferlist& out) {
    stringstream ss;
    bool r = mds->asok_command(command, cmdmap, format, ss);
    out.append(ss);
    return r;
  }
};

bool MDS::asok_command(string command, cmdmap_t& cmdmap, string format,
		    ostream& ss)
{
  dout(1) << "asok_command: " << command << dendl;

  Formatter *f = new_formatter(format);
  if (!f)
    f = new_formatter("json-pretty");
  if (command == "status") {
    f->open_object_section("status");
    f->dump_stream("cluster_fsid") << monc->get_fsid();
    f->dump_unsigned("whoami", whoami);
    f->dump_string("state", ceph_mds_state_name(get_state()));
    f->dump_unsigned("mdsmap_epoch", mdsmap->get_epoch());
    f->close_section(); // status
  } else if (command == "dump_ops_in_flight") {
    op_tracker.dump_ops_in_flight(f);
  } else if (command == "dump_historic_ops") {
    op_tracker.dump_historic_ops(f);
  } else if (command == "session ls") {
    mds_lock.Lock();

    heartbeat_reset();

    // Dump sessions, decorated with recovery/replay status
    f->open_array_section("sessions");
    const ceph::unordered_map<entity_name_t, Session*> session_map = sessionmap.get_sessions();
    for (ceph::unordered_map<entity_name_t,Session*>::const_iterator p = session_map.begin();
         p != session_map.end();
         ++p)  {
      if (!p->first.is_client()) {
        continue;
      }

      Session *s = p->second;

      f->open_object_section("session");
      f->dump_int("id", p->first.num());

      f->dump_int("num_leases", s->leases.size());
      f->dump_int("num_caps", s->caps.size());

      f->dump_string("state", s->get_state_name());
      f->dump_int("replay_requests", is_clientreplay() ? s->get_request_count() : 0);
      f->dump_bool("reconnecting", server->waiting_for_reconnect(p->first.num()));
      f->dump_stream("inst") << s->info.inst;
      f->open_object_section("client_metadata");
      for (map<string, string>::const_iterator i = s->info.client_metadata.begin();
          i != s->info.client_metadata.end(); ++i) {
        f->dump_string(i->first.c_str(), i->second);
      }
      f->close_section(); // client_metadata
      f->close_section(); //session
    }
    f->close_section(); //sessions

    mds_lock.Unlock();
  } else if (command == "session evict") {
    std::string client_id;
    const bool got_arg = cmd_getval(g_ceph_context, cmdmap, "client_id", client_id);
    assert(got_arg == true);

    mds_lock.Lock();
    Session *session = sessionmap.get_session(entity_name_t(CEPH_ENTITY_TYPE_CLIENT,
							    strtol(client_id.c_str(), 0, 10)));
    if (session) {
      C_SaferCond on_safe;
      server->kill_session(session, &on_safe);

      mds_lock.Unlock();
      on_safe.wait();
    } else {
      dout(15) << "session " << session << " not in sessionmap!" << dendl;
      mds_lock.Unlock();
    }
  }
  f->flush(ss);
  delete f;
  return true;
}

void MDS::set_up_admin_socket()
{
  int r;
  AdminSocket *admin_socket = g_ceph_context->get_admin_socket();
  asok_hook = new MDSSocketHook(this);
  r = admin_socket->register_command("status", "status", asok_hook,
				     "high-level status of MDS");
  assert(0 == r);
  r = admin_socket->register_command("dump_ops_in_flight",
				     "dump_ops_in_flight", asok_hook,
				     "show the ops currently in flight");
  assert(0 == r);
  r = admin_socket->register_command("dump_historic_ops", "dump_historic_ops",
				     asok_hook,
				     "show slowest recent ops");
  assert(0 == r);
  r = admin_socket->register_command("session evict",
				     "session evict name=client_id,type=CephString",
				     asok_hook,
				     "Evict a CephFS client");
  assert(0 == r);
  r = admin_socket->register_command("session ls",
				     "session ls",
				     asok_hook,
				     "Enumerate connected CephFS clients");
  assert(0 == r);
}

void MDS::clean_up_admin_socket()
{
  AdminSocket *admin_socket = g_ceph_context->get_admin_socket();
  admin_socket->unregister_command("status");
  admin_socket->unregister_command("dump_ops_in_flight");
  admin_socket->unregister_command("dump_historic_ops");
  delete asok_hook;
  asok_hook = NULL;
}

const char** MDS::get_tracked_conf_keys() const
{
  static const char* KEYS[] = {
    "mds_op_complaint_time", "mds_op_log_threshold",
    "mds_op_history_size", "mds_op_history_duration",
    NULL
  };
  return KEYS;
}

void MDS::handle_conf_change(const struct md_config_t *conf,
			     const std::set <std::string> &changed)
{
  if (changed.count("mds_op_complaint_time") ||
      changed.count("mds_op_log_threshold")) {
    op_tracker.set_complaint_and_threshold(conf->mds_op_complaint_time,
                                           conf->mds_op_log_threshold);
  }
  if (changed.count("mds_op_history_size") ||
      changed.count("mds_op_history_duration")) {
    op_tracker.set_history_size_and_duration(conf->mds_op_history_size,
                                             conf->mds_op_history_duration);
  }
}

void MDS::create_logger()
{
  dout(10) << "create_logger" << dendl;
  {
    PerfCountersBuilder mds_plb(g_ceph_context, "mds", l_mds_first, l_mds_last);

    mds_plb.add_u64_counter(l_mds_request, "request");
    mds_plb.add_u64_counter(l_mds_reply, "reply");
    mds_plb.add_time_avg(l_mds_reply_latency, "reply_latency");
    mds_plb.add_u64_counter(l_mds_forward, "forward");
    
    mds_plb.add_u64_counter(l_mds_dir_fetch, "dir_fetch");
    mds_plb.add_u64_counter(l_mds_dir_commit, "dir_commit");
    mds_plb.add_u64_counter(l_mds_dir_split, "dir_split");

    mds_plb.add_u64(l_mds_inode_max, "inode_max");
    mds_plb.add_u64(l_mds_inodes, "inodes");
    mds_plb.add_u64(l_mds_inodes_top, "inodes_top");
    mds_plb.add_u64(l_mds_inodes_bottom, "inodes_bottom");
    mds_plb.add_u64(l_mds_inodes_pin_tail, "inodes_pin_tail");  
    mds_plb.add_u64(l_mds_inodes_pinned, "inodes_pinned");
    mds_plb.add_u64_counter(l_mds_inodes_expired, "inodes_expired");
    mds_plb.add_u64_counter(l_mds_inodes_with_caps, "inodes_with_caps");
    mds_plb.add_u64_counter(l_mds_caps, "caps");
    mds_plb.add_u64(l_mds_subtrees, "subtrees");
    
    mds_plb.add_u64_counter(l_mds_traverse, "traverse"); 
    mds_plb.add_u64_counter(l_mds_traverse_hit, "traverse_hit");
    mds_plb.add_u64_counter(l_mds_traverse_forward, "traverse_forward");
    mds_plb.add_u64_counter(l_mds_traverse_discover, "traverse_discover");
    mds_plb.add_u64_counter(l_mds_traverse_dir_fetch, "traverse_dir_fetch");
    mds_plb.add_u64_counter(l_mds_traverse_remote_ino, "traverse_remote_ino");
    mds_plb.add_u64_counter(l_mds_traverse_lock, "traverse_lock");
    
    mds_plb.add_u64(l_mds_load_cent, "load_cent");
    mds_plb.add_u64(l_mds_dispatch_queue_len, "q");
    
    mds_plb.add_u64_counter(l_mds_exported, "exported");
    mds_plb.add_u64_counter(l_mds_exported_inodes, "exported_inodes");
    mds_plb.add_u64_counter(l_mds_imported, "imported");
    mds_plb.add_u64_counter(l_mds_imported_inodes, "imported_inodes");
    logger = mds_plb.create_perf_counters();
    g_ceph_context->get_perfcounters_collection()->add(logger);
  }

  {
    PerfCountersBuilder mdm_plb(g_ceph_context, "mds_mem", l_mdm_first, l_mdm_last);
    mdm_plb.add_u64(l_mdm_ino, "ino");
    mdm_plb.add_u64_counter(l_mdm_inoa, "ino+");
    mdm_plb.add_u64_counter(l_mdm_inos, "ino-");
    mdm_plb.add_u64(l_mdm_dir, "dir");
    mdm_plb.add_u64_counter(l_mdm_dira, "dir+");
    mdm_plb.add_u64_counter(l_mdm_dirs, "dir-");
    mdm_plb.add_u64(l_mdm_dn, "dn");
    mdm_plb.add_u64_counter(l_mdm_dna, "dn+");
    mdm_plb.add_u64_counter(l_mdm_dns, "dn-");
    mdm_plb.add_u64(l_mdm_cap, "cap");
    mdm_plb.add_u64_counter(l_mdm_capa, "cap+");
    mdm_plb.add_u64_counter(l_mdm_caps, "cap-");
    mdm_plb.add_u64(l_mdm_rss, "rss");
    mdm_plb.add_u64(l_mdm_heap, "heap");
    mdm_plb.add_u64(l_mdm_malloc, "malloc");
    mdm_plb.add_u64(l_mdm_buf, "buf");
    mlogger = mdm_plb.create_perf_counters();
    g_ceph_context->get_perfcounters_collection()->add(mlogger);
  }

  mdlog->create_logger();
  server->create_logger();
}



MDSTableClient *MDS::get_table_client(int t)
{
  switch (t) {
  case TABLE_ANCHOR: return NULL;
  case TABLE_SNAP: return snapclient;
  default: assert(0);
  }
}

MDSTableServer *MDS::get_table_server(int t)
{
  switch (t) {
  case TABLE_ANCHOR: return NULL;
  case TABLE_SNAP: return snapserver;
  default: assert(0);
  }
}








void MDS::send_message(Message *m, Connection *c)
{ 
  assert(c);
  c->send_message(m);
}


void MDS::send_message_mds(Message *m, int mds)
{
  if (!mdsmap->is_up(mds)) {
    dout(10) << "send_message_mds mds." << mds << " not up, dropping " << *m << dendl;
    m->put();
    return;
  }

  // send mdsmap first?
  if (mds != whoami && peer_mdsmap_epoch[mds] < mdsmap->get_epoch()) {
    messenger->send_message(new MMDSMap(monc->get_fsid(), mdsmap), 
			    mdsmap->get_inst(mds));
    peer_mdsmap_epoch[mds] = mdsmap->get_epoch();
  }

  // send message
  messenger->send_message(m, mdsmap->get_inst(mds));
}

void MDS::forward_message_mds(Message *m, int mds)
{
  assert(mds != whoami);

  // client request?
  if (m->get_type() == CEPH_MSG_CLIENT_REQUEST &&
      (static_cast<MClientRequest*>(m))->get_source().is_client()) {
    MClientRequest *creq = static_cast<MClientRequest*>(m);
    creq->inc_num_fwd();    // inc forward counter

    /*
     * don't actually forward if non-idempotent!
     * client has to do it.  although the MDS will ignore duplicate requests,
     * the affected metadata may migrate, in which case the new authority
     * won't have the metareq_id in the completed request map.
     */
    // NEW: always make the client resend!  
    bool client_must_resend = true;  //!creq->can_forward();

    // tell the client where it should go
    messenger->send_message(new MClientRequestForward(creq->get_tid(), mds, creq->get_num_fwd(),
						      client_must_resend),
			    creq->get_source_inst());
    
    if (client_must_resend) {
      m->put();
      return; 
    }
  }

  // these are the only types of messages we should be 'forwarding'; they
  // explicitly encode their source mds, which gets clobbered when we resend
  // them here.
  assert(m->get_type() == MSG_MDS_DIRUPDATE ||
	 m->get_type() == MSG_MDS_EXPORTDIRDISCOVER);

  // send mdsmap first?
  if (peer_mdsmap_epoch[mds] < mdsmap->get_epoch()) {
    messenger->send_message(new MMDSMap(monc->get_fsid(), mdsmap), 
			    mdsmap->get_inst(mds));
    peer_mdsmap_epoch[mds] = mdsmap->get_epoch();
  }

  messenger->send_message(m, mdsmap->get_inst(mds));
}



void MDS::send_message_client_counted(Message *m, client_t client)
{
  Session *session =  sessionmap.get_session(entity_name_t::CLIENT(client.v));
  if (session) {
    send_message_client_counted(m, session);
  } else {
    dout(10) << "send_message_client_counted no session for client." << client << " " << *m << dendl;
  }
}

void MDS::send_message_client_counted(Message *m, Connection *connection)
{
  Session *session = static_cast<Session *>(connection->get_priv());
  if (session) {
    session->put();  // do not carry ref
    send_message_client_counted(m, session);
  } else {
    dout(10) << "send_message_client_counted has no session for " << m->get_source_inst() << dendl;
    // another Connection took over the Session
  }
}

void MDS::send_message_client_counted(Message *m, Session *session)
{
  version_t seq = session->inc_push_seq();
  dout(10) << "send_message_client_counted " << session->info.inst.name << " seq "
	   << seq << " " << *m << dendl;
  if (session->connection) {
    session->connection->send_message(m);
  } else {
    session->preopen_out_queue.push_back(m);
  }
}

void MDS::send_message_client(Message *m, Session *session)
{
  dout(10) << "send_message_client " << session->info.inst << " " << *m << dendl;
  if (session->connection) {
    session->connection->send_message(m);
  } else {
    session->preopen_out_queue.push_back(m);
  }
}

int MDS::init(MDSMap::DaemonState wanted_state)
{
  dout(10) << sizeof(MDSCacheObject) << "\tMDSCacheObject" << dendl;
  dout(10) << sizeof(CInode) << "\tCInode" << dendl;
  dout(10) << sizeof(elist<void*>::item) << "\t elist<>::item   *7=" << 7*sizeof(elist<void*>::item) << dendl;
  dout(10) << sizeof(inode_t) << "\t inode_t " << dendl;
  dout(10) << sizeof(nest_info_t) << "\t  nest_info_t " << dendl;
  dout(10) << sizeof(frag_info_t) << "\t  frag_info_t " << dendl;
  dout(10) << sizeof(SimpleLock) << "\t SimpleLock   *5=" << 5*sizeof(SimpleLock) << dendl;
  dout(10) << sizeof(ScatterLock) << "\t ScatterLock  *3=" << 3*sizeof(ScatterLock) << dendl;
  dout(10) << sizeof(CDentry) << "\tCDentry" << dendl;
  dout(10) << sizeof(elist<void*>::item) << "\t elist<>::item" << dendl;
  dout(10) << sizeof(SimpleLock) << "\t SimpleLock" << dendl;
  dout(10) << sizeof(CDir) << "\tCDir " << dendl;
  dout(10) << sizeof(elist<void*>::item) << "\t elist<>::item   *2=" << 2*sizeof(elist<void*>::item) << dendl;
  dout(10) << sizeof(fnode_t) << "\t fnode_t " << dendl;
  dout(10) << sizeof(nest_info_t) << "\t  nest_info_t *2" << dendl;
  dout(10) << sizeof(frag_info_t) << "\t  frag_info_t *2" << dendl;
  dout(10) << sizeof(Capability) << "\tCapability " << dendl;
  dout(10) << sizeof(xlist<void*>::item) << "\t xlist<>::item   *2=" << 2*sizeof(xlist<void*>::item) << dendl;

  objecter->init();

  messenger->add_dispatcher_tail(objecter);
  messenger->add_dispatcher_tail(&beacon);
  messenger->add_dispatcher_tail(this);

  // get monmap
  monc->set_messenger(messenger);

  monc->set_want_keys(CEPH_ENTITY_TYPE_MON | CEPH_ENTITY_TYPE_OSD | CEPH_ENTITY_TYPE_MDS);
  monc->init();

  finisher.start();

  // tell monc about log_client so it will know about mon session resets
  monc->set_log_client(&log_client);
  
  int r = monc->authenticate();
  if (r < 0) {
    derr << "ERROR: failed to authenticate: " << cpp_strerror(-r) << dendl;
    mds_lock.Lock();
    suicide();
    mds_lock.Unlock();
    return r;
  }
  while (monc->wait_auth_rotating(30.0) < 0) {
    derr << "unable to obtain rotating service keys; retrying" << dendl;
  }
  objecter->start();

  mds_lock.Lock();
  if (want_state == CEPH_MDS_STATE_DNE) {
    mds_lock.Unlock();
    return 0;
  }

  monc->sub_want("mdsmap", 0, 0);
  monc->renew_subs();

  mds_lock.Unlock();

  // verify that osds support tmap2omap
  while (true) {
    objecter->maybe_request_map();
    objecter->wait_for_osd_map();
    const OSDMap *osdmap = objecter->get_osdmap_read();
    uint64_t osd_features = osdmap->get_up_osd_features();
    if (osd_features & CEPH_FEATURE_OSD_TMAP2OMAP) {
      objecter->put_osdmap_read();
      break;
    }
    if (osdmap->get_num_up_osds() > 0) {
        derr << "*** one or more OSDs do not support TMAP2OMAP; upgrade OSDs before starting MDS (or downgrade MDS) ***" << dendl;
    } else {
        derr << "*** no OSDs are up as of epoch " << osdmap->get_epoch() << ", waiting" << dendl;
    }
    objecter->put_osdmap_read();
    sleep(10);
  }

  mds_lock.Lock();
  if (want_state == MDSMap::STATE_DNE) {
    suicide();  // we could do something more graceful here
  }

  timer.init();

  if (wanted_state==MDSMap::STATE_BOOT && g_conf->mds_standby_replay) {
    wanted_state = MDSMap::STATE_STANDBY_REPLAY;
  }

  // starting beacon.  this will induce an MDSMap from the monitor
  want_state = wanted_state;
  if (wanted_state==MDSMap::STATE_STANDBY_REPLAY ||
      wanted_state==MDSMap::STATE_ONESHOT_REPLAY) {
    g_conf->set_val_or_die("mds_standby_replay", "true");
    g_conf->apply_changes(NULL);
    if ( wanted_state == MDSMap::STATE_ONESHOT_REPLAY &&
        (g_conf->mds_standby_for_rank == -1) &&
        g_conf->mds_standby_for_name.empty()) {
      // uh-oh, must specify one or the other!
      dout(0) << "Specified oneshot replay mode but not an MDS!" << dendl;
      suicide();
    }
    want_state = MDSMap::STATE_BOOT;
    standby_type = wanted_state;
  }

  standby_for_rank = g_conf->mds_standby_for_rank;
  standby_for_name.assign(g_conf->mds_standby_for_name);

  if (wanted_state == MDSMap::STATE_STANDBY_REPLAY &&
      standby_for_rank == -1) {
    if (standby_for_name.empty())
      standby_for_rank = MDSMap::MDS_STANDBY_ANY;
    else
      standby_for_rank = MDSMap::MDS_STANDBY_NAME;
  } else if (standby_type == MDSMap::STATE_NULL && !standby_for_name.empty())
    standby_for_rank = MDSMap::MDS_MATCHED_ACTIVE;

  beacon.init(mdsmap, want_state, standby_for_rank, standby_for_name);
  whoami = -1;
  messenger->set_myname(entity_name_t::MDS(whoami));
  
  // schedule tick
  reset_tick();

  // Start handler for finished_queue
  progress_thread.create();

  create_logger();
  set_up_admin_socket();
  g_conf->add_observer(this);

  mds_lock.Unlock();

  return 0;
}

void MDS::reset_tick()
{
  // cancel old
  if (tick_event) timer.cancel_event(tick_event);

  // schedule
  tick_event = new C_MDS_Tick(this);
  timer.add_event_after(g_conf->mds_tick_interval, tick_event);
}

void MDS::tick()
{
  heartbeat_reset();

  tick_event = 0;

  // reschedule
  reset_tick();

  if (beacon.is_laggy()) {
    dout(5) << "tick bailing out since we seem laggy" << dendl;
    return;
  } else {
    // Wake up thread in case we use to be laggy and have waiting_for_nolaggy
    // messages to progress.
    progress_thread.signal();
  }

  // make sure mds log flushes, trims periodically
  mdlog->flush();

  if (is_active() || is_stopping()) {
    mdcache->trim();
    mdcache->trim_client_leases();
    mdcache->check_memory_usage();
    mdlog->trim();  // NOT during recovery!
  }

  // log
  utime_t now = ceph_clock_now(g_ceph_context);
  mds_load_t load = balancer->get_load(now);
  
  if (logger) {
    req_rate = logger->get(l_mds_request);
    
    logger->set(l_mds_load_cent, 100 * load.mds_load());
    logger->set(l_mds_dispatch_queue_len, messenger->get_dispatch_queue_len());
    logger->set(l_mds_subtrees, mdcache->num_subtrees());

    mdcache->log_stat();
  }

  // ...
  if (is_clientreplay() || is_active() || is_stopping()) {
    locker->tick();
    server->find_idle_sessions();
  }
  
  if (is_reconnect())
    server->reconnect_tick();
  
  if (is_active()) {
    balancer->tick();
    mdcache->find_stale_fragment_freeze();
    mdcache->migrator->find_stale_export_freeze();
    if (snapserver)
      snapserver->check_osd_map(false);
  }

  // Expose ourselves to Beacon to update health indicators
  beacon.notify_health(this);

  check_ops_in_flight();
}

void MDS::check_ops_in_flight()
{
  vector<string> warnings;
  if (op_tracker.check_ops_in_flight(warnings)) {
    for (vector<string>::iterator i = warnings.begin();
        i != warnings.end();
        ++i) {
      clog->warn() << *i;
    }
  }
  return;
}


/* This function DOES put the passed message before returning*/
void MDS::handle_command(MMonCommand *m)
{
  dout(10) << "handle_command args: " << m->cmd << dendl;
  if (m->cmd[0] == "injectargs") {
    if (m->cmd.size() < 2) {
      derr << "Ignoring empty injectargs!" << dendl;
    }
    else {
      std::ostringstream oss;
      mds_lock.Unlock();
      g_conf->injectargs(m->cmd[1], &oss);
      mds_lock.Lock();
      derr << "injectargs:" << dendl;
      derr << oss.str() << dendl;
    }
  }
  else if (m->cmd[0] == "dumpcache") {
    if (m->cmd.size() > 1)
      mdcache->dump_cache(m->cmd[1].c_str());
    else
      mdcache->dump_cache();
  }
  else if (m->cmd[0] == "exit") {
    suicide();
  }
  else if (m->cmd[0] == "respawn") {
    respawn();
  }
  else if (m->cmd[0] == "session" && m->cmd[1] == "kill") {
    Session *session = sessionmap.get_session(entity_name_t(CEPH_ENTITY_TYPE_CLIENT,
							    strtol(m->cmd[2].c_str(), 0, 10)));
    if (session)
      server->kill_session(session, NULL);
    else
      dout(15) << "session " << session << " not in sessionmap!" << dendl;
  } else if (m->cmd[0] == "issue_caps") {
    long inum = strtol(m->cmd[1].c_str(), 0, 10);
    CInode *in = mdcache->get_inode(inodeno_t(inum));
    if (in) {
      bool r = locker->issue_caps(in);
      dout(20) << "called issue_caps on inode "  << inum
	       << " with result " << r << dendl;
    } else dout(15) << "inode " << inum << " not in mdcache!" << dendl;
  } else if (m->cmd[0] == "try_eval") {
    long inum = strtol(m->cmd[1].c_str(), 0, 10);
    int mask = strtol(m->cmd[2].c_str(), 0, 10);
    CInode * ino = mdcache->get_inode(inodeno_t(inum));
    if (ino) {
      locker->try_eval(ino, mask);
      dout(20) << "try_eval(" << inum << ", " << mask << ")" << dendl;
    } else dout(15) << "inode " << inum << " not in mdcache!" << dendl;
  } else if (m->cmd[0] == "fragment_dir") {
    if (m->cmd.size() == 4) {
      filepath fp(m->cmd[1].c_str());
      CInode *in = mdcache->cache_traverse(fp);
      if (in) {
	frag_t fg;
	if (fg.parse(m->cmd[2].c_str())) {
	  CDir *dir = in->get_dirfrag(fg);
	  if (dir) {
	    if (dir->is_auth()) {
	      int by = atoi(m->cmd[3].c_str());
	      if (by)
		mdcache->split_dir(dir, by);
	      else
		dout(0) << "need to split by >0 bits" << dendl;
	    } else dout(0) << "dir " << dir->dirfrag() << " not auth" << dendl;
	  } else dout(0) << "dir " << in->ino() << " " << fg << " dne" << dendl;
	} else dout(0) << " frag " << m->cmd[2] << " does not parse" << dendl;
      } else dout(0) << "path " << fp << " not found" << dendl;
    } else dout(0) << "bad syntax" << dendl;
  } else if (m->cmd[0] == "merge_dir") {
    if (m->cmd.size() == 3) {
      filepath fp(m->cmd[1].c_str());
      CInode *in = mdcache->cache_traverse(fp);
      if (in) {
	frag_t fg;
	if (fg.parse(m->cmd[2].c_str())) {
	  mdcache->merge_dir(in, fg);
	} else dout(0) << " frag " << m->cmd[2] << " does not parse" << dendl;
      } else dout(0) << "path " << fp << " not found" << dendl;
    } else dout(0) << "bad syntax" << dendl;
  } else if (m->cmd[0] == "export_dir") {
    if (m->cmd.size() == 3) {
      filepath fp(m->cmd[1].c_str());
      int target = atoi(m->cmd[2].c_str());
      if (target != whoami && mdsmap->is_up(target) && mdsmap->is_in(target)) {
	CInode *in = mdcache->cache_traverse(fp);
	if (in) {
	  CDir *dir = in->get_dirfrag(frag_t());
	  if (dir && dir->is_auth()) {
	    mdcache->migrator->export_dir(dir, target);
	  } else dout(0) << "bad export_dir path dirfrag frag_t() or dir not auth" << dendl;
	} else dout(0) << "bad export_dir path" << dendl;
      } else dout(0) << "bad export_dir target syntax" << dendl;
    } else dout(0) << "bad export_dir syntax" << dendl;
  } 
  else if (m->cmd[0] == "cpu_profiler") {
    ostringstream ss;
    cpu_profiler_handle_command(m->cmd, ss);
    clog->info() << ss.str();
  }
 else if (m->cmd[0] == "heap") {
   if (!ceph_using_tcmalloc())
     clog->info() << "tcmalloc not enabled, can't use heap profiler commands\n";
   else {
     ostringstream ss;
     vector<std::string> cmdargs;
     cmdargs.insert(cmdargs.begin(), m->cmd.begin()+1, m->cmd.end());
     ceph_heap_profiler_handle_command(cmdargs, ss);
     clog->info() << ss.str();
   }
 } else dout(0) << "unrecognized command! " << m->cmd << dendl;
  m->put();
}

/* This function deletes the passed message before returning. */
void MDS::handle_mds_map(MMDSMap *m)
{
  version_t epoch = m->get_epoch();
  dout(5) << "handle_mds_map epoch " << epoch << " from " << m->get_source() << dendl;

  // note source's map version
  if (m->get_source().is_mds() && 
      peer_mdsmap_epoch[m->get_source().num()] < epoch) {
    dout(15) << " peer " << m->get_source()
	     << " has mdsmap epoch >= " << epoch
	     << dendl;
    peer_mdsmap_epoch[m->get_source().num()] = epoch;
  }

  // is it new?
  if (epoch <= mdsmap->get_epoch()) {
    dout(5) << " old map epoch " << epoch << " <= " << mdsmap->get_epoch() 
	    << ", discarding" << dendl;
    m->put();
    return;
  }

  // keep old map, for a moment
  MDSMap *oldmap = mdsmap;
  int oldwhoami = whoami;
  MDSMap::DaemonState oldstate = state;
  entity_addr_t addr;

  // decode and process
  mdsmap = new MDSMap;
  mdsmap->decode(m->get_encoded());

  monc->sub_got("mdsmap", mdsmap->get_epoch());

  // verify compatset
  CompatSet mdsmap_compat(get_mdsmap_compat_set_all());
  dout(10) << "     my compat " << mdsmap_compat << dendl;
  dout(10) << " mdsmap compat " << mdsmap->compat << dendl;
  if (!mdsmap_compat.writeable(mdsmap->compat)) {
    dout(0) << "handle_mds_map mdsmap compatset " << mdsmap->compat
	    << " not writeable with daemon features " << mdsmap_compat
	    << ", killing myself" << dendl;
    suicide();
    goto out;
  }

  // see who i am
  addr = messenger->get_myaddr();
  whoami = mdsmap->get_rank_gid(monc->get_global_id());
  state = mdsmap->get_state_gid(monc->get_global_id());
  incarnation = mdsmap->get_inc_gid(monc->get_global_id());
  dout(10) << "map says i am " << addr << " mds." << whoami << "." << incarnation
	   << " state " << ceph_mds_state_name(state) << dendl;

  // mark down any failed peers
  for (map<uint64_t,MDSMap::mds_info_t>::const_iterator p = oldmap->get_mds_info().begin();
       p != oldmap->get_mds_info().end();
       ++p) {
    if (mdsmap->get_mds_info().count(p->first) == 0) {
      dout(10) << " peer mds gid " << p->first << " removed from map" << dendl;
      messenger->mark_down(p->second.addr);
    }
  }

  if (state != oldstate)
    last_state = oldstate;

  if (state == MDSMap::STATE_STANDBY) {
    state = MDSMap::STATE_STANDBY;
    set_want_state(state);
    dout(1) << "handle_mds_map standby" << dendl;

    if (standby_type) // we want to be in standby_replay or oneshot_replay!
      request_state(standby_type);

    goto out;
  } else if (state == MDSMap::STATE_STANDBY_REPLAY) {
    if (standby_type != MDSMap::STATE_NULL && standby_type != MDSMap::STATE_STANDBY_REPLAY) {
      set_want_state(standby_type);
      beacon.send();
      state = oldstate;
      goto out;
    }
  }

  if (whoami < 0) {
    if (state == MDSMap::STATE_STANDBY_REPLAY ||
        state == MDSMap::STATE_ONESHOT_REPLAY) {
      // fill in whoami from standby-for-rank. If we let this be changed
      // the logic used to set it here will need to be adjusted.
      whoami = mdsmap->get_mds_info_gid(monc->get_global_id()).standby_for_rank;
    } else {
      if (want_state == MDSMap::STATE_STANDBY) {
        dout(10) << "dropped out of mdsmap, try to re-add myself" << dendl;
        state = MDSMap::STATE_BOOT;
        set_want_state(state);
        goto out;
      }
      if (want_state == MDSMap::STATE_BOOT) {
        dout(10) << "not in map yet" << dendl;
      } else {
	// did i get kicked by someone else?
	if (g_conf->mds_enforce_unique_name) {
	  if (uint64_t existing = mdsmap->find_mds_gid_by_name(name)) {
	    MDSMap::mds_info_t& i = mdsmap->get_info_gid(existing);
	    if (i.global_id > monc->get_global_id()) {
	      dout(1) << "handle_mds_map i (" << addr
		      << ") dne in the mdsmap, new instance has larger gid " << i.global_id
		      << ", suicide" << dendl;
	      suicide();
	      goto out;
	    }
	  }
	}

        dout(1) << "handle_mds_map i (" << addr
            << ") dne in the mdsmap, respawning myself" << dendl;
        respawn();
      }
      goto out;
    }
  }

  // ??

  if (oldwhoami != whoami || oldstate != state) {
    // update messenger.
    if (state == MDSMap::STATE_STANDBY_REPLAY || state == MDSMap::STATE_ONESHOT_REPLAY) {
      dout(1) << "handle_mds_map i am now mds." << monc->get_global_id() << "." << incarnation
	      << "replaying mds." << whoami << "." << incarnation << dendl;
      messenger->set_myname(entity_name_t::MDS(monc->get_global_id()));
    } else {
      dout(1) << "handle_mds_map i am now mds." << whoami << "." << incarnation << dendl;
      messenger->set_myname(entity_name_t::MDS(whoami));
    }
  }

  // tell objecter my incarnation
  if (objecter->get_client_incarnation() != incarnation)
    objecter->set_client_incarnation(incarnation);

  // for debug
  if (g_conf->mds_dump_cache_on_map)
    mdcache->dump_cache();

  // did it change?
  if (oldstate != state) {
    dout(1) << "handle_mds_map state change "
	    << ceph_mds_state_name(oldstate) << " --> "
	    << ceph_mds_state_name(state) << dendl;
    set_want_state(state);

    if (oldstate == MDSMap::STATE_STANDBY_REPLAY) {
        dout(10) << "Monitor activated us! Deactivating replay loop" << dendl;
        assert (state == MDSMap::STATE_REPLAY);
    } else {
      // did i just recover?
      if ((is_active() || is_clientreplay()) &&
          (oldstate == MDSMap::STATE_CREATING ||
	   oldstate == MDSMap::STATE_REJOIN ||
	   oldstate == MDSMap::STATE_RECONNECT))
        recovery_done(oldstate);

      if (is_active()) {
        active_start();
      } else if (is_any_replay()) {
        replay_start();
      } else if (is_resolve()) {
        resolve_start();
      } else if (is_reconnect()) {
        reconnect_start();
      } else if (is_rejoin()) {
	rejoin_start();
      } else if (is_clientreplay()) {
        clientreplay_start();
      } else if (is_creating()) {
        boot_create();
      } else if (is_starting()) {
        boot_start();
      } else if (is_stopping()) {
        assert(oldstate == MDSMap::STATE_ACTIVE);
        stopping_start();
      }
    }
  }
  
  // RESOLVE
  // is someone else newly resolving?
  if (is_resolve() || is_reconnect() || is_rejoin() ||
      is_clientreplay() || is_active() || is_stopping()) {
    if (!oldmap->is_resolving() && mdsmap->is_resolving()) {
      set<int> resolve;
      mdsmap->get_mds_set(resolve, MDSMap::STATE_RESOLVE);
      dout(10) << " resolve set is " << resolve << dendl;
      calc_recovery_set();
      mdcache->send_resolves();
    }
  }
  
  // REJOIN
  // is everybody finally rejoining?
  if (is_rejoin() || is_clientreplay() || is_active() || is_stopping()) {
    // did we start?
    if (!oldmap->is_rejoining() && mdsmap->is_rejoining())
      rejoin_joint_start();

    // did we finish?
    if (g_conf->mds_dump_cache_after_rejoin &&
	oldmap->is_rejoining() && !mdsmap->is_rejoining()) 
      mdcache->dump_cache();      // for DEBUG only

    if (oldstate >= MDSMap::STATE_REJOIN) {
      // ACTIVE|CLIENTREPLAY|REJOIN => we can discover from them.
      set<int> olddis, dis;
      oldmap->get_mds_set(olddis, MDSMap::STATE_ACTIVE);
      oldmap->get_mds_set(olddis, MDSMap::STATE_CLIENTREPLAY);
      oldmap->get_mds_set(olddis, MDSMap::STATE_REJOIN);
      mdsmap->get_mds_set(dis, MDSMap::STATE_ACTIVE);
      mdsmap->get_mds_set(dis, MDSMap::STATE_CLIENTREPLAY);
      mdsmap->get_mds_set(dis, MDSMap::STATE_REJOIN);
      for (set<int>::iterator p = dis.begin(); p != dis.end(); ++p)
	if (*p != whoami &&            // not me
	    olddis.count(*p) == 0) {  // newly so?
	  mdcache->kick_discovers(*p);
	  mdcache->kick_open_ino_peers(*p);
	}
    }
  }

  if (oldmap->is_degraded() && !mdsmap->is_degraded() && state >= MDSMap::STATE_ACTIVE)
    dout(1) << "cluster recovered." << dendl;

  // did someone go active?
  if (oldstate >= MDSMap::STATE_CLIENTREPLAY &&
      (is_clientreplay() || is_active() || is_stopping())) {
    set<int> oldactive, active;
    oldmap->get_mds_set(oldactive, MDSMap::STATE_ACTIVE);
    oldmap->get_mds_set(oldactive, MDSMap::STATE_CLIENTREPLAY);
    mdsmap->get_mds_set(active, MDSMap::STATE_ACTIVE);
    mdsmap->get_mds_set(active, MDSMap::STATE_CLIENTREPLAY);
    for (set<int>::iterator p = active.begin(); p != active.end(); ++p) 
      if (*p != whoami &&            // not me
	  oldactive.count(*p) == 0)  // newly so?
	handle_mds_recovery(*p);
  }

  // did someone fail?
  if (true) {
    // new failed?
    set<int> oldfailed, failed;
    oldmap->get_failed_mds_set(oldfailed);
    mdsmap->get_failed_mds_set(failed);
    for (set<int>::iterator p = failed.begin(); p != failed.end(); ++p)
      if (oldfailed.count(*p) == 0) {
	messenger->mark_down(oldmap->get_inst(*p).addr);
	handle_mds_failure(*p);
      }
    
    // or down then up?
    //  did their addr/inst change?
    set<int> up;
    mdsmap->get_up_mds_set(up);
    for (set<int>::iterator p = up.begin(); p != up.end(); ++p) 
      if (oldmap->have_inst(*p) &&
	  oldmap->get_inst(*p) != mdsmap->get_inst(*p)) {
	messenger->mark_down(oldmap->get_inst(*p).addr);
	handle_mds_failure(*p);
      }
  }
  if (is_clientreplay() || is_active() || is_stopping()) {
    // did anyone stop?
    set<int> oldstopped, stopped;
    oldmap->get_stopped_mds_set(oldstopped);
    mdsmap->get_stopped_mds_set(stopped);
    for (set<int>::iterator p = stopped.begin(); p != stopped.end(); ++p) 
      if (oldstopped.count(*p) == 0)      // newly so?
	mdcache->migrator->handle_mds_failure_or_stop(*p);
  }

  if (!is_any_replay())
    balancer->try_rebalance();

  {
    map<epoch_t,list<MDSInternalContextBase*> >::iterator p = waiting_for_mdsmap.begin();
    while (p != waiting_for_mdsmap.end() && p->first <= mdsmap->get_epoch()) {
      list<MDSInternalContextBase*> ls;
      ls.swap(p->second);
      waiting_for_mdsmap.erase(p++);
      finish_contexts(g_ceph_context, ls);
    }
  }

 out:
  beacon.notify_mdsmap(mdsmap);

  m->put();
  delete oldmap;
}

void MDS::bcast_mds_map()
{
  dout(7) << "bcast_mds_map " << mdsmap->get_epoch() << dendl;

  // share the map with mounted clients
  set<Session*> clients;
  sessionmap.get_client_session_set(clients);
  for (set<Session*>::const_iterator p = clients.begin();
       p != clients.end();
       ++p) 
    (*p)->connection->send_message(new MMDSMap(monc->get_fsid(), mdsmap));
  last_client_mdsmap_bcast = mdsmap->get_epoch();
}


void MDS::request_state(MDSMap::DaemonState s)
{
  dout(3) << "request_state " << ceph_mds_state_name(s) << dendl;
  set_want_state(s);
  beacon.send();
}


class C_MDS_CreateFinish : public MDSInternalContext {
public:
  C_MDS_CreateFinish(MDS *m) : MDSInternalContext(m) {}
  void finish(int r) { mds->creating_done(); }
};

void MDS::boot_create()
{
  dout(3) << "boot_create" << dendl;

  MDSGatherBuilder fin(g_ceph_context, new C_MDS_CreateFinish(this));

  mdcache->init_layouts();

  // start with a fresh journal
  dout(10) << "boot_create creating fresh journal" << dendl;
  mdlog->create(fin.new_sub());

  // open new journal segment, but do not journal subtree map (yet)
  mdlog->prepare_new_segment();

  if (whoami == mdsmap->get_root()) {
    dout(3) << "boot_create creating fresh hierarchy" << dendl;
    mdcache->create_empty_hierarchy(fin.get());
  }

  dout(3) << "boot_create creating mydir hierarchy" << dendl;
  mdcache->create_mydir_hierarchy(fin.get());

  // fixme: fake out inotable (reset, pretend loaded)
  dout(10) << "boot_create creating fresh inotable table" << dendl;
  inotable->reset();
  inotable->save(fin.new_sub());

  // write empty sessionmap
  sessionmap.save(fin.new_sub());

  // initialize tables
  if (mdsmap->get_tableserver() == whoami) {
    dout(10) << "boot_create creating fresh snaptable" << dendl;
    snapserver->reset();
    snapserver->save(fin.new_sub());
  }

  assert(g_conf->mds_kill_create_at != 1);

  // ok now journal it
  mdlog->journal_segment_subtree_map(fin.new_sub());
  mdlog->flush();

  fin.activate();
}

void MDS::creating_done()
{
  dout(1)<< "creating_done" << dendl;
  request_state(MDSMap::STATE_ACTIVE);
}


class C_MDS_BootStart : public MDSInternalContext {
  MDS::BootStep nextstep;
public:
  C_MDS_BootStart(MDS *m, MDS::BootStep n) : MDSInternalContext(m), nextstep(n) {}
  void finish(int r) {
    mds->boot_start(nextstep, r);
  }
};


void MDS::boot_start(BootStep step, int r)
{
  // Handle errors from previous step
  if (r < 0) {
    if (is_standby_replay() && (r == -EAGAIN)) {
      dout(0) << "boot_start encountered an error EAGAIN"
              << ", respawning since we fell behind journal" << dendl;
      respawn();
    } else {
      dout(0) << "boot_start encountered an error, failing" << dendl;
      suicide();
      return;
    }
  }

  assert(is_starting() || is_any_replay());

  switch(step) {
    case MDS_BOOT_INITIAL:
      {
        mdcache->init_layouts();

        MDSGatherBuilder gather(g_ceph_context,
            new C_MDS_BootStart(this, MDS_BOOT_OPEN_ROOT));
        dout(2) << "boot_start " << step << ": opening inotable" << dendl;
        inotable->load(gather.new_sub());

        dout(2) << "boot_start " << step << ": opening sessionmap" << dendl;
        sessionmap.load(gather.new_sub());

        dout(2) << "boot_start " << step << ": opening mds log" << dendl;
        mdlog->open(gather.new_sub());

        if (mdsmap->get_tableserver() == whoami) {
          dout(2) << "boot_start " << step << ": opening snap table" << dendl;
          snapserver->load(gather.new_sub());
        }

        gather.activate();
      }
      break;
    case MDS_BOOT_OPEN_ROOT:
      {
        dout(2) << "boot_start " << step << ": loading/discovering base inodes" << dendl;

        MDSGatherBuilder gather(g_ceph_context,
            new C_MDS_BootStart(this, MDS_BOOT_PREPARE_LOG));

        mdcache->open_mydir_inode(gather.new_sub());

        if (is_starting() ||
            whoami == mdsmap->get_root()) {  // load root inode off disk if we are auth
          mdcache->open_root_inode(gather.new_sub());
        } else {
          // replay.  make up fake root inode to start with
          mdcache->create_root_inode();
        }
        gather.activate();
      }
      break;
    case MDS_BOOT_PREPARE_LOG:
      if (is_any_replay()) {
        dout(2) << "boot_start " << step << ": replaying mds log" << dendl;
        mdlog->replay(new C_MDS_BootStart(this, MDS_BOOT_REPLAY_DONE));
      } else {
        dout(2) << "boot_start " << step << ": positioning at end of old mds log" << dendl;
        mdlog->append();
        starting_done();
      }
      break;
    case MDS_BOOT_REPLAY_DONE:
      assert(is_any_replay());
      replay_done();
      break;
  }
}

void MDS::starting_done()
{
  dout(3) << "starting_done" << dendl;
  assert(is_starting());
  request_state(MDSMap::STATE_ACTIVE);

  mdcache->open_root();

  // start new segment
  mdlog->start_new_segment();
}


void MDS::calc_recovery_set()
{
  // initialize gather sets
  set<int> rs;
  mdsmap->get_recovery_mds_set(rs);
  rs.erase(whoami);
  mdcache->set_recovery_set(rs);

  dout(1) << " recovery set is " << rs << dendl;
}


void MDS::replay_start()
{
  dout(1) << "replay_start" << dendl;

  if (is_standby_replay())
    standby_replaying = true;
  
  standby_type = MDSMap::STATE_NULL;

  calc_recovery_set();

  // Check if we need to wait for a newer OSD map before starting
  Context *fin = new C_OnFinisher(new C_IO_Wrapper(this, new C_MDS_BootStart(this, MDS_BOOT_INITIAL)), &finisher);
  bool const ready = objecter->wait_for_map(
      mdsmap->get_last_failure_osd_epoch(),
      fin);

  if (ready) {
    delete fin;
    boot_start();
  } else {
    dout(1) << " waiting for osdmap " << mdsmap->get_last_failure_osd_epoch() 
	    << " (which blacklists prior instance)" << dendl;
  }
}


class MDS::C_MDS_StandbyReplayRestartFinish : public MDSIOContext {
  uint64_t old_read_pos;
public:
  C_MDS_StandbyReplayRestartFinish(MDS *mds_, uint64_t old_read_pos_) :
    MDSIOContext(mds_), old_read_pos(old_read_pos_) {}
  void finish(int r) {
    mds->_standby_replay_restart_finish(r, old_read_pos);
  }
};

void MDS::_standby_replay_restart_finish(int r, uint64_t old_read_pos)
{
  if (old_read_pos < mdlog->get_journaler()->get_trimmed_pos()) {
    dout(0) << "standby MDS fell behind active MDS journal's expire_pos, restarting" << dendl;
    respawn(); /* we're too far back, and this is easier than
		  trying to reset everything in the cache, etc */
  } else {
    mdlog->standby_trim_segments();
    boot_start(MDS_BOOT_PREPARE_LOG, r);
  }
}

inline void MDS::standby_replay_restart()
{
  dout(1) << "standby_replay_restart"
	  << (standby_replaying ? " (as standby)":" (final takeover pass)")
	  << dendl;
  if (standby_replaying) {
    /* Go around for another pass of replaying in standby */
    mdlog->get_journaler()->reread_head_and_probe(
      new C_MDS_StandbyReplayRestartFinish(
        this,
	mdlog->get_journaler()->get_read_pos()));
  } else {
    /* We are transitioning out of standby: wait for OSD map update
       before making final pass */
    Context *fin = new C_OnFinisher(new C_IO_Wrapper(this,
          new C_MDS_BootStart(this, MDS_BOOT_PREPARE_LOG)),
      &finisher);
    bool const ready =
      objecter->wait_for_map(mdsmap->get_last_failure_osd_epoch(), fin);
    if (ready) {
      delete fin;
      mdlog->get_journaler()->reread_head_and_probe(
        new C_MDS_StandbyReplayRestartFinish(
          this,
	  mdlog->get_journaler()->get_read_pos()));
    } else {
      dout(1) << " waiting for osdmap " << mdsmap->get_last_failure_osd_epoch() 
              << " (which blacklists prior instance)" << dendl;
    }
  }
}

class MDS::C_MDS_StandbyReplayRestart : public MDSInternalContext {
public:
  C_MDS_StandbyReplayRestart(MDS *m) : MDSInternalContext(m) {}
  void finish(int r) {
    assert(!r);
    mds->standby_replay_restart();
  }
};

void MDS::replay_done()
{
  dout(1) << "replay_done" << (standby_replaying ? " (as standby)" : "") << dendl;

  if (is_oneshot_replay()) {
    dout(2) << "hack.  journal looks ok.  shutting down." << dendl;
    suicide();
    return;
  }

  if (is_standby_replay()) {
    // The replay was done in standby state, and we are still in that state
    assert(standby_replaying);
    dout(10) << "setting replay timer" << dendl;
    timer.add_event_after(g_conf->mds_replay_interval,
                          new C_MDS_StandbyReplayRestart(this));
    return;
  } else if (standby_replaying) {
    // The replay was done in standby state, we have now _left_ that state
    dout(10) << " last replay pass was as a standby; making final pass" << dendl;
    standby_replaying = false;
    standby_replay_restart();
    return;
  } else {
    // Replay is complete, journal read should be up to date
    assert(mdlog->get_journaler()->get_read_pos() == mdlog->get_journaler()->get_write_pos());
    assert(!is_standby_replay());

    // Reformat and come back here
    if (mdlog->get_journaler()->get_stream_format() < g_conf->mds_journal_format) {
        dout(4) << "reformatting journal on standbyreplay->replay transition" << dendl;
        mdlog->reopen(new C_MDS_BootStart(this, MDS_BOOT_REPLAY_DONE));
        return;
    }
  }

  dout(1) << "making mds journal writeable" << dendl;
  mdlog->get_journaler()->set_writeable();
  mdlog->get_journaler()->trim_tail();

  if (g_conf->mds_wipe_sessions) {
    dout(1) << "wiping out client sessions" << dendl;
    sessionmap.wipe();
    sessionmap.save(new C_MDSInternalNoop);
  }
  if (g_conf->mds_wipe_ino_prealloc) {
    dout(1) << "wiping out ino prealloc from sessions" << dendl;
    sessionmap.wipe_ino_prealloc();
    sessionmap.save(new C_MDSInternalNoop);
  }
  if (g_conf->mds_skip_ino) {
    inodeno_t i = g_conf->mds_skip_ino;
    dout(1) << "skipping " << i << " inodes" << dendl;
    inotable->skip_inos(i);
    inotable->save(new C_MDSInternalNoop);
  }

  if (mdsmap->get_num_in_mds() == 1 &&
      mdsmap->get_num_failed_mds() == 0) { // just me!
    dout(2) << "i am alone, moving to state reconnect" << dendl;      
    request_state(MDSMap::STATE_RECONNECT);
  } else {
    dout(2) << "i am not alone, moving to state resolve" << dendl;
    request_state(MDSMap::STATE_RESOLVE);
  }
}

void MDS::reopen_log()
{
  dout(1) << "reopen_log" << dendl;
  mdcache->rollback_uncommitted_fragments();
}


void MDS::resolve_start()
{
  dout(1) << "resolve_start" << dendl;

  reopen_log();

  mdcache->resolve_start();
  finish_contexts(g_ceph_context, waiting_for_resolve);
}
void MDS::resolve_done()
{
  dout(1) << "resolve_done" << dendl;
  request_state(MDSMap::STATE_RECONNECT);
}

void MDS::reconnect_start()
{
  dout(1) << "reconnect_start" << dendl;

  if (last_state == MDSMap::STATE_REPLAY)
    reopen_log();

  server->reconnect_clients();
  finish_contexts(g_ceph_context, waiting_for_reconnect);
}
void MDS::reconnect_done()
{
  dout(1) << "reconnect_done" << dendl;
  request_state(MDSMap::STATE_REJOIN);    // move to rejoin state
}

void MDS::rejoin_joint_start()
{
  dout(1) << "rejoin_joint_start" << dendl;
  mdcache->rejoin_send_rejoins();
}
void MDS::rejoin_start()
{
  dout(1) << "rejoin_start" << dendl;
  mdcache->rejoin_start();
}
void MDS::rejoin_done()
{
  dout(1) << "rejoin_done" << dendl;
  mdcache->show_subtrees();
  mdcache->show_cache();

  // funny case: is our cache empty?  no subtrees?
  if (!mdcache->is_subtrees()) {
    dout(1) << " empty cache, no subtrees, leaving cluster" << dendl;
    request_state(MDSMap::STATE_STOPPED);
    return;
  }

  if (replay_queue.empty())
    request_state(MDSMap::STATE_ACTIVE);
  else
    request_state(MDSMap::STATE_CLIENTREPLAY);
}

void MDS::clientreplay_start()
{
  dout(1) << "clientreplay_start" << dendl;
  finish_contexts(g_ceph_context, waiting_for_replay);  // kick waiters
  queue_one_replay();
}

void MDS::clientreplay_done()
{
  dout(1) << "clientreplay_done" << dendl;
  request_state(MDSMap::STATE_ACTIVE);
}

void MDS::active_start()
{
  dout(1) << "active_start" << dendl;

  if (last_state == MDSMap::STATE_CREATING)
    mdcache->open_root();

  mdcache->clean_open_file_lists();
  mdcache->export_remaining_imported_caps();
  finish_contexts(g_ceph_context, waiting_for_replay);  // kick waiters
  finish_contexts(g_ceph_context, waiting_for_active);  // kick waiters
}

void MDS::recovery_done(int oldstate)
{
  dout(1) << "recovery_done -- successful recovery!" << dendl;
  assert(is_clientreplay() || is_active());
  
  // kick snaptable (resent AGREEs)
  if (mdsmap->get_tableserver() == whoami) {
    set<int> active;
    mdsmap->get_clientreplay_or_active_or_stopping_mds_set(active);
    snapserver->finish_recovery(active);
  }

  if (oldstate == MDSMap::STATE_CREATING)
    return;

  mdcache->start_recovered_truncates();
  mdcache->do_file_recover();

  mdcache->reissue_all_caps();
  
  // tell connected clients
  //bcast_mds_map();     // not anymore, they get this from the monitor

  mdcache->populate_mydir();
}

void MDS::handle_mds_recovery(int who) 
{
  dout(5) << "handle_mds_recovery mds." << who << dendl;
  
  mdcache->handle_mds_recovery(who);

  if (mdsmap->get_tableserver() == whoami) {
    snapserver->handle_mds_recovery(who);
  }

  queue_waiters(waiting_for_active_peer[who]);
  waiting_for_active_peer.erase(who);
}

void MDS::handle_mds_failure(int who)
{
  if (who == whoami) {
    dout(5) << "handle_mds_failure for myself; not doing anything" << dendl;
    return;
  }
  dout(5) << "handle_mds_failure mds." << who << dendl;

  mdcache->handle_mds_failure(who);

  snapclient->handle_mds_failure(who);
}

void MDS::stopping_start()
{
  dout(2) << "stopping_start" << dendl;

  if (mdsmap->get_num_in_mds() == 1 && !sessionmap.empty()) {
    // we're the only mds up!
    dout(0) << "we are the last MDS, and have mounted clients: we cannot flush our journal.  suicide!" << dendl;
    suicide();
  }

  mdcache->shutdown_start();
}

void MDS::stopping_done()
{
  dout(2) << "stopping_done" << dendl;

  // tell monitor we shut down cleanly.
  request_state(MDSMap::STATE_STOPPED);
}

void MDS::handle_signal(int signum)
{
  assert(signum == SIGINT || signum == SIGTERM);
  derr << "*** got signal " << sys_siglist[signum] << " ***" << dendl;
  mds_lock.Lock();
  suicide();
  mds_lock.Unlock();
}

void MDS::suicide()
{
  assert(mds_lock.is_locked());
  set_want_state(MDSMap::STATE_DNE); // whatever.

  dout(1) << "suicide.  wanted " << ceph_mds_state_name(want_state)
	  << ", now " << ceph_mds_state_name(state) << dendl;

  mdlog->shutdown();

  finisher.stop(); // no flushing

  // stop timers
  beacon.shutdown();
  if (tick_event) {
    timer.cancel_event(tick_event);
    tick_event = 0;
  }
  timer.cancel_all_events();
  //timer.join();
  timer.shutdown();
  
  clean_up_admin_socket();

  // shut down cache
  mdcache->shutdown();

  if (objecter->initialized.read())
    objecter->shutdown();

  monc->shutdown();

  op_tracker.on_shutdown();

  progress_thread.shutdown();

  // shut down messenger
  messenger->shutdown();

  // Workaround unclean shutdown: HeartbeatMap will assert if
  // worker is not removed (as we do in ~MDS), but ~MDS is not
  // always called after suicide.
  if (hb) {
    cct->get_heartbeat_map()->remove_worker(hb);
    hb = NULL;
  }
}

void MDS::respawn()
{
  dout(1) << "respawn" << dendl;

  char *new_argv[orig_argc+1];
  dout(1) << " e: '" << orig_argv[0] << "'" << dendl;
  for (int i=0; i<orig_argc; i++) {
    new_argv[i] = (char *)orig_argv[i];
    dout(1) << " " << i << ": '" << orig_argv[i] << "'" << dendl;
  }
  new_argv[orig_argc] = NULL;

  /* Determine the path to our executable, try to read
   * linux-specific /proc/ path first */
  char exe_path[PATH_MAX];
  ssize_t exe_path_bytes = readlink("/proc/self/exe", exe_path,
				    sizeof(exe_path) - 1);
  if (exe_path_bytes < 0) {
    /* Print CWD for the user's interest */
    char buf[PATH_MAX];
    char *cwd = getcwd(buf, sizeof(buf));
    assert(cwd);
    dout(1) << " cwd " << cwd << dendl;

    /* Fall back to a best-effort: just running in our CWD */
    strncpy(exe_path, orig_argv[0], sizeof(exe_path) - 1);
  } else {
    exe_path[exe_path_bytes] = '\0';
  }

  dout(1) << " exe_path " << exe_path << dendl;

  unblock_all_signals(NULL);
  execv(exe_path, new_argv);

  dout(0) << "respawn execv " << orig_argv[0]
	  << " failed with " << cpp_strerror(errno) << dendl;
  suicide();
}




bool MDS::ms_dispatch(Message *m)
{
  bool ret;
  mds_lock.Lock();

  heartbeat_reset();

  if (want_state == CEPH_MDS_STATE_DNE) {
    dout(10) << " stopping, discarding " << *m << dendl;
    m->put();
    ret = true;
  } else {
    inc_dispatch_depth();
    ret = _dispatch(m);
    dec_dispatch_depth();
  }
  mds_lock.Unlock();
  return ret;
}

bool MDS::ms_get_authorizer(int dest_type, AuthAuthorizer **authorizer, bool force_new)
{
  dout(10) << "MDS::ms_get_authorizer type=" << ceph_entity_type_name(dest_type) << dendl;

  /* monitor authorization is being handled on different layer */
  if (dest_type == CEPH_ENTITY_TYPE_MON)
    return true;

  if (force_new) {
    if (monc->wait_auth_rotating(10) < 0)
      return false;
  }

  *authorizer = monc->auth->build_authorizer(dest_type);
  return *authorizer != NULL;
}


#define ALLOW_MESSAGES_FROM(peers) \
do { \
  if (m->get_connection() && (m->get_connection()->get_peer_type() & (peers)) == 0) { \
    dout(0) << __FILE__ << "." << __LINE__ << ": filtered out request, peer=" << m->get_connection()->get_peer_type() \
           << " allowing=" << #peers << " message=" << *m << dendl; \
    m->put();							    \
    return true; \
  } \
} while (0)


/*
 * high priority messages we always process
 */
bool MDS::handle_core_message(Message *m)
{
  switch (m->get_type()) {
  case CEPH_MSG_MON_MAP:
    ALLOW_MESSAGES_FROM(CEPH_ENTITY_TYPE_MON);
    m->put();
    break;

    // MDS
  case CEPH_MSG_MDS_MAP:
    ALLOW_MESSAGES_FROM(CEPH_ENTITY_TYPE_MON | CEPH_ENTITY_TYPE_MDS);
    handle_mds_map(static_cast<MMDSMap*>(m));
    break;

    // misc
  case MSG_MON_COMMAND:
    ALLOW_MESSAGES_FROM(CEPH_ENTITY_TYPE_MON);
    handle_command(static_cast<MMonCommand*>(m));
    break;    

    // OSD
  case CEPH_MSG_OSD_MAP:
    ALLOW_MESSAGES_FROM(CEPH_ENTITY_TYPE_MON | CEPH_ENTITY_TYPE_OSD);
    if (is_active() && snapserver)
      snapserver->check_osd_map(true);
    break;

  default:
    return false;
  }
  return true;
}

/*
 * lower priority messages we defer if we seem laggy
 */
bool MDS::handle_deferrable_message(Message *m)
{
  int port = m->get_type() & 0xff00;

  switch (port) {
  case MDS_PORT_CACHE:
    ALLOW_MESSAGES_FROM(CEPH_ENTITY_TYPE_MDS);
    mdcache->dispatch(m);
    break;
    
  case MDS_PORT_MIGRATOR:
    ALLOW_MESSAGES_FROM(CEPH_ENTITY_TYPE_MDS);
    mdcache->migrator->dispatch(m);
    break;
    
  default:
    switch (m->get_type()) {
      // SERVER
    case CEPH_MSG_CLIENT_SESSION:
    case CEPH_MSG_CLIENT_RECONNECT:
      ALLOW_MESSAGES_FROM(CEPH_ENTITY_TYPE_CLIENT);
      // fall-thru
    case CEPH_MSG_CLIENT_REQUEST:
      server->dispatch(m);
      break;
    case MSG_MDS_SLAVE_REQUEST:
      ALLOW_MESSAGES_FROM(CEPH_ENTITY_TYPE_MDS);
      server->dispatch(m);
      break;
      
    case MSG_MDS_HEARTBEAT:
      ALLOW_MESSAGES_FROM(CEPH_ENTITY_TYPE_MDS);
      balancer->proc_message(m);
      break;
	  
    case MSG_MDS_TABLE_REQUEST:
      ALLOW_MESSAGES_FROM(CEPH_ENTITY_TYPE_MDS);
      {
	MMDSTableRequest *req = static_cast<MMDSTableRequest*>(m);
	if (req->op < 0) {
	  MDSTableClient *client = get_table_client(req->table);
	      client->handle_request(req);
	} else {
	  MDSTableServer *server = get_table_server(req->table);
	  server->handle_request(req);
	}
      }
      break;

    case MSG_MDS_LOCK:
    case MSG_MDS_INODEFILECAPS:
      ALLOW_MESSAGES_FROM(CEPH_ENTITY_TYPE_MDS);
      locker->dispatch(m);
      break;
      
    case CEPH_MSG_CLIENT_CAPS:
    case CEPH_MSG_CLIENT_CAPRELEASE:
    case CEPH_MSG_CLIENT_LEASE:
      ALLOW_MESSAGES_FROM(CEPH_ENTITY_TYPE_CLIENT);
      locker->dispatch(m);
      break;
      
    default:
      return false;
    }
  }

  return true;
}

bool MDS::is_stale_message(Message *m)
{
  // from bad mds?
  if (m->get_source().is_mds()) {
    int from = m->get_source().num();
    if (!mdsmap->have_inst(from) ||
	mdsmap->get_inst(from) != m->get_source_inst() ||
	mdsmap->is_down(from)) {
      // bogus mds?
      if (m->get_type() == CEPH_MSG_MDS_MAP) {
	dout(5) << "got " << *m << " from old/bad/imposter mds " << m->get_source()
		<< ", but it's an mdsmap, looking at it" << dendl;
      } else if (m->get_type() == MSG_MDS_CACHEEXPIRE &&
		 mdsmap->get_inst(from) == m->get_source_inst()) {
	dout(5) << "got " << *m << " from down mds " << m->get_source()
		<< ", but it's a cache_expire, looking at it" << dendl;
      } else {
	dout(5) << "got " << *m << " from down/old/bad/imposter mds " << m->get_source()
		<< ", dropping" << dendl;
	return true;
      }
    }
  }
  return false;
}

/**
 * Advance finished_queue and waiting_for_nolaggy.
 *
 * Usually drain both queues, but may not drain waiting_for_nolaggy
 * if beacon is currently laggy.
 */
void MDS::_advance_queues()
{
  assert(mds_lock.is_locked_by_me());

  while (!finished_queue.empty()) {
    dout(7) << "mds has " << finished_queue.size() << " queued contexts" << dendl;
    dout(10) << finished_queue << dendl;
    list<MDSInternalContextBase*> ls;
    ls.swap(finished_queue);
    while (!ls.empty()) {
      dout(10) << " finish " << ls.front() << dendl;

      dout(1) << "trigger:  " << this << dendl;
      dout(1) << "          " << this->mds_lock.is_locked() << dendl;
      dout(1) << "          " << this->mds_lock.is_locked_by_me() << dendl;
      ls.front()->complete(0);
      ls.pop_front();

      heartbeat_reset();
    }
  }

  while (!waiting_for_nolaggy.empty()) {
    // stop if we're laggy now!
    if (beacon.is_laggy())
      break;

    Message *old = waiting_for_nolaggy.front();
    waiting_for_nolaggy.pop_front();

    if (is_stale_message(old)) {
      old->put();
    } else {
      dout(7) << " processing laggy deferred " << *old << dendl;
      handle_deferrable_message(old);
    }

    heartbeat_reset();
  }
}

/* If this function returns true, it has put the message. If it returns false,
 * it has not put the message. */
bool MDS::_dispatch(Message *m)
{
  if (is_stale_message(m)) {
    m->put();
    return true;
  }

  // core
  if (!handle_core_message(m)) {
    if (beacon.is_laggy()) {
      dout(10) << " laggy, deferring " << *m << dendl;
      waiting_for_nolaggy.push_back(m);
    } else {
      if (!handle_deferrable_message(m)) {
	dout(0) << "unrecognized message " << *m << dendl;
	m->put();
	return false;
      }
    }
  }

  if (dispatch_depth > 1)
    return true;

  // finish any triggered contexts
  _advance_queues();

  if (beacon.is_laggy()) {
    // We've gone laggy during dispatch, don't do any
    // more housekeeping
    return true;
  }

  // done with all client replayed requests?
  if (is_clientreplay() &&
      mdcache->is_open() &&
      replay_queue.empty() &&
      want_state == MDSMap::STATE_CLIENTREPLAY) {
    int num_requests = mdcache->get_num_client_requests();
    dout(10) << " still have " << num_requests << " active replay requests" << dendl;
    if (num_requests == 0)
      clientreplay_done();
  }

  // hack: thrash exports
  static utime_t start;
  utime_t now = ceph_clock_now(g_ceph_context);
  if (start == utime_t()) 
    start = now;
  /*double el = now - start;
  if (el > 30.0 &&
    el < 60.0)*/
  for (int i=0; i<g_conf->mds_thrash_exports; i++) {
    set<int> s;
    if (!is_active()) break;
    mdsmap->get_mds_set(s, MDSMap::STATE_ACTIVE);
    if (s.size() < 2 || mdcache->get_num_inodes() < 10) 
      break;  // need peers for this to work.

    dout(7) << "mds thrashing exports pass " << (i+1) << "/" << g_conf->mds_thrash_exports << dendl;
    
    // pick a random dir inode
    CInode *in = mdcache->hack_pick_random_inode();

    list<CDir*> ls;
    in->get_dirfrags(ls);
    if (ls.empty())
      continue;                // must be an open dir.
    list<CDir*>::iterator p = ls.begin();
    int n = rand() % ls.size();
    while (n--)
      ++p;
    CDir *dir = *p;
    if (!dir->get_parent_dir()) continue;    // must be linked.
    if (!dir->is_auth()) continue;           // must be auth.

    int dest;
    do {
      int k = rand() % s.size();
      set<int>::iterator p = s.begin();
      while (k--) ++p;
      dest = *p;
    } while (dest == whoami);
    mdcache->migrator->export_dir_nicely(dir,dest);
  }
  // hack: thrash fragments
  for (int i=0; i<g_conf->mds_thrash_fragments; i++) {
    if (!is_active()) break;
    if (mdcache->get_num_fragmenting_dirs() > 5) break;
    dout(7) << "mds thrashing fragments pass " << (i+1) << "/" << g_conf->mds_thrash_fragments << dendl;
    
    // pick a random dir inode
    CInode *in = mdcache->hack_pick_random_inode();

    list<CDir*> ls;
    in->get_dirfrags(ls);
    if (ls.empty()) continue;                // must be an open dir.
    CDir *dir = ls.front();
    if (!dir->get_parent_dir()) continue;    // must be linked.
    if (!dir->is_auth()) continue;           // must be auth.
    frag_t fg = dir->get_frag();
    if (fg == frag_t() || (rand() % (1 << fg.bits()) == 0))
      mdcache->split_dir(dir, 1);
    else
      balancer->queue_merge(dir);
  }

  // hack: force hash root?
  /*
  if (false &&
      mdcache->get_root() &&
      mdcache->get_root()->dir &&
      !(mdcache->get_root()->dir->is_hashed() || 
        mdcache->get_root()->dir->is_hashing())) {
    dout(0) << "hashing root" << dendl;
    mdcache->migrator->hash_dir(mdcache->get_root()->dir);
  }
  */

  if (mlogger) {
    mlogger->set(l_mdm_ino, g_num_ino);
    mlogger->set(l_mdm_dir, g_num_dir);
    mlogger->set(l_mdm_dn, g_num_dn);
    mlogger->set(l_mdm_cap, g_num_cap);

    mlogger->inc(l_mdm_inoa, g_num_inoa);  g_num_inoa = 0;
    mlogger->inc(l_mdm_inos, g_num_inos);  g_num_inos = 0;
    mlogger->inc(l_mdm_dira, g_num_dira);  g_num_dira = 0;
    mlogger->inc(l_mdm_dirs, g_num_dirs);  g_num_dirs = 0;
    mlogger->inc(l_mdm_dna, g_num_dna);  g_num_dna = 0;
    mlogger->inc(l_mdm_dns, g_num_dns);  g_num_dns = 0;
    mlogger->inc(l_mdm_capa, g_num_capa);  g_num_capa = 0;
    mlogger->inc(l_mdm_caps, g_num_caps);  g_num_caps = 0;

    mlogger->set(l_mdm_buf, buffer::get_total_alloc());

  }

  // shut down?
  if (is_stopping()) {
    mdlog->trim();
    if (mdcache->shutdown_pass()) {
      dout(7) << "shutdown_pass=true, finished w/ shutdown, moving to down:stopped" << dendl;
      stopping_done();
    }
    else {
      dout(7) << "shutdown_pass=false" << dendl;
    }
  }
  return true;
}




void MDS::ms_handle_connect(Connection *con) 
{
}

bool MDS::ms_handle_reset(Connection *con) 
{
  if (con->get_peer_type() != CEPH_ENTITY_TYPE_CLIENT)
    return false;

  Mutex::Locker l(mds_lock);
  dout(5) << "ms_handle_reset on " << con->get_peer_addr() << dendl;
  if (want_state == CEPH_MDS_STATE_DNE)
    return false;

  Session *session = static_cast<Session *>(con->get_priv());
  if (session) {
    if (session->is_closed()) {
      dout(3) << "ms_handle_reset closing connection for session " << session->info.inst << dendl;
      con->mark_down();
      con->set_priv(NULL);
      sessionmap.remove_session(session);
    }
    session->put();
  } else {
    con->mark_down();
  }
  return false;
}


void MDS::ms_handle_remote_reset(Connection *con) 
{
  if (con->get_peer_type() != CEPH_ENTITY_TYPE_CLIENT)
    return;

  Mutex::Locker l(mds_lock);
  dout(5) << "ms_handle_remote_reset on " << con->get_peer_addr() << dendl;
  if (want_state == CEPH_MDS_STATE_DNE)
    return;

  Session *session = static_cast<Session *>(con->get_priv());
  if (session) {
    if (session->is_closed()) {
      dout(3) << "ms_handle_remote_reset closing connection for session " << session->info.inst << dendl;
      con->mark_down();
      con->set_priv(NULL);
      sessionmap.remove_session(session);
    }
    session->put();
  }
}

bool MDS::ms_verify_authorizer(Connection *con, int peer_type,
			       int protocol, bufferlist& authorizer_data, bufferlist& authorizer_reply,
			       bool& is_valid, CryptoKey& session_key)
{
  Mutex::Locker l(mds_lock);
  if (want_state == CEPH_MDS_STATE_DNE)
    return false;

  AuthAuthorizeHandler *authorize_handler = 0;
  switch (peer_type) {
  case CEPH_ENTITY_TYPE_MDS:
    authorize_handler = authorize_handler_cluster_registry->get_handler(protocol);
    break;
  default:
    authorize_handler = authorize_handler_service_registry->get_handler(protocol);
  }
  if (!authorize_handler) {
    dout(0) << "No AuthAuthorizeHandler found for protocol " << protocol << dendl;
    is_valid = false;
    return true;
  }

  AuthCapsInfo caps_info;
  EntityName name;
  uint64_t global_id;

  is_valid = authorize_handler->verify_authorizer(cct, monc->rotating_secrets,
						  authorizer_data, authorizer_reply, name, global_id, caps_info, session_key);

  if (is_valid) {
    // wire up a Session* to this connection, and add it to the session map
    entity_name_t n(con->get_peer_type(), global_id);
    Session *s = sessionmap.get_session(n);
    if (!s) {
      s = new Session;
      s->info.inst.addr = con->get_peer_addr();
      s->info.inst.name = n;
      dout(10) << " new session " << s << " for " << s->info.inst << " con " << con << dendl;
      con->set_priv(s);
      s->connection = con;
      sessionmap.add_session(s);
    } else {
      dout(10) << " existing session " << s << " for " << s->info.inst << " existing con " << s->connection
	       << ", new/authorizing con " << con << dendl;
      con->set_priv(s->get());

      // Wait until we fully accept the connection before setting
      // s->connection.  In particular, if there are multiple incoming
      // connection attempts, they will all get their authorizer
      // validated, but some of them may "lose the race" and get
      // dropped.  We only want to consider the winner(s).  See
      // ms_handle_accept().  This is important for Sessions we replay
      // from the journal on recovery that don't have established
      // messenger state; we want the con from only the winning
      // connect attempt(s).  (Normal reconnects that don't follow MDS
      // recovery are reconnected to the existing con by the
      // messenger.)
    }

    /*
    s->caps.set_allow_all(caps_info.allow_all);
 
    if (caps_info.caps.length() > 0) {
      bufferlist::iterator iter = caps_info.caps.begin();
      s->caps.parse(iter);
      dout(10) << " session " << s << " has caps " << s->caps << dendl;
    }
    */
  }

  return true;  // we made a decision (see is_valid)
}


void MDS::ms_handle_accept(Connection *con)
{
  Mutex::Locker l(mds_lock);
  Session *s = static_cast<Session *>(con->get_priv());
  dout(10) << "ms_handle_accept " << con->get_peer_addr() << " con " << con << " session " << s << dendl;
  if (s) {
    if (s->connection != con) {
      dout(10) << " session connection " << s->connection << " -> " << con << dendl;
      s->connection = con;

      // send out any queued messages
      while (!s->preopen_out_queue.empty()) {
	con->send_message(s->preopen_out_queue.front());
	s->preopen_out_queue.pop_front();
      }
    }
    s->put();
  }
}

void MDS::set_want_state(MDSMap::DaemonState newstate)
{
  if (want_state != newstate) {
    dout(10) << __func__ << " "
      << ceph_mds_state_name(want_state) << " -> "
      << ceph_mds_state_name(newstate) << dendl;
    want_state = newstate;
    beacon.notify_want_state(newstate);
  }
}

/**
 * Call this when you take mds_lock, or periodically if you're going to
 * hold the lock for a long time (e.g. iterating over clients/inodes)
 */
void MDS::heartbeat_reset()
{
  assert(hb != NULL);
  // NB not enabling suicide grace, because the mon takes care of killing us
  // (by blacklisting us) when we fail to send beacons, and it's simpler to
  // only have one way of dying.
  cct->get_heartbeat_map()->reset_timeout(hb, g_conf->mds_beacon_grace, 0);
}


void *MDS::ProgressThread::entry()
{
  Mutex::Locker l(mds->mds_lock);
  while (true) {
    while (!stopping &&
	   mds->finished_queue.empty() &&
	   (mds->waiting_for_nolaggy.empty() || mds->beacon.is_laggy())) {
      cond.Wait(mds->mds_lock);
    }

    if (stopping) {
      break;
    }

    mds->_advance_queues();
  }

  return NULL;
}


void MDS::ProgressThread::shutdown()
{
  assert(mds->mds_lock.is_locked_by_me());

  stopping = true;
  cond.Signal();
  mds->mds_lock.Unlock();
  join();
  mds->mds_lock.Lock();
}
