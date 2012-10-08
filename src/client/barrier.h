// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 *
 * Copyright (C) 2012 Linux Box Corporation.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef BARRIER_H
#define BARRIER_H

#include "include/types.h"

#include <string>
#include <list>
#include <set>
#include <map>
#include <boost/intrusive/list.hpp>
#include <fstream>
#include <exception>

using std::list;
using std::set;
using std::map;
using std::fstream;

#include <ext/hash_map>

#include "include/interval_set.h"

#include "common/Mutex.h"
#include "common/Cond.h"

#include "common/config.h"

class Client;

typedef std::pair<uint64_t,uint64_t> barrier_interval;

using namespace std;

/*
 * we keep count of uncommitted writes on the inode, so that
 * ll_commit_blocks can do the right thing.
 *
 * This is just a hacked copy of Ceph's sync callback.
 */

enum CBlockSync_State
{
  CBlockSync_State_None, /* initial state */
  CBlockSync_State_Unclaimed, /* outstanding write */
  CBlockSync_State_Committing, /* commit in progress */
  CBlockSync_State_Completed,
};

class Barrier;
class BarrierContext;

class C_Block_Sync : public Context {
private:
  Client *cl;
  uint64_t ino;
  barrier_interval iv;
  enum CBlockSync_State state;
  Barrier *barrier;

public:
  boost::intrusive::list_member_hook<> intervals_hook;
  C_Block_Sync(Client *c, uint64_t i, barrier_interval iv);
  void finish(int);

  friend class Barrier;
  friend class BarrierContext;
};

typedef boost::intrusive::list< C_Block_Sync,
				boost::intrusive::member_hook< 
				  C_Block_Sync, 
				  boost::intrusive::list_member_hook<>,
				  &C_Block_Sync::intervals_hook >
				> BlockSyncList;

class Barrier
{
private:
  Cond cond;
  interval_set<uint64_t> span;
  BlockSyncList write_list;

public:
  boost::intrusive::list_member_hook<> active_commits_hook;

  Barrier();
  ~Barrier();

  friend class BarrierContext;
};

typedef boost::intrusive::list< Barrier,
				boost::intrusive::member_hook< 
				  Barrier, 
				  boost::intrusive::list_member_hook<>,
				  &Barrier::active_commits_hook >
				> BarrierList;

class BarrierContext
{
private:
  Client *cl;
  uint64_t ino;
  Mutex lock;

  // writes not claimed by a commit
  BlockSyncList outstanding_writes;

  // commits in progress, with their claimed writes
  BarrierList active_commits;
  interval_set<uint64_t> active_commit_interval;
    
public:
  BarrierContext(Client *c, uint64_t ino);
  void write_nobarrier(C_Block_Sync &cbs);
  void write_barrier(C_Block_Sync &cbs);
  void commit_barrier(barrier_interval &civ);
  void complete(C_Block_Sync &cbs);
  ~BarrierContext();
};

#endif
