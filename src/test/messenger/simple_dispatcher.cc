// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 CohortFS, LLC
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "simple_dispatcher.h"
#include "messages/MPing.h"
#include "messages/MDataPing.h"

SimpleDispatcher::SimpleDispatcher(Messenger *msgr) :
  Dispatcher(msgr->cct),
  active(false),
  messenger(msgr),
  dcount(0)
{
  // nothing
}

SimpleDispatcher::~SimpleDispatcher() {
  // nothing
}

bool SimpleDispatcher::ms_dispatch(Message *m)
{
  ConnectionRef conn;
  uint64_t dc = 0;

  dc = ++dcount;

#if 0
  cout << __func__ << " " << m << std::endl;
#endif

  switch (m->get_type()) {
  case CEPH_MSG_PING:
    if (active) {
#if 0
      cout << "pong!" << std::endl;
#endif
    } else {
#if 0
      cout << "ping! " << dc << std::endl;
#endif
    }
    break;
  case MSG_DATA_PING:
  {
    MDataPing* mdp = static_cast<MDataPing*>(m);
    //cout << "MDataPing " << mdp->tag << " " << mdp->counter << std::endl;
    //mdp->get_data().hexdump(cout);
  }
    break;
  default:
    abort();
  }

  if ((dc % 512) == 0) {
    cout << "ping " << dc << " " << time(0) << std::endl;
  }

  m->put();

  return true;
}

bool SimpleDispatcher::ms_handle_reset(Connection *con)
{
  return true;
}

void SimpleDispatcher::ms_handle_remote_reset(Connection *con)
{
  // nothing
}

