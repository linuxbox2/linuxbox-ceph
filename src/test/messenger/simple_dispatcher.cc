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

SimpleDispatcher::SimpleDispatcher(Messenger *msgr) :
  Dispatcher(msgr->cct),
  active(false),
  messenger(msgr)
{
  // nothing
}

SimpleDispatcher::~SimpleDispatcher() {
  // nothing
}

bool SimpleDispatcher::ms_dispatch(Message *m)
{
  ConnectionRef conn;
  int code;

  cout << __func__ << " " << m << std::endl;

  switch (m->get_type()) {
  case CEPH_MSG_PING:
    if (active) {
      cout << "pong!" << std::endl;
    } else {
      cout << "ping!" << std::endl;
      conn = m->get_connection();
      code = messenger->send_message(new MPing(), conn);
      if (code != 0) {
	cout << "send_reply returned " << code << std::endl;
	return false;
      }
    }
    m->put();
    break;
  default:
    abort();
  }
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

