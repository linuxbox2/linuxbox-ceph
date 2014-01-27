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

#include <sys/types.h>

#include <iostream>
#include <string>

using namespace std;

#include "common/config.h"
#include "msg/msg_types.h"
#include "msg/XioMessenger.h"
#include "msg/XioMsg.h"
#include "messages/MPing.h"
#include "common/Timer.h"
#include "common/ceph_argparse.h"
#include "global/global_init.h"
#include "perfglue/heap_profiler.h"
#include "common/address_helper.h"
#include "message_helper.h"
#include "simple_dispatcher.h"

#define dout_subsys ceph_subsys_xio_client

int main(int argc, const char **argv)
{
	vector<const char*> args;
	Messenger* messenger;
	SimpleDispatcher *dispatcher;
	entity_addr_t dest_addr;
	ConnectionRef conn;
	int r = 0;

	struct timespec ts = {
		.tv_sec = 1,
		.tv_nsec = 0
	};

	argv_to_vec(argc, argv, args);
	env_to_vec(args);

	global_init(NULL, args,
		    CEPH_ENTITY_TYPE_ANY, CODE_ENVIRONMENT_UTILITY, 0);

	messenger = new XioMessenger(g_ceph_context,
				     entity_name_t::GENERIC(),
				     "xio_client",
				     0 /* nonce */,
				     0 /* portals */);

	messenger->set_default_policy(Messenger::Policy::lossy_client(0, 0));

	entity_addr_from_url(&dest_addr, "tcp://localhost:1234");
	entity_inst_t dest_server(entity_name_t::GENERIC(), dest_addr);

	dispatcher = new SimpleDispatcher(messenger);
	messenger->add_dispatcher_head(dispatcher);

	dispatcher->set_active(); // this side is the pinger

	r = messenger->start();
	if (r < 0)
		goto out;

	conn = messenger->get_connection(dest_server);

	// do stuff
	time_t t1, t2;
	t1 = time(NULL);

	int msg_ix;
	for (msg_ix = 0; msg_ix < 512; ++msg_ix) {
	  //messenger->send_message(new MPing(), conn);
	  messenger->send_message(new_ping_monstyle("sping", 100), conn);
	}

	// do stuff
	while (conn->is_connected()) {
	  nanosleep(&ts, NULL);
	}

	t2 = time(NULL);
	cout << "Processed " << dispatcher->get_dcount() + 256
	     << " round-trip messages in " << t2-t1 << "s"
	     << std::endl;
out:
	return r;
}
