// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 * Portions Copyright (C) 2013 CohortFS, LLC
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "XioMessenger.h"


extern "C" {

  static int on_session_event(struct xio_session *session,
			      struct xio_session_event_data *event_data,
			      void *cb_user_context)
  {
    printf("session event: %s. reason: %s\n",
	   xio_session_event_str(event_data->event),
	   xio_strerror(event_data->reason));

    xio_session_close(session);

    return 0;
}

  static int on_new_session(struct xio_session *session,
			    struct xio_new_session_req *req,
			    void *cb_user_context)
  {
 
    printf("new session");

    xio_accept(session, NULL, 0, NULL, 0);

    return 0;
  }
  
  static int on_request(struct xio_session *session,
			struct xio_msg *req,
			int more_in_batch,
			void *cb_user_context)
  {

    printf("new request");

    return 0;
  }  

} /* extern "C" */

atomic_t initialized;

XioMessenger::XioMessenger(CephContext *cct, entity_name_t name,
			   string mname, uint64_t nonce)
  : SimplePolicyMessenger(cct, name, mname, nonce)
{ 
}




