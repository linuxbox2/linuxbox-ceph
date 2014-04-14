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

#include "Message.h"
#include "Pipe.h"
#include "SimpleMessenger.h"
#include "Connection.h"

PipeConnection::~PipeConnection() {
    if (priv) {
	priv->put();
    }
    if (pipe) {
	pipe->put();
	pipe = NULL;
    }
}

Pipe* PipeConnection::get_pipe() {
    Mutex::Locker l(lock);
    if (pipe)
	return pipe->get();
    return NULL;
}

bool PipeConnection::try_get_pipe(Pipe **p) {
    Mutex::Locker l(lock);
    if (failed) {
	*p = NULL;
    } else {
	if (pipe)
	    *p = pipe->get();
	else
	    *p = NULL;
    }
    return !failed;
}

bool PipeConnection::clear_pipe(Pipe *old_p) {
    Mutex::Locker l(lock);
#if 0 /* XXX */
    cout << __func__ << " pipe: " << pipe << " old_p: " << old_p <<
	std::endl;
#endif
    if (old_p == pipe) {
	pipe->put();
	pipe = NULL;
	failed = true;
	return true;
    }
    return false;
}

void PipeConnection::reset_pipe(Pipe *p) {
    Mutex::Locker l(lock);
    if (pipe)
	pipe->put();
    pipe = p->get();
}
