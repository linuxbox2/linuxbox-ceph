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

#undef TRACEPOINT_PROVIDER
#define TRACEPOINT_PROVIDER xio_trace

/*
 * include file (this files's name)
 */
#undef TRACEPOINT_INCLUDE
#define TRACEPOINT_INCLUDE "msg/XioTrace.h"

/*
 * Add this precompiler conditionals to ensure the tracepoint event generation
 * can include this file more than once.
 */
#if !defined(_SAMPLE_COMPONENT_PROVIDER_H) || defined(TRACEPOINT_HEADER_MULTI_READ)
#define _XIO_TRACE_H

/*
 * Add this to allow programs to call "tracepoint(...):
 */
#include <lttng/tracepoint.h>

/*
 * The following tracepoint event writes a message (c string) into the
 * field message of the trace event message in the provider
 * sample_component in other words:
 *
 *    xio_trace:message:message = text.
 */
TRACEPOINT_EVENT(
	/*
	 * provider name, not a variable but a string starting with a letter
	 * and containing either letters, numbers or underscores.
	 * Needs to be the same as TRACEPOINT_PROVIDER
	 */
	xio_trace,
	/*
	 * tracepoint name, same format as sample provider. Does not need to be
	 * declared before. in this case the name is "message"
	 */
	message,
	/*
	 * TP_ARGS macro contains the arguments passed for the tracepoint
	 * it is in the following format
	 *		TP_ARGS( type1, name1, type2, name2, ... type10, name10)
	 * where there can be from zero to ten elements.
	 * typeN is the datatype, such as int, struct or double **.
	 * name is the variable name (in "int myInt" the name would be myint)
	 *		TP_ARGS() is valid to mean no arguments
	 *		TP_ARGS( void ) is valid too
	 */
	TP_ARGS(char *, text),
	/*
	 * TP_FIELDS describes how to write the fields of the trace event.
	 * You can use the args here
	 */
	TP_FIELDS(
	/*
	 * The ctf_string macro takes a c string and writes it into a field
	 * named "message"
	 */
		ctf_string(message, text)
	)
)
/*
 * Trace loglevel, shows the level of the trace event. It can be TRACE_EMERG,
 * TRACE_ALERT, TRACE_CRIT, TRACE_ERR, TRACE_WARNING, TRACE_INFO or others.
 * If this is not set, TRACE_DEFAULT is assumed.
 * The first two arguments identify the tracepoint
 * See details in <lttng/tracepoint.h> line 347
 */
TRACEPOINT_LOGLEVEL(
       /*
        * The provider name, must be the same as the provider name in the
        * TRACEPOINT_EVENT and as TRACEPOINT_PROVIDER above.
        */
	xio_trace,
       /*
        * The tracepoint name, must be the same as the tracepoint name in the
        * TRACEPOINT_EVENT
        */
	message,
       /*
        * The tracepoint loglevel. Warning, some levels are abbreviated and
        * others are not, please see <lttng/tracepoint.h>
        */
	TRACE_WARNING)

#endif /* _SAMPLE_COMPONENT_PROVIDER_H */

/*
 * Add this after defining the tracepoint events to expand the macros.
 */
#include <lttng/tracepoint-event.h>
