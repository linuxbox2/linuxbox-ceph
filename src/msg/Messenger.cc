// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "include/types.h"
#include "Messenger.h"

#include "SimpleMessenger.h"

Messenger *Messenger::create(CephContext *cct,
			     entity_name_t name,
			     string lname,
			     uint64_t nonce)
{
  return new SimpleMessenger(cct, name, lname, nonce);
}

/*
 * I. return former behavior with stock ceph.conf + tcp (+ no rdma)
 * II. default to no crc when rdma in config (even if doing tcp)
 * III. lots of silly configuration possible, precompute to make cheap.
 *
 * Note that crc is silly.  tcp always does checksumming too, + retries
 *  if the data is wrong.  Ethernet does checksumming also, so does PPP.
 *  Authentication (should) be doing cryptographic checksums.  All
 *  crc does is slow the code down (and maybe catch programming errors.)
 *
 * behavior:
 *	ms_datacrc = false ms_headercrc = false	=> 0		force crc no
 *	ms_datacrc = false ms_headercrc = true	=> MS_CRC_HEADER	force former "no crc"
 *	ms_datacrc = true ms_headercrc = true	=> MS_CRC_HEADER|MS_CRC_DATA force crc yes
 *	otherwise, if cluster_rdma is specified	=> 0		no crc
 *	otherwise, if "ms nocrc" == true	=> MS_CRC_HEADER	former "no crc"
 *	otherwise,				=> MS_CRC_HEADER|MS_CRC_DATA crc yes
 */
int Messenger::get_default_crc_flags(md_config_t * conf)
{
  int r = 0;
  if (conf->ms_datacrc)
    r |= MSG_CRC_DATA;
  if (conf->ms_headercrc)
    r |= MSG_CRC_HEADER;

  if (r == MSG_CRC_DATA) {
    if (conf->cluster_rdma)
      r = 0;
    else if (conf->ms_nocrc)
      r = MSG_CRC_HEADER;
    else
      r |= MSG_CRC_HEADER;
  }
  return r;
}
