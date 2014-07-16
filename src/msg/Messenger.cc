
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
	switch(r) {
	case MSG_CRC_DATA:
		std::vector <std::string> my_sections;
		conf->get_my_sections(my_sections);
		std::string val;
		r = MSG_CRC_HEADER | MSG_CRC_DATA;
		if (conf->get_val_from_conf_file(my_sections, "cluster rdma",
			val, true) == 0) {
			r = 0;
		}
		if (conf->get_val_from_conf_file(my_sections, "ms nocrc",
			val, true) == 0) {
			int v = -1;
			if (strcasecmp(val.c_str(), "false") == 0)
				v = 0;
			else if (strcasecmp(val.c_str(), "true") == 0)
				v = 1;
			else {
				std::string err;
				int b = strict_strtol(val.c_str(), 10, &err);
					if (!err.empty()) {
						v = -1;
// XXX should emit an error?
					}
				v = !!b;
			}
			switch(v) {
			case 1:
				r &= ~MSG_CRC_DATA;
			}
		}
		break;
	/* default: break; */
	}
	return r;
}
