// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <errno.h>
#include <array>
#include <string.h>

#include "common/ceph_crypto.h"
#include "common/Formatter.h"
#include "common/utf8.h"
#include "common/ceph_json.h"
#include "common/safe_io.h"
#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/replace.hpp>

#include "rgw_rest.h"
#include "rgw_rest_s3.h"
#include "rgw_rest_s3website.h"
#include "rgw_auth_s3.h"
#include "rgw_acl.h"
#include "rgw_policy_s3.h"
#include "rgw_user.h"
#include "rgw_cors.h"
#include "rgw_cors_s3.h"

#include "rgw_client_io.h"

#include "rgw_keystone.h"

#include <typeinfo> // for 'typeid'

#include "rgw_ldap.h"
#include "rgw_token.h"
#include "include/assert.h"

#define dout_subsys ceph_subsys_rgw

using namespace rgw;
using namespace ceph::crypto;

using std::get;

void list_all_buckets_start(struct req_state *s)
{
  s->formatter->open_array_section_in_ns("ListAllMyBucketsResult", XMLNS_AWS_S3);
}

void list_all_buckets_end(struct req_state *s)
{
  s->formatter->close_section();
}

void dump_bucket(struct req_state *s, RGWBucketEnt& obj)
{
  s->formatter->open_object_section("Bucket");
  s->formatter->dump_string("Name", obj.bucket.name);
  dump_time(s, "CreationDate", &obj.creation_time);
  s->formatter->close_section();
}

void rgw_get_errno_s3(rgw_http_error *e , int err_no)
{
  rgw_http_errors::const_iterator r = rgw_http_s3_errors.find(err_no);

  if (r != rgw_http_s3_errors.end()) {
    e->http_ret = r->second.first;
    e->s3_code = r->second.second;
  } else {
    e->http_ret = 500;
    e->s3_code = "UnknownError";
  }
}

struct response_attr_param {
  const char *param;
  const char *http_attr;
};

static struct response_attr_param resp_attr_params[] = {
  {"response-content-type", "Content-Type"},
  {"response-content-language", "Content-Language"},
  {"response-expires", "Expires"},
  {"response-cache-control", "Cache-Control"},
  {"response-content-disposition", "Content-Disposition"},
  {"response-content-encoding", "Content-Encoding"},
  {NULL, NULL},
};

int RGWGetObj_ObjStore_S3Website::send_response_data(bufferlist& bl, off_t bl_ofs, off_t bl_len) {
  map<string, bufferlist>::iterator iter;
  iter = attrs.find(RGW_ATTR_AMZ_WEBSITE_REDIRECT_LOCATION);
  if (iter != attrs.end()) {
    bufferlist &bl = iter->second;
    s->redirect = string(bl.c_str(), bl.length());
    s->err->http_ret = 301;
    ldout(s->cct, 20) << __CEPH_ASSERT_FUNCTION << " redirecting per x-amz-website-redirect-location=" << s->redirect << dendl;
    op_ret = -ERR_WEBSITE_REDIRECT;
    s->set_req_state_err(op_ret);
    dump_errno(s);
    dump_content_length(s, 0);
    dump_redirect(s, s->redirect);
    end_header(s, this);
    return op_ret;
  } else {
    return RGWGetObj_ObjStore_S3::send_response_data(bl, bl_ofs, bl_len);
  }
}

int RGWGetObj_ObjStore_S3Website::send_response_data_error()
{
  return RGWGetObj_ObjStore_S3::send_response_data_error();
}

int RGWGetObj_ObjStore_S3::send_response_data_error()
{
  bufferlist bl;
  return send_response_data(bl, 0 , 0);
}

template <class T>
int decode_attr_bl_single_value(map<string, bufferlist>& attrs, const char *attr_name, T *result, T def_val)
{
  map<string, bufferlist>::iterator iter = attrs.find(attr_name);
  if (iter == attrs.end()) {
    *result = def_val;
    return 0;
  }
  bufferlist& bl = iter->second;
  if (bl.length() == 0) {
    *result = def_val;
    return 0;
  }
  bufferlist::iterator bliter = bl.begin();
  try {
    ::decode(*result, bliter);
  } catch (buffer::error& err) {
    return -EIO;
  }
  return 0;
}

int RGWGetObj_ObjStore_S3::send_response_data(bufferlist& bl, off_t bl_ofs,
					      off_t bl_len)
{
  const char *content_type = NULL;
  string content_type_str;
  map<string, string> response_attrs;
  map<string, string>::iterator riter;
  bufferlist metadata_bl;

  if (sent_header)
    goto send_data;

  if (custom_http_ret) {
    s->set_req_state_err(0);
    dump_errno(s, custom_http_ret);
  } else {
    s->set_req_state_err((partial_content && !op_ret) ? STATUS_PARTIAL_CONTENT
          	  : op_ret);
    dump_errno(s);
  }

  if (op_ret)
    goto done;

  if (range_str)
    dump_range(s, start, end, s->obj_size);

  if (s->system_request &&
      s->info.args.exists(RGW_SYS_PARAM_PREFIX "prepend-metadata")) {

    dump_header(s, "Rgwx-Object-Size", (long long)total_len);

    if (rgwx_stat) {
      /*
       * in this case, we're not returning the object's content, only the prepended
       * extra metadata
       */
      total_len = 0;
    }

    /* JSON encode object metadata */
    JSONFormatter jf;
    jf.open_object_section("obj_metadata");
    encode_json("attrs", attrs, &jf);
    utime_t ut(lastmod);
    encode_json("mtime", ut, &jf);
    jf.close_section();
    stringstream ss;
    jf.flush(ss);
    metadata_bl.append(ss.str());
    dump_header(s, "Rgwx-Embedded-Metadata-Len", metadata_bl.length());
    total_len += metadata_bl.length();
  }

  if (s->system_request && !real_clock::is_zero(lastmod)) {
    /* we end up dumping mtime in two different methods, a bit redundant */
    dump_epoch_header(s, "Rgwx-Mtime", lastmod);
    uint64_t pg_ver = 0;
    int r = decode_attr_bl_single_value(attrs, RGW_ATTR_PG_VER, &pg_ver, (uint64_t)0);
    if (r < 0) {
      ldout(s->cct, 0) << "ERROR: failed to decode pg ver attr, ignoring" << dendl;
    }
    dump_header(s, "Rgwx-Obj-PG-Ver", pg_ver);

    uint32_t source_zone_short_id = 0;
    r = decode_attr_bl_single_value(attrs, RGW_ATTR_SOURCE_ZONE, &source_zone_short_id, (uint32_t)0);
    if (r < 0) {
      ldout(s->cct, 0) << "ERROR: failed to decode pg ver attr, ignoring" << dendl;
    }
    if (source_zone_short_id != 0) {
      dump_header(s, "Rgwx-Source-Zone-Short-Id", source_zone_short_id);
    }
  }

  dump_content_length(s, total_len);
  dump_last_modified(s, lastmod);

  if (! op_ret) {
    if (! lo_etag.empty()) {
      /* Handle etag of Swift API's large objects (DLO/SLO). It's entirerly
       * legit to perform GET on them through S3 API. In such situation,
       * a client should receive the composited content with corresponding
       * etag value. */
      dump_etag(s, lo_etag);
    } else {
      auto iter = attrs.find(RGW_ATTR_ETAG);
      if (iter != attrs.end()) {
        dump_etag(s, iter->second);
      }
    }

    for (struct response_attr_param *p = resp_attr_params; p->param; p++) {
      bool exists;
      string val = s->info.args.get(p->param, &exists);
      if (exists) {
	if (strcmp(p->param, "response-content-type") != 0) {
	  response_attrs[p->http_attr] = val;
	} else {
	  content_type_str = val;
	  content_type = content_type_str.c_str();
	}
      }
    }

    for (auto iter = attrs.begin(); iter != attrs.end(); ++iter) {
      const char *name = iter->first.c_str();
      map<string, string>::iterator aiter = rgw_to_http_attrs.find(name);
      if (aiter != rgw_to_http_attrs.end()) {
        if (response_attrs.count(aiter->second) == 0) {
          /* Was not already overridden by a response param. */
          response_attrs[aiter->second] = iter->second.c_str();
        }
      } else if (iter->first.compare(RGW_ATTR_CONTENT_TYPE) == 0) {
        /* Special handling for content_type. */
        if (!content_type) {
          content_type = iter->second.c_str();
        }
      } else if (strncmp(name, RGW_ATTR_META_PREFIX,
			 sizeof(RGW_ATTR_META_PREFIX)-1) == 0) {
        /* User custom metadata. */
        name += sizeof(RGW_ATTR_PREFIX) - 1;
        dump_header(s, name, iter->second);
      }
    }
  }

done:
  for (riter = response_attrs.begin(); riter != response_attrs.end();
       ++riter) {
    dump_header(s, riter->first, riter->second);
  }

  if (op_ret == -ERR_NOT_MODIFIED) {
      end_header(s, this);
  } else {
      if (!content_type)
          content_type = "binary/octet-stream";

      end_header(s, this, content_type);
  }

  if (metadata_bl.length()) {
    dump_body(s, metadata_bl);
  }
  sent_header = true;

send_data:
  if (get_data && !op_ret) {
    int r = dump_body(s, bl.c_str() + bl_ofs, bl_len);
    if (r < 0)
      return r;
  }

  return 0;
}

void RGWListBuckets_ObjStore_S3::send_response_begin(bool has_buckets)
{
  if (op_ret)
    s->set_req_state_err(op_ret);
  dump_errno(s);
  dump_start(s);
  end_header(s, NULL, "application/xml");

  if (! op_ret) {
    list_all_buckets_start(s);
    dump_owner(s, s->user->user_id, s->user->display_name);
    s->formatter->open_array_section("Buckets");
    sent_data = true;
  }
}

void RGWListBuckets_ObjStore_S3::send_response_data(RGWUserBuckets& buckets)
{
  if (!sent_data)
    return;

  map<string, RGWBucketEnt>& m = buckets.get_buckets();
  map<string, RGWBucketEnt>::iterator iter;

  for (iter = m.begin(); iter != m.end(); ++iter) {
    RGWBucketEnt obj = iter->second;
    dump_bucket(s, obj);
  }
  rgw_flush_formatter(s, s->formatter);
}

void RGWListBuckets_ObjStore_S3::send_response_end()
{
  if (sent_data) {
    s->formatter->close_section();
    list_all_buckets_end(s);
    rgw_flush_formatter_and_reset(s, s->formatter);
  }
}

int RGWGetUsage_ObjStore_S3::get_params()
{
  start_date = s->info.args.get("start-date");
  end_date = s->info.args.get("end-date"); 
  return 0;
}

static void dump_usage_categories_info(Formatter *formatter, const rgw_usage_log_entry& entry, map<string, bool> *categories)
{
  formatter->open_array_section("categories");
  map<string, rgw_usage_data>::const_iterator uiter;
  for (uiter = entry.usage_map.begin(); uiter != entry.usage_map.end(); ++uiter) {
    if (categories && !categories->empty() && !categories->count(uiter->first))
      continue;
    const rgw_usage_data& usage = uiter->second;
    formatter->open_object_section("Entry");
    formatter->dump_string("Category", uiter->first);
    formatter->dump_int("BytesSent", usage.bytes_sent);
    formatter->dump_int("BytesReceived", usage.bytes_received);
    formatter->dump_int("Ops", usage.ops);
    formatter->dump_int("SuccessfulOps", usage.successful_ops);
    formatter->close_section(); // Entry
  }
  formatter->close_section(); // Category
}

void RGWGetUsage_ObjStore_S3::send_response()
{
  if (op_ret < 0)
    s->set_req_state_err(op_ret);
  dump_errno(s);

  end_header(s, this, "application/xml");
  dump_start(s);
  if (op_ret < 0)
    return;

  Formatter *formatter = s->formatter;
  string last_owner;
  bool user_section_open = false;
  
  formatter->open_object_section("Usage");
  if (show_log_entries) {
    formatter->open_array_section("Entries");
  }
  map<rgw_user_bucket, rgw_usage_log_entry>::iterator iter;
  for (iter = usage.begin(); iter != usage.end(); ++iter) {
    const rgw_user_bucket& ub = iter->first;
    const rgw_usage_log_entry& entry = iter->second;

    if (show_log_entries) {
      if (ub.user.compare(last_owner) != 0) {
        if (user_section_open) {
          formatter->close_section();
          formatter->close_section();
        }
        formatter->open_object_section("User");
        formatter->dump_string("Owner", ub.user);
        formatter->open_array_section("Buckets");
        user_section_open = true;
        last_owner = ub.user;
      }
      formatter->open_object_section("Bucket");
      formatter->dump_string("Bucket", ub.bucket);
      utime_t ut(entry.epoch, 0);
      ut.gmtime(formatter->dump_stream("Time"));
      formatter->dump_int("Epoch", entry.epoch);
      dump_usage_categories_info(formatter, entry, &categories);
      formatter->close_section(); // bucket
    }

    summary_map[ub.user].aggregate(entry, &categories);
  }

  if (show_log_entries) {
     if (user_section_open) {
       formatter->close_section(); // buckets
       formatter->close_section(); //user
     }
     formatter->close_section(); // entries
   }
  
   if (show_log_sum) {
     formatter->open_array_section("Summary");
     map<string, rgw_usage_log_entry>::iterator siter;
     for (siter = summary_map.begin(); siter != summary_map.end(); ++siter) {
       const rgw_usage_log_entry& entry = siter->second;
       formatter->open_object_section("User");
       formatter->dump_string("User", siter->first);
       dump_usage_categories_info(formatter, entry, &categories);
       rgw_usage_data total_usage;
       entry.sum(total_usage, categories);
       formatter->open_object_section("Total");
       formatter->dump_int("BytesSent", total_usage.bytes_sent);
       formatter->dump_int("BytesReceived", total_usage.bytes_received);
       formatter->dump_int("Ops", total_usage.ops);
       formatter->dump_int("SuccessfulOps", total_usage.successful_ops);
       formatter->close_section(); // total 
       formatter->close_section(); // user
     }

     if (s->cct->_conf->rgw_rest_getusage_op_compat) {
       formatter->open_object_section("Stats"); 
     }	
       
     formatter->dump_int("TotalBytes", header.stats.total_bytes);
     formatter->dump_int("TotalBytesRounded", header.stats.total_bytes_rounded);
     formatter->dump_int("TotalEntries", header.stats.total_entries);
     
     if (s->cct->_conf->rgw_rest_getusage_op_compat) { 
       formatter->close_section(); //Stats
     }

     formatter->close_section(); // summary
   }
   formatter->close_section(); // usage
   rgw_flush_formatter_and_reset(s, s->formatter);
}

int RGWListBucket_ObjStore_S3::get_params()
{
  list_versions = s->info.args.exists("versions");
  prefix = s->info.args.get("prefix");
  if (!list_versions) {
    marker = s->info.args.get("marker");
  } else {
    marker.name = s->info.args.get("key-marker");
    marker.instance = s->info.args.get("version-id-marker");
  }
  max_keys = s->info.args.get("max-keys");
  op_ret = parse_max_keys();
  if (op_ret < 0) {
    return op_ret;
  }
  delimiter = s->info.args.get("delimiter");
  encoding_type = s->info.args.get("encoding-type");
  if (s->system_request) {
    s->info.args.get_bool("objs-container", &objs_container, false);
    const char *shard_id_str = s->info.env->get("HTTP_RGWX_SHARD_ID");
    if (shard_id_str) {
      string err;
      shard_id = strict_strtol(shard_id_str, 10, &err);
      if (!err.empty()) {
        ldout(s->cct, 5) << "bad shard id specified: " << shard_id_str << dendl;
        return -EINVAL;
      }
    } else {
      shard_id = s->bucket_instance_shard_id;
    }
  }
  return 0;
}

void RGWListBucket_ObjStore_S3::send_versioned_response()
{
  s->formatter->open_object_section_in_ns("ListVersionsResult", XMLNS_AWS_S3);
  if (!s->bucket_tenant.empty())
    s->formatter->dump_string("Tenant", s->bucket_tenant);
  s->formatter->dump_string("Name", s->bucket_name);
  s->formatter->dump_string("Prefix", prefix);
  s->formatter->dump_string("KeyMarker", marker.name);
  if (is_truncated && !next_marker.empty())
    s->formatter->dump_string("NextKeyMarker", next_marker.name);
  s->formatter->dump_int("MaxKeys", max);
  if (!delimiter.empty())
    s->formatter->dump_string("Delimiter", delimiter);

  s->formatter->dump_string("IsTruncated", (max && is_truncated ? "true"
					    : "false"));

  bool encode_key = false;
  if (strcasecmp(encoding_type.c_str(), "url") == 0) {
    s->formatter->dump_string("EncodingType", "url");
    encode_key = true;
  }

  if (op_ret >= 0) {
    if (objs_container) {
      s->formatter->open_array_section("Entries");
    }

    vector<RGWObjEnt>::iterator iter;
    for (iter = objs.begin(); iter != objs.end(); ++iter) {
      const char *section_name = (iter->is_delete_marker() ? "DeleteMarker"
				  : "Version");
      s->formatter->open_object_section(section_name);
      if (objs_container) {
        s->formatter->dump_bool("IsDeleteMarker", iter->is_delete_marker());
      }
      if (encode_key) {
	string key_name;
	url_encode(iter->key.name, key_name);
	s->formatter->dump_string("Key", key_name);
      } else {
	s->formatter->dump_string("Key", iter->key.name);
      }
      string version_id = iter->key.instance;
      if (version_id.empty()) {
	version_id = "null";
      }
      if (s->system_request) {
        if (iter->versioned_epoch > 0) {
          s->formatter->dump_int("VersionedEpoch", iter->versioned_epoch);
        }
        s->formatter->dump_string("RgwxTag", iter->tag);
        utime_t ut(iter->mtime);
        ut.gmtime_nsec(s->formatter->dump_stream("RgwxMtime"));
      }
      s->formatter->dump_string("VersionId", version_id);
      s->formatter->dump_bool("IsLatest", iter->is_current());
      dump_time(s, "LastModified", &iter->mtime);
      if (!iter->is_delete_marker()) {
	s->formatter->dump_format("ETag", "\"%s\"", iter->etag.c_str());
	s->formatter->dump_int("Size", iter->accounted_size);
	s->formatter->dump_string("StorageClass", "STANDARD");
      }
      dump_owner(s, iter->owner, iter->owner_display_name);
      s->formatter->close_section();
    }
    if (objs_container) {
      s->formatter->close_section();
    }

    if (!common_prefixes.empty()) {
      map<string, bool>::iterator pref_iter;
      for (pref_iter = common_prefixes.begin();
	   pref_iter != common_prefixes.end(); ++pref_iter) {
	s->formatter->open_array_section("CommonPrefixes");
	s->formatter->dump_string("Prefix", pref_iter->first);
	s->formatter->close_section();
      }
    }
  }
  s->formatter->close_section();
  rgw_flush_formatter_and_reset(s, s->formatter);
}

void RGWListBucket_ObjStore_S3::send_response()
{
  if (op_ret < 0)
    s->set_req_state_err(op_ret);
  dump_errno(s);

  end_header(s, this, "application/xml");
  dump_start(s);
  if (op_ret < 0)
    return;

  if (list_versions) {
    send_versioned_response();
    return;
  }

  s->formatter->open_object_section_in_ns("ListBucketResult", XMLNS_AWS_S3);
  if (!s->bucket_tenant.empty())
    s->formatter->dump_string("Tenant", s->bucket_tenant);
  s->formatter->dump_string("Name", s->bucket_name);
  s->formatter->dump_string("Prefix", prefix);
  s->formatter->dump_string("Marker", marker.name);
  if (is_truncated && !next_marker.empty())
    s->formatter->dump_string("NextMarker", next_marker.name);
  s->formatter->dump_int("MaxKeys", max);
  if (!delimiter.empty())
    s->formatter->dump_string("Delimiter", delimiter);

  s->formatter->dump_string("IsTruncated", (max && is_truncated ? "true"
					    : "false"));

  bool encode_key = false;
  if (strcasecmp(encoding_type.c_str(), "url") == 0) {
    s->formatter->dump_string("EncodingType", "url");
    encode_key = true;
  }

  if (op_ret >= 0) {
    vector<RGWObjEnt>::iterator iter;
    for (iter = objs.begin(); iter != objs.end(); ++iter) {
      s->formatter->open_array_section("Contents");
      if (encode_key) {
	string key_name;
	url_encode(iter->key.name, key_name);
	s->formatter->dump_string("Key", key_name);
      } else {
	s->formatter->dump_string("Key", iter->key.name);
      }
      dump_time(s, "LastModified", &iter->mtime);
      s->formatter->dump_format("ETag", "\"%s\"", iter->etag.c_str());
      s->formatter->dump_int("Size", iter->accounted_size);
      s->formatter->dump_string("StorageClass", "STANDARD");
      dump_owner(s, iter->owner, iter->owner_display_name);
      if (s->system_request) {
        s->formatter->dump_string("RgwxTag", iter->tag);
      }
      s->formatter->close_section();
    }
    if (!common_prefixes.empty()) {
      map<string, bool>::iterator pref_iter;
      for (pref_iter = common_prefixes.begin();
	   pref_iter != common_prefixes.end(); ++pref_iter) {
	s->formatter->open_array_section("CommonPrefixes");
	s->formatter->dump_string("Prefix", pref_iter->first);
	s->formatter->close_section();
      }
    }
  }
  s->formatter->close_section();
  rgw_flush_formatter_and_reset(s, s->formatter);
}

void RGWGetBucketLogging_ObjStore_S3::send_response()
{
  dump_errno(s);
  end_header(s, this, "application/xml");
  dump_start(s);

  s->formatter->open_object_section_in_ns("BucketLoggingStatus", XMLNS_AWS_S3);
  s->formatter->close_section();
  rgw_flush_formatter_and_reset(s, s->formatter);
}

void RGWGetBucketLocation_ObjStore_S3::send_response()
{
  dump_errno(s);
  end_header(s, this);
  dump_start(s);

  RGWZoneGroup zonegroup;
  string api_name;

  int ret = store->get_zonegroup(s->bucket_info.zonegroup, zonegroup);
  if (ret >= 0) {
    api_name = zonegroup.api_name;
  } else  {
    if (s->bucket_info.zonegroup != "default") {
      api_name = s->bucket_info.zonegroup;
    }
  }

  s->formatter->dump_format_ns("LocationConstraint", XMLNS_AWS_S3,
			       "%s", api_name.c_str());
  rgw_flush_formatter_and_reset(s, s->formatter);
}

void RGWGetBucketVersioning_ObjStore_S3::send_response()
{
  dump_errno(s);
  end_header(s, this, "application/xml");
  dump_start(s);

  s->formatter->open_object_section_in_ns("VersioningConfiguration", XMLNS_AWS_S3);
  if (versioned) {
    const char *status = (versioning_enabled ? "Enabled" : "Suspended");
    s->formatter->dump_string("Status", status);
  }
  s->formatter->close_section();
  rgw_flush_formatter_and_reset(s, s->formatter);
}

class RGWSetBucketVersioningParser : public RGWXMLParser
{
  XMLObj *alloc_obj(const char *el) {
    return new XMLObj;
  }

public:
  RGWSetBucketVersioningParser() {}
  ~RGWSetBucketVersioningParser() {}

  int get_versioning_status(bool *status) {
    XMLObj *config = find_first("VersioningConfiguration");
    if (!config)
      return -EINVAL;

    *status = false;

    XMLObj *field = config->find_first("Status");
    if (!field)
      return 0;

    string& s = field->get_data();

    if (stringcasecmp(s, "Enabled") == 0) {
      *status = true;
    } else if (stringcasecmp(s, "Suspended") != 0) {
      return -EINVAL;
    }

    return 0;
  }
};

int RGWSetBucketVersioning_ObjStore_S3::get_params()
{
#define GET_BUCKET_VERSIONING_BUF_MAX (128 * 1024)

  char *data = nullptr;
  int len = 0;
  int r =
    rgw_rest_read_all_input(s, &data, &len, GET_BUCKET_VERSIONING_BUF_MAX);
  if (r < 0) {
    return r;
  }
  
  auto data_deleter = std::unique_ptr<char, decltype(free)*>{data, free};

  if (s->aws4_auth_needs_complete) {
    int ret_auth = do_aws4_auth_completion();
    if (ret_auth < 0) {
      return ret_auth;
    }
  }

  RGWSetBucketVersioningParser parser;

  if (!parser.init()) {
    ldout(s->cct, 0) << "ERROR: failed to initialize parser" << dendl;
    r = -EIO;
    return r;
  }

  if (!parser.parse(data, len, 1)) {
    ldout(s->cct, 10) << "failed to parse data: " << data << dendl;
    r = -EINVAL;
    return r;
  }

  if (!store->is_meta_master()) {
    /* only need to keep this data around if we're not meta master */
    in_data.append(data, len);
  }

  r = parser.get_versioning_status(&enable_versioning);
  
  return r;
}

void RGWSetBucketVersioning_ObjStore_S3::send_response()
{
  if (op_ret)
    s->set_req_state_err(op_ret);
  dump_errno(s);
  end_header(s);
}

int RGWSetBucketWebsite_ObjStore_S3::get_params()
{
  static constexpr uint32_t GET_BUCKET_WEBSITE_BUF_MAX = (128 * 1024);

  char *data = nullptr;
  int len = 0;
  int r = rgw_rest_read_all_input(s, &data, &len, GET_BUCKET_WEBSITE_BUF_MAX);
  if (r < 0) {
    return r;
  }

  auto data_deleter = std::unique_ptr<char, decltype(free)*>{data, free};

  if (s->aws4_auth_needs_complete) {
      int ret_auth = do_aws4_auth_completion();
      if (ret_auth < 0) {
        return ret_auth;
      }
  }

  RGWXMLDecoder::XMLParser parser;
  if (!parser.init()) {
    ldout(s->cct, 0) << "ERROR: failed to initialize parser" << dendl;
    return -EIO;
  }

  if (!parser.parse(data, len, 1)) {
    string str(data, len);
    ldout(s->cct, 5) << "failed to parse xml: " << str << dendl;
    return -EINVAL;
  }

  try {
    RGWXMLDecoder::decode_xml("WebsiteConfiguration", website_conf, &parser, true);
  } catch (RGWXMLDecoder::err& err) {
    string str(data, len);
    ldout(s->cct, 5) << "unexpected xml: " << str << dendl;
    return -EINVAL;
  }

  return 0;
}

void RGWSetBucketWebsite_ObjStore_S3::send_response()
{
  if (op_ret < 0)
    s->set_req_state_err(op_ret);
  dump_errno(s);
  end_header(s);
}

void RGWDeleteBucketWebsite_ObjStore_S3::send_response()
{
  if (op_ret == 0) {
    op_ret = STATUS_NO_CONTENT;
  }
  s->set_req_state_err(op_ret);
  dump_errno(s);
  end_header(s);
}

void RGWGetBucketWebsite_ObjStore_S3::send_response()
{
  if (op_ret)
    s->set_req_state_err(op_ret);
  dump_errno(s);
  end_header(s, this, "application/xml");
  dump_start(s);

  if (op_ret < 0) {
    return;
  }

  RGWBucketWebsiteConf& conf = s->bucket_info.website_conf;

  s->formatter->open_object_section_in_ns("WebsiteConfiguration", XMLNS_AWS_S3);
  conf.dump_xml(s->formatter);
  s->formatter->close_section(); // WebsiteConfiguration
  rgw_flush_formatter_and_reset(s, s->formatter);
}

static void dump_bucket_metadata(struct req_state *s, RGWBucketEnt& bucket)
{
  dump_header(s, "X-RGW-Object-Count", static_cast<long long>(bucket.count));
  dump_header(s, "X-RGW-Bytes-Used", static_cast<long long>(bucket.size));
}

void RGWStatBucket_ObjStore_S3::send_response()
{
  if (op_ret >= 0) {
    dump_bucket_metadata(s, bucket);
  }

  s->set_req_state_err(op_ret);
  dump_errno(s);

  end_header(s, this);
  dump_start(s);
}

static int create_s3_policy(struct req_state *s, RGWRados *store,
			    RGWAccessControlPolicy_S3& s3policy,
			    ACLOwner& owner)
{
  if (s->has_acl_header) {
    if (!s->canned_acl.empty())
      return -ERR_INVALID_REQUEST;

    return s3policy.create_from_headers(store, s->info.env, owner);
  }

  return s3policy.create_canned(owner, s->bucket_owner, s->canned_acl);
}

class RGWLocationConstraint : public XMLObj
{
public:
  RGWLocationConstraint() {}
  ~RGWLocationConstraint() {}
  bool xml_end(const char *el) {
    if (!el)
      return false;

    location_constraint = get_data();

    return true;
  }

  string location_constraint;
};

class RGWCreateBucketConfig : public XMLObj
{
public:
  RGWCreateBucketConfig() {}
  ~RGWCreateBucketConfig() {}
};

class RGWCreateBucketParser : public RGWXMLParser
{
  XMLObj *alloc_obj(const char *el) {
    return new XMLObj;
  }

public:
  RGWCreateBucketParser() {}
  ~RGWCreateBucketParser() {}

  bool get_location_constraint(string& zone_group) {
    XMLObj *config = find_first("CreateBucketConfiguration");
    if (!config)
      return false;

    XMLObj *constraint = config->find_first("LocationConstraint");
    if (!constraint)
      return false;

    zone_group = constraint->get_data();

    return true;
  }
};

int RGWCreateBucket_ObjStore_S3::get_params()
{
  RGWAccessControlPolicy_S3 s3policy(s->cct);

  int r = create_s3_policy(s, store, s3policy, s->owner);
  if (r < 0)
    return r;

  policy = s3policy;

  int len = 0;
  char *data = nullptr;
#define CREATE_BUCKET_MAX_REQ_LEN (512 * 1024) /* this is way more than enough */
  op_ret = rgw_rest_read_all_input(s, &data, &len, CREATE_BUCKET_MAX_REQ_LEN);
  if ((op_ret < 0) && (op_ret != -ERR_LENGTH_REQUIRED))
    return op_ret;

  auto data_deleter = std::unique_ptr<char, decltype(free)*>{data, free};

  if (s->aws4_auth_needs_complete) {
    int ret_auth = do_aws4_auth_completion();
    if (ret_auth < 0) {
      return ret_auth;
    }
  }
  
  bufferptr in_ptr(data, len);
  in_data.append(in_ptr);

  if (len) {
    RGWCreateBucketParser parser;

    if (!parser.init()) {
      ldout(s->cct, 0) << "ERROR: failed to initialize parser" << dendl;
      return -EIO;
    }

    bool success = parser.parse(data, len, 1);
    ldout(s->cct, 20) << "create bucket input data=" << data << dendl;

    if (!success) {
      ldout(s->cct, 0) << "failed to parse input: " << data << dendl;
      return -EINVAL;
    }

    if (!parser.get_location_constraint(location_constraint)) {
      ldout(s->cct, 0) << "provided input did not specify location constraint correctly" << dendl;
      return -EINVAL;
    }

    ldout(s->cct, 10) << "create bucket location constraint: "
		      << location_constraint << dendl;
  }

  size_t pos = location_constraint.find(':');
  if (pos != string::npos) {
    placement_rule = location_constraint.substr(pos + 1);
    location_constraint = location_constraint.substr(0, pos);
  }

  return 0;
}

void RGWCreateBucket_ObjStore_S3::send_response()
{
  if (op_ret == -ERR_BUCKET_EXISTS)
    op_ret = 0;
  if (op_ret)
    s->set_req_state_err(op_ret);
  dump_errno(s);
  end_header(s);

  if (op_ret < 0)
    return;

  if (s->system_request) {
    JSONFormatter f; /* use json formatter for system requests output */

    f.open_object_section("info");
    encode_json("entry_point_object_ver", ep_objv, &f);
    encode_json("object_ver", info.objv_tracker.read_version, &f);
    encode_json("bucket_info", info, &f);
    f.close_section();
    rgw_flush_formatter_and_reset(s, &f);
  }
}

void RGWDeleteBucket_ObjStore_S3::send_response()
{
  int r = op_ret;
  if (!r)
    r = STATUS_NO_CONTENT;

  s->set_req_state_err(r);
  dump_errno(s);
  end_header(s, this);

  if (s->system_request) {
    JSONFormatter f; /* use json formatter for system requests output */

    f.open_object_section("info");
    encode_json("object_ver", objv_tracker.read_version, &f);
    f.close_section();
    rgw_flush_formatter_and_reset(s, &f);
  }
}

int RGWPutObj_ObjStore_S3::get_params()
{
  RGWObjectCtx& obj_ctx = *static_cast<RGWObjectCtx *>(s->obj_ctx);
  map<string, bufferlist> src_attrs;
  size_t pos;
  int ret;

  RGWAccessControlPolicy_S3 s3policy(s->cct);
  if (!s->length)
    return -ERR_LENGTH_REQUIRED;

  ret = create_s3_policy(s, store, s3policy, s->owner);
  if (ret < 0)
    return ret;

  policy = s3policy;

  if_match = s->info.env->get("HTTP_IF_MATCH");
  if_nomatch = s->info.env->get("HTTP_IF_NONE_MATCH");
  copy_source = s->info.env->get("HTTP_X_AMZ_COPY_SOURCE");
  copy_source_range = s->info.env->get("HTTP_X_AMZ_COPY_SOURCE_RANGE");

  /* handle x-amz-copy-source */

  if (copy_source) {
    copy_source_bucket_name = copy_source;
    pos = copy_source_bucket_name.find("/");
    if (pos == std::string::npos) {
      ret = -EINVAL;
      ldout(s->cct, 5) << "x-amz-copy-source bad format" << dendl;
      return ret;
    }
    copy_source_object_name = copy_source_bucket_name.substr(pos + 1, copy_source_bucket_name.size());
    copy_source_bucket_name = copy_source_bucket_name.substr(0, pos);
#define VERSION_ID_STR "?versionId="
    pos = copy_source_object_name.find(VERSION_ID_STR);
    if (pos == std::string::npos) {
      url_decode(copy_source_object_name, copy_source_object_name);
    } else {
      copy_source_version_id = copy_source_object_name.substr(pos + sizeof(VERSION_ID_STR) - 1);
      url_decode(copy_source_object_name.substr(0, pos), copy_source_object_name);
    }
    pos = copy_source_bucket_name.find(":");
    if (pos == std::string::npos) {
       copy_source_tenant_name = s->src_tenant_name;
    } else {
       copy_source_tenant_name = copy_source_bucket_name.substr(0, pos);
       copy_source_bucket_name = copy_source_bucket_name.substr(pos + 1, copy_source_bucket_name.size());
       if (copy_source_bucket_name.empty()) {
         ret = -EINVAL;
         ldout(s->cct, 5) << "source bucket name is empty" << dendl;
         return ret;
       }
    }
    ret = store->get_bucket_info(obj_ctx,
                                 copy_source_tenant_name,
                                 copy_source_bucket_name,
                                 copy_source_bucket_info,
                                 NULL, &src_attrs);
    if (ret < 0) {
      ldout(s->cct, 5) << __func__ << "(): get_bucket_info() returned ret=" << ret << dendl;
      return ret;
    }

    /* handle x-amz-copy-source-range */

    if (copy_source_range) {
      string range = copy_source_range;
      pos = range.find("=");
      if (pos == std::string::npos) {
        ret = -EINVAL;
        ldout(s->cct, 5) << "x-amz-copy-source-range bad format" << dendl;
        return ret;
      }
      range = range.substr(pos + 1);
      pos = range.find("-");
      if (pos == std::string::npos) {
        ret = -EINVAL;
        ldout(s->cct, 5) << "x-amz-copy-source-range bad format" << dendl;
        return ret;
      }
      string first = range.substr(0, pos);
      string last = range.substr(pos + 1);
      copy_source_range_fst = strtoull(first.c_str(), NULL, 10);
      copy_source_range_lst = strtoull(last.c_str(), NULL, 10);
    }

  } /* copy_source */

  return RGWPutObj_ObjStore::get_params();
}

int RGWPutObj_ObjStore_S3::validate_aws4_single_chunk(char *chunk_str,
                                                      char *chunk_data_str,
                                                      unsigned int chunk_data_size,
                                                      string chunk_signature)
{

  /* string to sign */

  string hash_empty_str;
  rgw_hash_s3_string_sha256("", 0, hash_empty_str);

  string hash_chunk_data;
  rgw_hash_s3_string_sha256(chunk_data_str, chunk_data_size, hash_chunk_data);

  string string_to_sign = "AWS4-HMAC-SHA256-PAYLOAD\n";
  string_to_sign.append(s->aws4_auth->date + "\n");
  string_to_sign.append(s->aws4_auth->credential_scope + "\n");
  string_to_sign.append(s->aws4_auth->seed_signature + "\n");
  string_to_sign.append(hash_empty_str + "\n");
  string_to_sign.append(hash_chunk_data);

  /* new chunk signature */

  char signature_k[CEPH_CRYPTO_HMACSHA256_DIGESTSIZE];
  calc_hmac_sha256(s->aws4_auth->signing_k, CEPH_CRYPTO_HMACSHA256_DIGESTSIZE,
      string_to_sign.c_str(), string_to_sign.size(), signature_k);

  char aux[CEPH_CRYPTO_HMACSHA256_DIGESTSIZE * 2 + 1];
  buf_to_hex((unsigned char *) signature_k, CEPH_CRYPTO_HMACSHA256_DIGESTSIZE, aux);

  string new_chunk_signature = string(aux);

  ldout(s->cct, 20) << "--------------- aws4 chunk validation" << dendl;
  ldout(s->cct, 20) << "chunk_signature     = " << chunk_signature << dendl;
  ldout(s->cct, 20) << "new_chunk_signature = " << new_chunk_signature << dendl;
  ldout(s->cct, 20) << "aws4 chunk signing_key    = " << s->aws4_auth->signing_key << dendl;
  ldout(s->cct, 20) << "aws4 chunk string_to_sign = " << string_to_sign << dendl;

  /* chunk auth ok? */

  if (new_chunk_signature != chunk_signature) {
    ldout(s->cct, 20) << "ERROR: AWS4 chunk signature does NOT match (new_chunk_signature != chunk_signature)" << dendl;
    return -ERR_SIGNATURE_NO_MATCH;
  }

  /* update seed signature */

  s->aws4_auth->seed_signature = new_chunk_signature;

  return 0;
}

int RGWPutObj_ObjStore_S3::validate_and_unwrap_available_aws4_chunked_data(bufferlist& bl_in,
                                                                           bufferlist& bl_out)
{

  /* string(IntHexBase(chunk-size)) + ";chunk-signature=" + signature + \r\n + chunk-data + \r\n */

  const unsigned int chunk_str_min_len = 1 + 17 + 64 + 2; /* len('0') = 1 */

  char *chunk_str = bl_in.c_str();
  unsigned int budget = bl_in.length();

  bl_out.clear();

  while (true) {

    /* check available metadata */

    if (budget < chunk_str_min_len) {
      return -ERR_SIGNATURE_NO_MATCH;
    }

    unsigned int chunk_offset = 0;

    /* grab chunk size */

    while ((*(chunk_str+chunk_offset) != ';') && (chunk_offset < chunk_str_min_len))
      chunk_offset++;
    string str = string(chunk_str, chunk_offset);
    unsigned int chunk_data_size;
    stringstream ss;
    ss << std::hex << str;
    ss >> chunk_data_size;
    if (ss.fail()) {
      return -ERR_SIGNATURE_NO_MATCH;
    }

    /* grab chunk signature */

    chunk_offset += 17;
    string chunk_signature = string(chunk_str, chunk_offset, 64);

    /* get chunk data */

    chunk_offset += 64 + 2;
    char *chunk_data_str = chunk_str + chunk_offset;

    /* handle budget */

    budget -= chunk_offset;
    if (budget < chunk_data_size) {
      return -ERR_SIGNATURE_NO_MATCH;
    } else {
      budget -= chunk_data_size;
    }

    /* auth single chunk */

    if (validate_aws4_single_chunk(chunk_str, chunk_data_str, chunk_data_size, chunk_signature) < 0) {
      ldout(s->cct, 20) << "ERROR AWS4 single chunk validation" << dendl;
      return -ERR_SIGNATURE_NO_MATCH;
    }

    /* aggregate single chunk */

    bl_out.append(chunk_data_str, chunk_data_size);

    /* last chunk or no more budget? */

    if ((chunk_data_size == 0) || (budget == 0))
      break;

    /* next chunk */

    chunk_offset += chunk_data_size;
    chunk_str += chunk_offset;
  }

  /* authorization ok */

  return 0;

}

int RGWPutObj_ObjStore_S3::get_data(bufferlist& bl)
{
  int ret = RGWPutObj_ObjStore::get_data(bl);
  if (ret < 0)
    s->aws4_auth_needs_complete = false;

  int ret_auth;

  if (s->aws4_auth_streaming_mode && ret > 0) {
    ret_auth = validate_and_unwrap_available_aws4_chunked_data(bl, s->aws4_auth->bl);
    if (ret_auth < 0) {
      return ret_auth;
    }
  }

  if ((ret == 0) && s->aws4_auth_needs_complete) {
    ret_auth = do_aws4_auth_completion();
    if (ret_auth < 0) {
      return ret_auth;
    }
  }

  return ret;
}

static int get_success_retcode(int code)
{
  switch (code) {
    case 201:
      return STATUS_CREATED;
    case 204:
      return STATUS_NO_CONTENT;
  }
  return 0;
}

void RGWPutObj_ObjStore_S3::send_response()
{
  if (op_ret) {
    s->set_req_state_err(op_ret);
    dump_errno(s);
  } else {
    if (s->cct->_conf->rgw_s3_success_create_obj_status) {
      op_ret = get_success_retcode(
	s->cct->_conf->rgw_s3_success_create_obj_status);
      s->set_req_state_err(op_ret);
    }
    if (!copy_source) {
      dump_errno(s);
      dump_etag(s, etag);
      dump_content_length(s, 0);
    } else {
      dump_errno(s);
      end_header(s, this, "application/xml");
      dump_start(s);
      struct tm tmp;
      utime_t ut(mtime);
      time_t secs = (time_t)ut.sec();
      gmtime_r(&secs, &tmp);
      char buf[TIME_BUF_SIZE];
      s->formatter->open_object_section_in_ns("CopyPartResult",
          "http://s3.amazonaws.com/doc/2006-03-01/");
      if (strftime(buf, sizeof(buf), "%Y-%m-%dT%T.000Z", &tmp) > 0) {
        s->formatter->dump_string("LastModified", buf);
      }
      s->formatter->dump_string("ETag", etag);
      s->formatter->close_section();
      rgw_flush_formatter_and_reset(s, s->formatter);
      return;
    }
  }
  if (s->system_request && !real_clock::is_zero(mtime)) {
    dump_epoch_header(s, "Rgwx-Mtime", mtime);
  }
  end_header(s, this);
}

/*
 * parses params in the format: 'first; param1=foo; param2=bar'
 */
static void parse_params(const string& params_str, string& first,
			 map<string, string>& params)
{
  int pos = params_str.find(';');
  if (pos < 0) {
    first = rgw_trim_whitespace(params_str);
    return;
  }

  first = rgw_trim_whitespace(params_str.substr(0, pos));

  pos++;

  while (pos < (int)params_str.size()) {
    ssize_t end = params_str.find(';', pos);
    if (end < 0)
      end = params_str.size();

    string param = params_str.substr(pos, end - pos);

    int eqpos = param.find('=');
    if (eqpos > 0) {
      string param_name = rgw_trim_whitespace(param.substr(0, eqpos));
      string val = rgw_trim_quotes(param.substr(eqpos + 1));
      params[param_name] = val;
    } else {
      params[rgw_trim_whitespace(param)] = "";
    }

    pos = end + 1;
  }
}

static int parse_part_field(const string& line, string& field_name,
			    struct post_part_field& field)
{
  int pos = line.find(':');
  if (pos < 0)
    return -EINVAL;

  field_name = line.substr(0, pos);
  if (pos >= (int)line.size() - 1)
    return 0;

  parse_params(line.substr(pos + 1), field.val, field.params);

  return 0;
}

bool is_crlf(const char *s)
{
  return (*s == '\r' && *(s + 1) == '\n');
}

/*
 * find the index of the boundary, if exists, or optionally the next end of line
 * also returns how many bytes to skip
 */
static int index_of(bufferlist& bl, int max_len, const string& str,
		    bool check_crlf,
		    bool *reached_boundary, int *skip)
{
  *reached_boundary = false;
  *skip = 0;

  if (str.size() < 2) // we assume boundary is at least 2 chars (makes it easier with crlf checks)
    return -EINVAL;

  if (bl.length() < str.size())
    return -1;

  const char *buf = bl.c_str();
  const char *s = str.c_str();

  if (max_len > (int)bl.length())
    max_len = bl.length();

  int i;
  for (i = 0; i < max_len; i++, buf++) {
    if (check_crlf &&
	i >= 1 &&
	is_crlf(buf - 1)) {
      return i + 1; // skip the crlf
    }
    if ((i < max_len - (int)str.size() + 1) &&
	(buf[0] == s[0] && buf[1] == s[1]) &&
	(strncmp(buf, s, str.size()) == 0)) {
      *reached_boundary = true;
      *skip = str.size();

      /* oh, great, now we need to swallow the preceding crlf
       * if exists
       */
      if ((i >= 2) &&
	  is_crlf(buf - 2)) {
	i -= 2;
	*skip += 2;
      }
      return i;
    }
  }

  return -1;
}

int RGWPostObj_ObjStore_S3::read_with_boundary(bufferlist& bl, uint64_t max,
					       bool check_crlf,
					       bool *reached_boundary,
					       bool *done)
{
  uint64_t cl = max + 2 + boundary.size();

  if (max > in_data.length()) {
    uint64_t need_to_read = cl - in_data.length();

    bufferptr bp(need_to_read);

    const auto read_len = recv_body(s, bp.c_str(), need_to_read);
    in_data.append(bp, 0, read_len);
  }

  *done = false;
  int skip;
  int index = index_of(in_data, cl, boundary, check_crlf, reached_boundary,
		       &skip);
  if (index >= 0)
    max = index;

  if (max > in_data.length())
    max = in_data.length();

  bl.substr_of(in_data, 0, max);

  bufferlist new_read_data;

  /*
   * now we need to skip boundary for next time, also skip any crlf, or
   * check to see if it's the last final boundary (marked with "--" at the end
   */
  if (*reached_boundary) {
    int left = in_data.length() - max;
    if (left < skip + 2) {
      int need = skip + 2 - left;
      bufferptr boundary_bp(need);
      recv_body(s, boundary_bp.c_str(), need);
      in_data.append(boundary_bp);
    }
    max += skip; // skip boundary for next time
    if (in_data.length() >= max + 2) {
      const char *data = in_data.c_str();
      if (is_crlf(data + max)) {
	max += 2;
      } else {
	if (*(data + max) == '-' &&
	    *(data + max + 1) == '-') {
	  *done = true;
	  max += 2;
	}
      }
    }
  }

  new_read_data.substr_of(in_data, max, in_data.length() - max);
  in_data = new_read_data;

  return 0;
}

int RGWPostObj_ObjStore_S3::read_line(bufferlist& bl, uint64_t max,
				  bool *reached_boundary, bool *done)
{
  return read_with_boundary(bl, max, true, reached_boundary, done);
}

int RGWPostObj_ObjStore_S3::read_data(bufferlist& bl, uint64_t max,
				  bool *reached_boundary, bool *done)
{
  return read_with_boundary(bl, max, false, reached_boundary, done);
}


int RGWPostObj_ObjStore_S3::read_form_part_header(struct post_form_part *part,
						  bool *done)
{
  bufferlist bl;
  bool reached_boundary;
  uint64_t chunk_size = s->cct->_conf->rgw_max_chunk_size;
  int r = read_line(bl, chunk_size, &reached_boundary, done);
  if (r < 0)
    return r;

  if (*done) {
    return 0;
  }

  if (reached_boundary) { // skip the first boundary
    r = read_line(bl, chunk_size, &reached_boundary, done);
    if (r < 0)
      return r;
    if (*done)
      return 0;
  }

  while (true) {
  /*
   * iterate through fields
   */
    string line = rgw_trim_whitespace(string(bl.c_str(), bl.length()));

    if (line.empty())
      break;

    struct post_part_field field;

    string field_name;
    r = parse_part_field(line, field_name, field);
    if (r < 0)
      return r;

    part->fields[field_name] = field;

    if (stringcasecmp(field_name, "Content-Disposition") == 0) {
      part->name = field.params["name"];
    }

    if (reached_boundary)
      break;

    r = read_line(bl, chunk_size, &reached_boundary, done);
  }

  return 0;
}

bool RGWPostObj_ObjStore_S3::part_str(const string& name, string *val)
{
  map<string, struct post_form_part, ltstr_nocase>::iterator iter
    = parts.find(name);
  if (iter == parts.end())
    return false;

  bufferlist& data = iter->second.data;
  string str = string(data.c_str(), data.length());
  *val = rgw_trim_whitespace(str);
  return true;
}

bool RGWPostObj_ObjStore_S3::part_bl(const string& name, bufferlist *pbl)
{
  map<string, struct post_form_part, ltstr_nocase>::iterator iter =
    parts.find(name);
  if (iter == parts.end())
    return false;

  *pbl = iter->second.data;
  return true;
}

void RGWPostObj_ObjStore_S3::rebuild_key(string& key)
{
  static string var = "${filename}";
  int pos = key.find(var);
  if (pos < 0)
    return;

  string new_key = key.substr(0, pos);
  new_key.append(filename);
  new_key.append(key.substr(pos + var.size()));

  key = new_key;
}

int RGWPostObj_ObjStore_S3::get_params()
{
  // get the part boundary
  string req_content_type_str = s->info.env->get("CONTENT_TYPE", "");
  string req_content_type;
  map<string, string> params;

  if (s->expect_cont) {
    /* ok, here it really gets ugly. With POST, the params are embedded in the
     * request body, so we need to continue before being able to actually look
     * at them. This diverts from the usual request flow.
     */
    dump_continue(s);
    s->expect_cont = false;
  }

  parse_params(req_content_type_str, req_content_type, params);

  if (req_content_type.compare("multipart/form-data") != 0) {
    err_msg = "Request Content-Type is not multipart/form-data";
    return -EINVAL;
  }

  if (s->cct->_conf->subsys.should_gather(ceph_subsys_rgw, 20)) {
    ldout(s->cct, 20) << "request content_type_str="
		      << req_content_type_str << dendl;
    ldout(s->cct, 20) << "request content_type params:" << dendl;
    map<string, string>::iterator iter;
    for (iter = params.begin(); iter != params.end(); ++iter) {
      ldout(s->cct, 20) << " " << iter->first << " -> " << iter->second
			<< dendl;
    }
  }

  ldout(s->cct, 20) << "adding bucket to policy env: " << s->bucket.name
		    << dendl;
  env.add_var("bucket", s->bucket.name);

  map<string, string>::iterator iter = params.find("boundary");
  if (iter == params.end()) {
    err_msg = "Missing multipart boundary specification";
    return -EINVAL;
  }

  // create the boundary
  boundary = "--";
  boundary.append(iter->second);

  bool done;
  do {
    struct post_form_part part;
    int r = read_form_part_header(&part, &done);
    if (r < 0)
      return r;

    if (s->cct->_conf->subsys.should_gather(ceph_subsys_rgw, 20)) {
      map<string, struct post_part_field, ltstr_nocase>::iterator piter;
      for (piter = part.fields.begin(); piter != part.fields.end(); ++piter) {
	ldout(s->cct, 20) << "read part header: name=" << part.name
			  << " content_type=" << part.content_type << dendl;
	ldout(s->cct, 20) << "name=" << piter->first << dendl;
	ldout(s->cct, 20) << "val=" << piter->second.val << dendl;
	ldout(s->cct, 20) << "params:" << dendl;
	map<string, string>& params = piter->second.params;
	for (iter = params.begin(); iter != params.end(); ++iter) {
	  ldout(s->cct, 20) << " " << iter->first << " -> " << iter->second
			    << dendl;
	}
      }
    }

    if (done) { /* unexpected here */
      err_msg = "Malformed request";
      return -EINVAL;
    }

    if (stringcasecmp(part.name, "file") == 0) { /* beginning of data transfer */
      struct post_part_field& field = part.fields["Content-Disposition"];
      map<string, string>::iterator iter = field.params.find("filename");
      if (iter != field.params.end()) {
	filename = iter->second;
      }
      parts[part.name] = part;
      data_pending = true;
      break;
    }

    bool boundary;
    uint64_t chunk_size = s->cct->_conf->rgw_max_chunk_size;
    r = read_data(part.data, chunk_size, &boundary, &done);
    if (!boundary) {
      err_msg = "Couldn't find boundary";
      return -EINVAL;
    }
    parts[part.name] = part;
    string part_str(part.data.c_str(), part.data.length());
    env.add_var(part.name, part_str);
  } while (!done);

  string object_str;
  if (!part_str("key", &object_str)) {
    err_msg = "Key not specified";
    return -EINVAL;
  }

  s->object = rgw_obj_key(object_str);

  rebuild_key(s->object.name);

  if (s->object.empty()) {
    err_msg = "Empty object name";
    return -EINVAL;
  }

  env.add_var("key", s->object.name);

  part_str("Content-Type", &content_type);
  env.add_var("Content-Type", content_type);

  map<string, struct post_form_part, ltstr_nocase>::iterator piter =
    parts.upper_bound(RGW_AMZ_META_PREFIX);
  for (; piter != parts.end(); ++piter) {
    string n = piter->first;
    if (strncasecmp(n.c_str(), RGW_AMZ_META_PREFIX,
		    sizeof(RGW_AMZ_META_PREFIX) - 1) != 0)
      break;

    string attr_name = RGW_ATTR_PREFIX;
    attr_name.append(n);

    /* need to null terminate it */
    bufferlist& data = piter->second.data;
    string str = string(data.c_str(), data.length());

    bufferlist attr_bl;
    attr_bl.append(str.c_str(), str.size() + 1);

    attrs[attr_name] = attr_bl;
  }
  // TODO: refactor this and the above loop to share code
  piter = parts.find(RGW_AMZ_WEBSITE_REDIRECT_LOCATION);
  if (piter != parts.end()) {
    string n = piter->first;
    string attr_name = RGW_ATTR_PREFIX;
    attr_name.append(n);
    /* need to null terminate it */
    bufferlist& data = piter->second.data;
    string str = string(data.c_str(), data.length());

    bufferlist attr_bl;
    attr_bl.append(str.c_str(), str.size() + 1);

    attrs[attr_name] = attr_bl;
  }

  int r = get_policy();
  if (r < 0)
    return r;

  min_len = post_policy.min_length;
  max_len = post_policy.max_length;

  return 0;
}

int RGWPostObj_ObjStore_S3::get_policy()
{
  bufferlist encoded_policy;

  if (part_bl("policy", &encoded_policy)) {

    // check that the signature matches the encoded policy
    string s3_access_key;
    if (!part_str("AWSAccessKeyId", &s3_access_key)) {
      ldout(s->cct, 0) << "No S3 access key found!" << dendl;
      err_msg = "Missing access key";
      return -EINVAL;
    }
    string received_signature_str;
    if (!part_str("signature", &received_signature_str)) {
      ldout(s->cct, 0) << "No signature found!" << dendl;
      err_msg = "Missing signature";
      return -EINVAL;
    }
    RGWUserInfo user_info;

    op_ret = rgw_get_user_info_by_access_key(store, s3_access_key, user_info);
    if (op_ret < 0) {
      S3AuthFactory aplfact(store, s->account_name);
      RGWGetPolicyV2Extractor extr(s3_access_key, received_signature_str);
      RGWLDAPAuthEngine ldap(s->cct, store, extr, &aplfact);
        // try external authenticators
      if (store->ctx()->_conf->rgw_s3_auth_use_keystone &&
	  store->ctx()->_conf->rgw_keystone_url.empty())
      {
	// keystone
	int external_auth_result = -EINVAL;
	dout(20) << "s3 keystone: trying keystone auth" << dendl;

	RGW_Auth_S3_Keystone_ValidateToken keystone_validator(store->ctx());
	external_auth_result =
	  keystone_validator.validate_s3token(s3_access_key,
					      string(encoded_policy.c_str(),
						    encoded_policy.length()),
					      received_signature_str);

	if (external_auth_result < 0) {
	  ldout(s->cct, 0) << "User lookup failed!" << dendl;
	  err_msg = "Bad access key / signature";
	  return -EACCES;
	}

	string project_id = keystone_validator.response.get_project_id();
	rgw_user uid(project_id);

	user_info.user_id = project_id;
	user_info.display_name = keystone_validator.response.get_project_name();

	/* try to store user if it not already exists */
	if (rgw_get_user_info_by_uid(store, uid, user_info) < 0) {
	  int ret = rgw_store_user_info(store, user_info, NULL, NULL, real_time(), true);
	  if (ret < 0) {
	    ldout(store->ctx(), 10)
	      << "NOTICE: failed to store new user's info: ret="
	      << ret << dendl;
	  }
	  s->perm_mask = RGW_PERM_FULL_CONTROL;
	}
  } else if (ldap.is_applicable()) {
    try {
      auto applier = ldap.authenticate();
      if (! applier) {
        return -EACCES;
      }

      try {
        applier->load_acct_info(*s->user);
        s->perm_mask = applier->get_perm_mask();
        applier->modify_request_state(s);
        s->auth_identity = std::move(applier);
      } catch (int err) {
        return -EACCES;
      }
    } catch (int err) {
      return -EACCES;
    }
      } else {
	return -EACCES;
      }
    } else {
      map<string, RGWAccessKey> access_keys  = user_info.access_keys;

      map<string, RGWAccessKey>::const_iterator iter =
	access_keys.find(s3_access_key);
      // We know the key must exist, since the user was returned by
      // rgw_get_user_info_by_access_key, but it doesn't hurt to check!
      if (iter == access_keys.end()) {
	ldout(s->cct, 0) << "Secret key lookup failed!" << dendl;
	err_msg = "No secret key for matching access key";
	return -EACCES;
      }
      string s3_secret_key = (iter->second).key;

      char expected_signature_char[CEPH_CRYPTO_HMACSHA1_DIGESTSIZE];

      calc_hmac_sha1(s3_secret_key.c_str(), s3_secret_key.size(),
		     encoded_policy.c_str(), encoded_policy.length(),
		     expected_signature_char);
      bufferlist expected_signature_hmac_raw;
      bufferlist expected_signature_hmac_encoded;
      expected_signature_hmac_raw.append(expected_signature_char,
					 CEPH_CRYPTO_HMACSHA1_DIGESTSIZE);
      expected_signature_hmac_raw.encode_base64(
	expected_signature_hmac_encoded);
      expected_signature_hmac_encoded.append((char)0); /* null terminate */

      if (received_signature_str.compare(
	    expected_signature_hmac_encoded.c_str()) != 0) {
	ldout(s->cct, 0) << "Signature verification failed!" << dendl;
	ldout(s->cct, 0) << "received: " << received_signature_str.c_str()
			 << dendl;
	ldout(s->cct, 0) << "expected: "
			 << expected_signature_hmac_encoded.c_str() << dendl;
	err_msg = "Bad access key / signature";
	return -EACCES;
      }
    }

    ldout(s->cct, 0) << "Successful Signature Verification!" << dendl;
    bufferlist decoded_policy;
    try {
      decoded_policy.decode_base64(encoded_policy);
    } catch (buffer::error& err) {
      ldout(s->cct, 0) << "failed to decode_base64 policy" << dendl;
      err_msg = "Could not decode policy";
      return -EINVAL;
    }

    decoded_policy.append('\0'); // NULL terminate

    ldout(s->cct, 0) << "POST policy: " << decoded_policy.c_str() << dendl;

    int r = post_policy.from_json(decoded_policy, err_msg);
    if (r < 0) {
      if (err_msg.empty()) {
	err_msg = "Failed to parse policy";
      }
      ldout(s->cct, 0) << "failed to parse policy" << dendl;
      return -EINVAL;
    }

    post_policy.set_var_checked("AWSAccessKeyId");
    post_policy.set_var_checked("policy");
    post_policy.set_var_checked("signature");

    r = post_policy.check(&env, err_msg);
    if (r < 0) {
      if (err_msg.empty()) {
	err_msg = "Policy check failed";
      }
      ldout(s->cct, 0) << "policy check failed" << dendl;
      return r;
    }

    // deep copy
    *(s->user) = user_info;
    s->owner.set_id(user_info.user_id);
    s->owner.set_name(user_info.display_name);

    /* FIXME: remove this after switching S3 to the new authentication
     * infrastructure. */
    s->auth_identity = rgw_auth_transform_old_authinfo(s);
  } else {
    ldout(s->cct, 0) << "No attached policy found!" << dendl;
  }

  string canned_acl;
  part_str("acl", &canned_acl);

  RGWAccessControlPolicy_S3 s3policy(s->cct);
  ldout(s->cct, 20) << "canned_acl=" << canned_acl << dendl;
  if (s3policy.create_canned(s->owner, s->bucket_owner, canned_acl) < 0) {
    err_msg = "Bad canned ACLs";
    return -EINVAL;
  }

  policy = s3policy;

  return 0;
}

int RGWPostObj_ObjStore_S3::complete_get_params()
{
  bool done;
  do {
    struct post_form_part part;
    int r = read_form_part_header(&part, &done);
    if (r < 0)
      return r;

    bufferlist part_data;
    bool boundary;
    uint64_t chunk_size = s->cct->_conf->rgw_max_chunk_size;
    r = read_data(part.data, chunk_size, &boundary, &done);
    if (!boundary) {
      return -EINVAL;
    }

    parts[part.name] = part;
  } while (!done);

  return 0;
}

int RGWPostObj_ObjStore_S3::get_data(bufferlist& bl)
{
  bool boundary;
  bool done;

  uint64_t chunk_size = s->cct->_conf->rgw_max_chunk_size;
  int r = read_data(bl, chunk_size, &boundary, &done);
  if (r < 0)
    return r;

  if (boundary) {
    data_pending = false;

    if (!done) {  /* reached end of data, let's drain the rest of the params */
      r = complete_get_params();
      if (r < 0)
	return r;
    }
  }

  return bl.length();
}

void RGWPostObj_ObjStore_S3::send_response()
{
  if (op_ret == 0 && parts.count("success_action_redirect")) {
    string redirect;

    part_str("success_action_redirect", &redirect);

    string tenant;
    string bucket;
    string key;
    string etag_str = "\"";

    etag_str.append(etag);
    etag_str.append("\"");

    string etag_url;

    url_encode(s->bucket_tenant, tenant); /* surely overkill, but cheap */
    url_encode(s->bucket_name, bucket);
    url_encode(s->object.name, key);
    url_encode(etag_str, etag_url);

    if (!s->bucket_tenant.empty()) {
      /*
       * What we really would like is to quaily the bucket name, so
       * that the client could simply copy it and paste into next request.
       * Unfortunately, in S3 we cannot know if the client will decide
       * to come through DNS, with "bucket.tenant" sytanx, or through
       * URL with "tenant\bucket" syntax. Therefore, we provide the
       * tenant separately.
       */
      redirect.append("?tenant=");
      redirect.append(tenant);
      redirect.append("&bucket=");
      redirect.append(bucket);
    } else {
      redirect.append("?bucket=");
      redirect.append(bucket);
    }
    redirect.append("&key=");
    redirect.append(key);
    redirect.append("&etag=");
    redirect.append(etag_url);

    int r = check_utf8(redirect.c_str(), redirect.size());
    if (r < 0) {
      op_ret = r;
      goto done;
    }
    dump_redirect(s, redirect);
    op_ret = STATUS_REDIRECT;
  } else if (op_ret == 0 && parts.count("success_action_status")) {
    string status_string;
    uint32_t status_int;

    part_str("success_action_status", &status_string);

    int r = stringtoul(status_string, &status_int);
    if (r < 0) {
      op_ret = r;
      goto done;
    }

    switch (status_int) {
      case 200:
	break;
      case 201:
	op_ret = STATUS_CREATED;
	break;
      default:
	op_ret = STATUS_NO_CONTENT;
	break;
    }
  } else if (! op_ret) {
    op_ret = STATUS_NO_CONTENT;
  }

done:
  if (op_ret == STATUS_CREATED) {
    s->formatter->open_object_section("PostResponse");
    if (g_conf->rgw_dns_name.length())
      s->formatter->dump_format("Location", "%s/%s",
				s->info.script_uri.c_str(),
				s->object.name.c_str());
    if (!s->bucket_tenant.empty())
      s->formatter->dump_string("Tenant", s->bucket_tenant);
    s->formatter->dump_string("Bucket", s->bucket_name);
    s->formatter->dump_string("Key", s->object.name);
    s->formatter->close_section();
  }
  s->err->message = err_msg;
  s->set_req_state_err(op_ret);
  dump_errno(s);
  if (op_ret >= 0) {
    dump_content_length(s, s->formatter->get_len());
  }
  end_header(s, this);
  if (op_ret != STATUS_CREATED)
    return;

  rgw_flush_formatter_and_reset(s, s->formatter);
}

int RGWDeleteObj_ObjStore_S3::get_params()
{
  const char *if_unmod = s->info.env->get("HTTP_X_AMZ_DELETE_IF_UNMODIFIED_SINCE");

  if (s->system_request) {
    s->info.args.get_bool(RGW_SYS_PARAM_PREFIX "no-precondition-error", &no_precondition_error, false);
  }

  if (if_unmod) {
    string if_unmod_str(if_unmod);
    string if_unmod_decoded;
    url_decode(if_unmod_str, if_unmod_decoded);
    uint64_t epoch;
    uint64_t nsec;
    if (utime_t::parse_date(if_unmod_decoded, &epoch, &nsec) < 0) {
      ldout(s->cct, 10) << "failed to parse time: " << if_unmod_decoded << dendl;
      return -EINVAL;
    }
    unmod_since = utime_t(epoch, nsec).to_real_time();
  }

  return 0;
}

void RGWDeleteObj_ObjStore_S3::send_response()
{
  int r = op_ret;
  if (r == -ENOENT)
    r = 0;
  if (!r)
    r = STATUS_NO_CONTENT;

  s->set_req_state_err(r);
  dump_errno(s);
  if (!version_id.empty()) {
    dump_header(s, "x-amz-version-id", version_id);
  }
  if (delete_marker) {
    dump_header(s, "x-amz-delete-marker", "true");
  }
  end_header(s, this);
}

int RGWCopyObj_ObjStore_S3::init_dest_policy()
{
  RGWAccessControlPolicy_S3 s3policy(s->cct);

  /* build a policy for the target object */
  int r = create_s3_policy(s, store, s3policy, s->owner);
  if (r < 0)
    return r;

  dest_policy = s3policy;

  return 0;
}

int RGWCopyObj_ObjStore_S3::get_params()
{
  if_mod = s->info.env->get("HTTP_X_AMZ_COPY_IF_MODIFIED_SINCE");
  if_unmod = s->info.env->get("HTTP_X_AMZ_COPY_IF_UNMODIFIED_SINCE");
  if_match = s->info.env->get("HTTP_X_AMZ_COPY_IF_MATCH");
  if_nomatch = s->info.env->get("HTTP_X_AMZ_COPY_IF_NONE_MATCH");

  src_tenant_name = s->src_tenant_name;
  src_bucket_name = s->src_bucket_name;
  src_object = s->src_object;
  dest_tenant_name = s->bucket.tenant;
  dest_bucket_name = s->bucket.name;
  dest_object = s->object.name;

  if (s->system_request) {
    source_zone = s->info.args.get(RGW_SYS_PARAM_PREFIX "source-zone");
    s->info.args.get_bool(RGW_SYS_PARAM_PREFIX "copy-if-newer", &copy_if_newer, false);
    if (!source_zone.empty()) {
      client_id = s->info.args.get(RGW_SYS_PARAM_PREFIX "client-id");
      op_id = s->info.args.get(RGW_SYS_PARAM_PREFIX "op-id");

      if (client_id.empty() || op_id.empty()) {
	ldout(s->cct, 0) <<
	  RGW_SYS_PARAM_PREFIX "client-id or "
	  RGW_SYS_PARAM_PREFIX "op-id were not provided, "
	  "required for intra-region copy"
			 << dendl;
	return -EINVAL;
      }
    }
  }

  const char *md_directive = s->info.env->get("HTTP_X_AMZ_METADATA_DIRECTIVE");
  if (md_directive) {
    if (strcasecmp(md_directive, "COPY") == 0) {
      attrs_mod = RGWRados::ATTRSMOD_NONE;
    } else if (strcasecmp(md_directive, "REPLACE") == 0) {
      attrs_mod = RGWRados::ATTRSMOD_REPLACE;
    } else if (!source_zone.empty()) {
      attrs_mod = RGWRados::ATTRSMOD_NONE; // default for intra-zone_group copy
    } else {
      ldout(s->cct, 0) << "invalid metadata directive" << dendl;
      return -EINVAL;
    }
  }

  if (source_zone.empty() &&
      (dest_tenant_name.compare(src_tenant_name) == 0) &&
      (dest_bucket_name.compare(src_bucket_name) == 0) &&
      (dest_object.compare(src_object.name) == 0) &&
      src_object.instance.empty() &&
      (attrs_mod != RGWRados::ATTRSMOD_REPLACE)) {
    /* can only copy object into itself if replacing attrs */
    ldout(s->cct, 0) << "can't copy object into itself if not replacing attrs"
		     << dendl;
    return -ERR_INVALID_REQUEST;
  }
  return 0;
}

void RGWCopyObj_ObjStore_S3::send_partial_response(off_t ofs)
{
  if (! sent_header) {
    if (op_ret)
    s->set_req_state_err(op_ret);
    dump_errno(s);

    end_header(s, this, "application/xml");
    if (op_ret == 0) {
      s->formatter->open_object_section_in_ns("CopyObjectResult", XMLNS_AWS_S3);
    }
    sent_header = true;
  } else {
    /* Send progress field. Note that this diverge from the original S3
     * spec. We do this in order to keep connection alive.
     */
    s->formatter->dump_int("Progress", (uint64_t)ofs);
  }
  rgw_flush_formatter(s, s->formatter);
}

void RGWCopyObj_ObjStore_S3::send_response()
{
  if (!sent_header)
    send_partial_response(0);

  if (op_ret == 0) {
    dump_time(s, "LastModified", &mtime);
    std::string etag_str = etag.to_str();
    if (! etag_str.empty()) {
      s->formatter->dump_string("ETag", std::move(etag_str));
    }
    s->formatter->close_section();
    rgw_flush_formatter_and_reset(s, s->formatter);
  }
}

void RGWGetACLs_ObjStore_S3::send_response()
{
  if (op_ret)
    s->set_req_state_err(op_ret);
  dump_errno(s);
  end_header(s, this, "application/xml");
  dump_start(s);
  rgw_flush_formatter(s, s->formatter);
  dump_body(s, acls);
}

int RGWPutACLs_ObjStore_S3::get_params()
{
  int ret =  RGWPutACLs_ObjStore::get_params();
  if (ret < 0)
    s->aws4_auth_needs_complete = false;
  if (s->aws4_auth_needs_complete) {
    int ret_auth = do_aws4_auth_completion();
    if (ret_auth < 0) {
      return ret_auth;
    }
  }
  return ret;
}

int RGWPutACLs_ObjStore_S3::get_policy_from_state(RGWRados *store,
						  struct req_state *s,
						  stringstream& ss)
{
  RGWAccessControlPolicy_S3 s3policy(s->cct);

  // bucket-* canned acls do not apply to bucket
  if (s->object.empty()) {
    if (s->canned_acl.find("bucket") != string::npos)
      s->canned_acl.clear();
  }

  int r = create_s3_policy(s, store, s3policy, owner);
  if (r < 0)
    return r;

  s3policy.to_xml(ss);

  return 0;
}

void RGWPutACLs_ObjStore_S3::send_response()
{
  if (op_ret)
    s->set_req_state_err(op_ret);
  dump_errno(s);
  end_header(s, this, "application/xml");
  dump_start(s);
}

void RGWGetLC_ObjStore_S3::execute()
{
  config.set_ctx(s->cct);

  map<string, bufferlist>::iterator aiter = s->bucket_attrs.find(RGW_ATTR_LC);
  if (aiter == s->bucket_attrs.end()) {
    op_ret = -ENOENT;
    return;
  }

  bufferlist::iterator iter(&aiter->second);
  try {
      config.decode(iter);
    } catch (const buffer::error& e) {
      ldout(s->cct, 0) << __func__ <<  "decode life cycle config failed" << dendl;
      op_ret = -EIO;
      return;
    }
}

void RGWGetLC_ObjStore_S3::send_response()
{
  if (op_ret) {
    if (op_ret == -ENOENT) {	
      s->set_req_state_err(ERR_NO_SUCH_LC);
    } else {
      s->set_req_state_err(op_ret);
    }
  }
  dump_errno(s);
  end_header(s, this, "application/xml");
  dump_start(s);

  config.dump_xml(s->formatter);
  rgw_flush_formatter_and_reset(s, s->formatter);
}

void RGWPutLC_ObjStore_S3::send_response()
{
  if (op_ret)
    s->set_req_state_err(op_ret);
  dump_errno(s);
  end_header(s, this, "application/xml");
  dump_start(s);
}

void RGWDeleteLC_ObjStore_S3::send_response()
{
  if (op_ret == 0)
      op_ret = STATUS_NO_CONTENT;
  if (op_ret) {   
    s->set_req_state_err(op_ret);
  }
  dump_errno(s);
  end_header(s, this, "application/xml");
  dump_start(s);
}

void RGWGetCORS_ObjStore_S3::send_response()
{
  if (op_ret) {
    if (op_ret == -ENOENT)
      s->set_req_state_err(ERR_NOT_FOUND);
    else
      s->set_req_state_err(op_ret);
  }
  dump_errno(s);
  end_header(s, NULL, "application/xml");
  dump_start(s);
  if (! op_ret) {
    string cors;
    RGWCORSConfiguration_S3 *s3cors =
      static_cast<RGWCORSConfiguration_S3 *>(&bucket_cors);
    stringstream ss;

    s3cors->to_xml(ss);
    cors = ss.str();
    dump_body(s, cors);
  }
}

int RGWPutCORS_ObjStore_S3::get_params()
{
  int r;
  char *data = NULL;
  int len = 0;
  size_t cl = 0;
  RGWCORSXMLParser_S3 parser(s->cct);
  RGWCORSConfiguration_S3 *cors_config;

  if (s->length)
    cl = atoll(s->length);
  if (cl) {
    data = (char *)malloc(cl + 1);
    if (!data) {
       r = -ENOMEM;
       goto done_err;
    }
    len = recv_body(s, data, cl);
    if (len < 0) {
      r = len;
      goto done_err;
    }
    data[len] = '\0';
  } else {
    len = 0;
  }

  if (s->aws4_auth_needs_complete) {
    int ret_auth = do_aws4_auth_completion();
    if (ret_auth < 0) {
      r = ret_auth;
      goto done_err;
    }
  }

  if (!parser.init()) {
    r = -EINVAL;
    goto done_err;
  }

  if (!data || !parser.parse(data, len, 1)) {
    r = -EINVAL;
    goto done_err;
  }
  cors_config =
    static_cast<RGWCORSConfiguration_S3 *>(parser.find_first(
					     "CORSConfiguration"));
  if (!cors_config) {
    r = -EINVAL;
    goto done_err;
  }

  if (s->cct->_conf->subsys.should_gather(ceph_subsys_rgw, 15)) {
    ldout(s->cct, 15) << "CORSConfiguration";
    cors_config->to_xml(*_dout);
    *_dout << dendl;
  }

  cors_config->encode(cors_bl);

  free(data);
  return 0;
done_err:
  free(data);
  return r;
}

void RGWPutCORS_ObjStore_S3::send_response()
{
  if (op_ret)
    s->set_req_state_err(op_ret);
  dump_errno(s);
  end_header(s, NULL, "application/xml");
  dump_start(s);
}

void RGWDeleteCORS_ObjStore_S3::send_response()
{
  int r = op_ret;
  if (!r || r == -ENOENT)
    r = STATUS_NO_CONTENT;

  s->set_req_state_err(r);
  dump_errno(s);
  end_header(s, NULL);
}

void RGWOptionsCORS_ObjStore_S3::send_response()
{
  string hdrs, exp_hdrs;
  uint32_t max_age = CORS_MAX_AGE_INVALID;
  /*EACCES means, there is no CORS registered yet for the bucket
   *ENOENT means, there is no match of the Origin in the list of CORSRule
   */
  if (op_ret == -ENOENT)
    op_ret = -EACCES;
  if (op_ret < 0) {
    s->set_req_state_err(op_ret);
    dump_errno(s);
    end_header(s, NULL);
    return;
  }
  get_response_params(hdrs, exp_hdrs, &max_age);

  dump_errno(s);
  dump_access_control(s, origin, req_meth, hdrs.c_str(), exp_hdrs.c_str(),
		      max_age);
  end_header(s, NULL);
}

void RGWGetRequestPayment_ObjStore_S3::send_response()
{
  dump_errno(s);
  end_header(s, this, "application/xml");
  dump_start(s);

  s->formatter->open_object_section_in_ns("RequestPaymentConfiguration", XMLNS_AWS_S3);
  const char *payer = requester_pays ? "Requester" :  "BucketOwner";
  s->formatter->dump_string("Payer", payer);
  s->formatter->close_section();
  rgw_flush_formatter_and_reset(s, s->formatter);
}

class RGWSetRequestPaymentParser : public RGWXMLParser
{
  XMLObj *alloc_obj(const char *el) {
    return new XMLObj;
  }

public:
  RGWSetRequestPaymentParser() {}
  ~RGWSetRequestPaymentParser() {}

  int get_request_payment_payer(bool *requester_pays) {
    XMLObj *config = find_first("RequestPaymentConfiguration");
    if (!config)
      return -EINVAL;

    *requester_pays = false;

    XMLObj *field = config->find_first("Payer");
    if (!field)
      return 0;

    string& s = field->get_data();

    if (stringcasecmp(s, "Requester") == 0) {
      *requester_pays = true;
    } else if (stringcasecmp(s, "BucketOwner") != 0) {
      return -EINVAL;
    }

    return 0;
  }
};

int RGWSetRequestPayment_ObjStore_S3::get_params()
{
#define GET_REQUEST_PAYMENT_BUF_MAX (128 * 1024)
  char *data;
  int len = 0;
  int r = rgw_rest_read_all_input(s, &data, &len, GET_REQUEST_PAYMENT_BUF_MAX);
  if (r < 0) {
    return r;
  }

  RGWSetRequestPaymentParser parser;

  if (!parser.init()) {
    ldout(s->cct, 0) << "ERROR: failed to initialize parser" << dendl;
    r = -EIO;
    goto done;
  }

  if (!parser.parse(data, len, 1)) {
    ldout(s->cct, 10) << "failed to parse data: " << data << dendl;
    r = -EINVAL;
    goto done;
  }

  r = parser.get_request_payment_payer(&requester_pays);

done:
  free(data);

  return r;
}

void RGWSetRequestPayment_ObjStore_S3::send_response()
{
  if (op_ret)
    s->set_req_state_err(op_ret);
  dump_errno(s);
  end_header(s);
}

int RGWInitMultipart_ObjStore_S3::get_params()
{
  RGWAccessControlPolicy_S3 s3policy(s->cct);
  op_ret = create_s3_policy(s, store, s3policy, s->owner);
  if (op_ret < 0)
    return op_ret;

  policy = s3policy;

  return 0;
}

void RGWInitMultipart_ObjStore_S3::send_response()
{
  if (op_ret)
    s->set_req_state_err(op_ret);
  dump_errno(s);
  end_header(s, this, "application/xml");
  if (op_ret == 0) {
    dump_start(s);
    s->formatter->open_object_section_in_ns("InitiateMultipartUploadResult", XMLNS_AWS_S3);
    if (!s->bucket_tenant.empty())
      s->formatter->dump_string("Tenant", s->bucket_tenant);
    s->formatter->dump_string("Bucket", s->bucket_name);
    s->formatter->dump_string("Key", s->object.name);
    s->formatter->dump_string("UploadId", upload_id);
    s->formatter->close_section();
    rgw_flush_formatter_and_reset(s, s->formatter);
  }
}

int RGWCompleteMultipart_ObjStore_S3::get_params()
{
  int ret = RGWCompleteMultipart_ObjStore::get_params();
  if (ret < 0) {
    return ret;
  }

  if (s->aws4_auth_needs_complete) {
    int ret_auth = do_aws4_auth_completion();
    if (ret_auth < 0) {
      return ret_auth;
    }
  }
  return 0;
}

void RGWCompleteMultipart_ObjStore_S3::send_response()
{
  if (op_ret)
    s->set_req_state_err(op_ret);
  dump_errno(s);
  end_header(s, this, "application/xml");
  if (op_ret == 0) { 
    dump_start(s);
    s->formatter->open_object_section_in_ns("CompleteMultipartUploadResult", XMLNS_AWS_S3);
    if (!s->bucket_tenant.empty()) {
      if (s->info.domain.length()) {
        s->formatter->dump_format("Location", "%s.%s.%s",
          s->bucket_name.c_str(),
          s->bucket_tenant.c_str(),
          s->info.domain.c_str());
      }
      s->formatter->dump_string("Tenant", s->bucket_tenant);
    } else {
      if (s->info.domain.length()) {
        s->formatter->dump_format("Location", "%s.%s",
          s->bucket_name.c_str(),
          s->info.domain.c_str());
      }
    }
    s->formatter->dump_string("Bucket", s->bucket_name);
    s->formatter->dump_string("Key", s->object.name);
    s->formatter->dump_string("ETag", etag);
    s->formatter->close_section();
    rgw_flush_formatter_and_reset(s, s->formatter);
  }
}

void RGWAbortMultipart_ObjStore_S3::send_response()
{
  int r = op_ret;
  if (!r)
    r = STATUS_NO_CONTENT;

  s->set_req_state_err(r);
  dump_errno(s);
  end_header(s, this);
}

void RGWListMultipart_ObjStore_S3::send_response()
{
  if (op_ret)
    s->set_req_state_err(op_ret);
  dump_errno(s);
  end_header(s, this, "application/xml");

  if (op_ret == 0) {
    dump_start(s);
    s->formatter->open_object_section_in_ns("ListPartsResult", XMLNS_AWS_S3);
    map<uint32_t, RGWUploadPartInfo>::iterator iter;
    map<uint32_t, RGWUploadPartInfo>::reverse_iterator test_iter;
    int cur_max = 0;

    iter = parts.begin();
    test_iter = parts.rbegin();
    if (test_iter != parts.rend()) {
      cur_max = test_iter->first;
    }
    if (!s->bucket_tenant.empty())
      s->formatter->dump_string("Tenant", s->bucket_tenant);
    s->formatter->dump_string("Bucket", s->bucket_name);
    s->formatter->dump_string("Key", s->object.name);
    s->formatter->dump_string("UploadId", upload_id);
    s->formatter->dump_string("StorageClass", "STANDARD");
    s->formatter->dump_int("PartNumberMarker", marker);
    s->formatter->dump_int("NextPartNumberMarker", cur_max);
    s->formatter->dump_int("MaxParts", max_parts);
    s->formatter->dump_string("IsTruncated", (truncated ? "true" : "false"));

    ACLOwner& owner = policy.get_owner();
    dump_owner(s, owner.get_id(), owner.get_display_name());

    for (; iter != parts.end(); ++iter) {
      RGWUploadPartInfo& info = iter->second;

      s->formatter->open_object_section("Part");

      dump_time(s, "LastModified", &info.modified);

      s->formatter->dump_unsigned("PartNumber", info.num);
      s->formatter->dump_format("ETag", "\"%s\"", info.etag.c_str());
      s->formatter->dump_unsigned("Size", info.accounted_size);
      s->formatter->close_section();
    }
    s->formatter->close_section();
    rgw_flush_formatter_and_reset(s, s->formatter);
  }
}

void RGWListBucketMultiparts_ObjStore_S3::send_response()
{
  if (op_ret < 0)
    s->set_req_state_err(op_ret);
  dump_errno(s);

  end_header(s, this, "application/xml");
  dump_start(s);
  if (op_ret < 0)
    return;

  s->formatter->open_object_section_in_ns("ListMultipartUploadsResult", XMLNS_AWS_S3);
  if (!s->bucket_tenant.empty())
    s->formatter->dump_string("Tenant", s->bucket_tenant);
  s->formatter->dump_string("Bucket", s->bucket_name);
  if (!prefix.empty())
    s->formatter->dump_string("ListMultipartUploadsResult.Prefix", prefix);
  string& key_marker = marker.get_key();
  if (!key_marker.empty())
    s->formatter->dump_string("KeyMarker", key_marker);
  string& upload_id_marker = marker.get_upload_id();
  if (!upload_id_marker.empty())
    s->formatter->dump_string("UploadIdMarker", upload_id_marker);
  string next_key = next_marker.mp.get_key();
  if (!next_key.empty())
    s->formatter->dump_string("NextKeyMarker", next_key);
  string next_upload_id = next_marker.mp.get_upload_id();
  if (!next_upload_id.empty())
    s->formatter->dump_string("NextUploadIdMarker", next_upload_id);
  s->formatter->dump_int("MaxUploads", max_uploads);
  if (!delimiter.empty())
    s->formatter->dump_string("Delimiter", delimiter);
  s->formatter->dump_string("IsTruncated", (is_truncated ? "true" : "false"));

  if (op_ret >= 0) {
    vector<RGWMultipartUploadEntry>::iterator iter;
    for (iter = uploads.begin(); iter != uploads.end(); ++iter) {
      RGWMPObj& mp = iter->mp;
      s->formatter->open_array_section("Upload");
      s->formatter->dump_string("Key", mp.get_key());
      s->formatter->dump_string("UploadId", mp.get_upload_id());
      dump_owner(s, s->user->user_id, s->user->display_name, "Initiator");
      dump_owner(s, s->user->user_id, s->user->display_name);
      s->formatter->dump_string("StorageClass", "STANDARD");
      dump_time(s, "Initiated", &iter->obj.mtime);
      s->formatter->close_section();
    }
    if (!common_prefixes.empty()) {
      s->formatter->open_array_section("CommonPrefixes");
      map<string, bool>::iterator pref_iter;
      for (pref_iter = common_prefixes.begin();
	   pref_iter != common_prefixes.end(); ++pref_iter) {
	s->formatter->dump_string("CommonPrefixes.Prefix", pref_iter->first);
      }
      s->formatter->close_section();
    }
  }
  s->formatter->close_section();
  rgw_flush_formatter_and_reset(s, s->formatter);
}

int RGWDeleteMultiObj_ObjStore_S3::get_params()
{
  int ret = RGWDeleteMultiObj_ObjStore::get_params();
  if (ret < 0) {
    return ret;
  }

  if (s->aws4_auth_needs_complete) {
    int ret_auth = do_aws4_auth_completion();
    if (ret_auth < 0) {
      return ret_auth;
    }
  }
  return 0;
}

void RGWDeleteMultiObj_ObjStore_S3::send_status()
{
  if (! status_dumped) {
    if (op_ret < 0)
      s->set_req_state_err(op_ret);
    dump_errno(s);
    status_dumped = true;
  }
}

void RGWDeleteMultiObj_ObjStore_S3::begin_response()
{

  if (!status_dumped) {
    send_status();
  }

  dump_start(s);
  end_header(s, this, "application/xml");
  s->formatter->open_object_section_in_ns("DeleteResult", XMLNS_AWS_S3);

  rgw_flush_formatter(s, s->formatter);
}

void RGWDeleteMultiObj_ObjStore_S3::send_partial_response(rgw_obj_key& key,
							  bool delete_marker,
							  const string& marker_version_id, int ret)
{
  if (!key.empty()) {
    if (op_ret == 0 && !quiet) {
      s->formatter->open_object_section("Deleted");
      s->formatter->dump_string("Key", key.name);
      if (!key.instance.empty()) {
	s->formatter->dump_string("VersionId", key.instance);
      }
      if (delete_marker) {
	s->formatter->dump_bool("DeleteMarker", true);
	s->formatter->dump_string("DeleteMarkerVersionId", marker_version_id);
      }
      s->formatter->close_section();
    } else if (op_ret < 0) {
      struct rgw_http_error r;
      int err_no;

      s->formatter->open_object_section("Error");

      err_no = -op_ret;
      rgw_get_errno_s3(&r, err_no);

      s->formatter->dump_string("Key", key.name);
      s->formatter->dump_string("VersionId", key.instance);
      s->formatter->dump_int("Code", r.http_ret);
      s->formatter->dump_string("Message", r.s3_code);
      s->formatter->close_section();
    }

    rgw_flush_formatter(s, s->formatter);
  }
}

void RGWDeleteMultiObj_ObjStore_S3::end_response()
{

  s->formatter->close_section();
  rgw_flush_formatter_and_reset(s, s->formatter);
}

RGWOp *RGWHandler_REST_Service_S3::op_get()
{
  if (is_usage_op()) {
    return new RGWGetUsage_ObjStore_S3;
  } else {
    return new RGWListBuckets_ObjStore_S3;
  }
}

RGWOp *RGWHandler_REST_Service_S3::op_head()
{
  return new RGWListBuckets_ObjStore_S3;
}

RGWOp *RGWHandler_REST_Bucket_S3::get_obj_op(bool get_data)
{
  // Non-website mode
  if (get_data)
    return new RGWListBucket_ObjStore_S3;
  else
    return new RGWStatBucket_ObjStore_S3;
}

RGWOp *RGWHandler_REST_Bucket_S3::op_get()
{
  if (s->info.args.sub_resource_exists("logging"))
    return new RGWGetBucketLogging_ObjStore_S3;

  if (s->info.args.sub_resource_exists("location"))
    return new RGWGetBucketLocation_ObjStore_S3;

  if (s->info.args.sub_resource_exists("versioning"))
    return new RGWGetBucketVersioning_ObjStore_S3;

  if (s->info.args.sub_resource_exists("website")) {
    if (!s->cct->_conf->rgw_enable_static_website) {
      return NULL;
    }
    return new RGWGetBucketWebsite_ObjStore_S3;
  }

  if (is_acl_op()) {
    return new RGWGetACLs_ObjStore_S3;
  } else if (is_cors_op()) {
    return new RGWGetCORS_ObjStore_S3;
  } else if (is_request_payment_op()) {
    return new RGWGetRequestPayment_ObjStore_S3;
  } else if (s->info.args.exists("uploads")) {
    return new RGWListBucketMultiparts_ObjStore_S3;
  } else if(is_lc_op()) {
    return new RGWGetLC_ObjStore_S3;
  }
  return get_obj_op(true);
}

RGWOp *RGWHandler_REST_Bucket_S3::op_head()
{
  if (is_acl_op()) {
    return new RGWGetACLs_ObjStore_S3;
  } else if (s->info.args.exists("uploads")) {
    return new RGWListBucketMultiparts_ObjStore_S3;
  }
  return get_obj_op(false);
}

RGWOp *RGWHandler_REST_Bucket_S3::op_put()
{
  if (s->info.args.sub_resource_exists("logging"))
    return NULL;
  if (s->info.args.sub_resource_exists("versioning"))
    return new RGWSetBucketVersioning_ObjStore_S3;
  if (s->info.args.sub_resource_exists("website")) {
    if (!s->cct->_conf->rgw_enable_static_website) {
      return NULL;
    }
    return new RGWSetBucketWebsite_ObjStore_S3;
  }
  if (is_acl_op()) {
    return new RGWPutACLs_ObjStore_S3;
  } else if (is_cors_op()) {
    return new RGWPutCORS_ObjStore_S3;
  } else if (is_request_payment_op()) {
    return new RGWSetRequestPayment_ObjStore_S3;
  } else if(is_lc_op()) {
    return new RGWPutLC_ObjStore_S3;
  }
  return new RGWCreateBucket_ObjStore_S3;
}

RGWOp *RGWHandler_REST_Bucket_S3::op_delete()
{
  if (is_cors_op()) {
    return new RGWDeleteCORS_ObjStore_S3;
  } else if(is_lc_op()) {
    return new RGWDeleteLC_ObjStore_S3;
  }

  if (s->info.args.sub_resource_exists("website")) {
    if (!s->cct->_conf->rgw_enable_static_website) {
      return NULL;
    }
    return new RGWDeleteBucketWebsite_ObjStore_S3;
  }

  return new RGWDeleteBucket_ObjStore_S3;
}

RGWOp *RGWHandler_REST_Bucket_S3::op_post()
{
  if (s->info.args.exists("delete")) {
    return new RGWDeleteMultiObj_ObjStore_S3;
  }

  return new RGWPostObj_ObjStore_S3;
}

RGWOp *RGWHandler_REST_Bucket_S3::op_options()
{
  return new RGWOptionsCORS_ObjStore_S3;
}

RGWOp *RGWHandler_REST_Obj_S3::get_obj_op(bool get_data)
{
  if (is_acl_op()) {
    return new RGWGetACLs_ObjStore_S3;
  }
  RGWGetObj_ObjStore_S3 *get_obj_op = new RGWGetObj_ObjStore_S3;
  get_obj_op->set_get_data(get_data);
  return get_obj_op;
}

RGWOp *RGWHandler_REST_Obj_S3::op_get()
{
  if (is_acl_op()) {
    return new RGWGetACLs_ObjStore_S3;
  } else if (s->info.args.exists("uploadId")) {
    return new RGWListMultipart_ObjStore_S3;
  }
  return get_obj_op(true);
}

RGWOp *RGWHandler_REST_Obj_S3::op_head()
{
  if (is_acl_op()) {
    return new RGWGetACLs_ObjStore_S3;
  } else if (s->info.args.exists("uploadId")) {
    return new RGWListMultipart_ObjStore_S3;
  }
  return get_obj_op(false);
}

RGWOp *RGWHandler_REST_Obj_S3::op_put()
{
  if (is_acl_op()) {
    return new RGWPutACLs_ObjStore_S3;
  }
  if (s->init_state.src_bucket.empty())
    return new RGWPutObj_ObjStore_S3;
  else
    return new RGWCopyObj_ObjStore_S3;
}

RGWOp *RGWHandler_REST_Obj_S3::op_delete()
{
  string upload_id = s->info.args.get("uploadId");

  if (upload_id.empty())
    return new RGWDeleteObj_ObjStore_S3;
  else
    return new RGWAbortMultipart_ObjStore_S3;
}

RGWOp *RGWHandler_REST_Obj_S3::op_post()
{
  if (s->info.args.exists("uploadId"))
    return new RGWCompleteMultipart_ObjStore_S3;

  if (s->info.args.exists("uploads"))
    return new RGWInitMultipart_ObjStore_S3;

  return NULL;
}

RGWOp *RGWHandler_REST_Obj_S3::op_options()
{
  return new RGWOptionsCORS_ObjStore_S3;
}

int RGWHandler_REST_S3::init_from_header(struct req_state* s,
					int default_formatter,
					bool configurable_format)
{
  string req;
  string first;

  const char *req_name = s->relative_uri.c_str();
  const char *p;

  if (*req_name == '?') {
    p = req_name;
  } else {
    p = s->info.request_params.c_str();
  }

  s->info.args.set(p);
  s->info.args.parse();

  /* must be called after the args parsing */
  int ret = allocate_formatter(s, default_formatter, configurable_format);
  if (ret < 0)
    return ret;

  if (*req_name != '/')
    return 0;

  req_name++;

  if (!*req_name)
    return 0;

  req = req_name;
  int pos = req.find('/');
  if (pos >= 0) {
    first = req.substr(0, pos);
  } else {
    first = req;
  }

  /*
   * XXX The intent of the check for empty is apparently to let the bucket
   * name from DNS to be set ahead. However, we currently take the DNS
   * bucket and re-insert it into URL in rgw_rest.cc:RGWREST::preprocess().
   * So, this check is meaningless.
   *
   * Rather than dropping this, the code needs to be changed into putting
   * the bucket (and its tenant) from DNS and Host: header (HTTP_HOST)
   * into req_status.bucket_name directly.
   */
  if (s->init_state.url_bucket.empty()) {
    // Save bucket to tide us over until token is parsed.
    s->init_state.url_bucket = first;
    if (pos >= 0) {
      string encoded_obj_str = req.substr(pos+1);
      s->object = rgw_obj_key(encoded_obj_str, s->info.args.get("versionId"));
    }
  } else {
    s->object = rgw_obj_key(req_name, s->info.args.get("versionId"));
  }
  return 0;
}

int RGWHandler_REST_S3::postauth_init()
{
  struct req_init_state *t = &s->init_state;
  bool relaxed_names = s->cct->_conf->rgw_relaxed_s3_bucket_names;

  rgw_parse_url_bucket(t->url_bucket, s->user->user_id.tenant,
		      s->bucket_tenant, s->bucket_name);

  dout(10) << "s->object=" << (!s->object.empty() ? s->object : rgw_obj_key("<NULL>"))
           << " s->bucket=" << rgw_make_bucket_entry_name(s->bucket_tenant, s->bucket_name) << dendl;

  int ret;
  ret = validate_tenant_name(s->bucket_tenant);
  if (ret)
    return ret;
  if (!s->bucket_name.empty()) {
    ret = valid_s3_bucket_name(s->bucket_name, relaxed_names);
    if (ret)
      return ret;
    ret = validate_object_name(s->object.name);
    if (ret)
      return ret;
  }

  if (!t->src_bucket.empty()) {
    rgw_parse_url_bucket(t->src_bucket, s->user->user_id.tenant,
			s->src_tenant_name, s->src_bucket_name);
    ret = validate_tenant_name(s->src_tenant_name);
    if (ret)
      return ret;
    ret = valid_s3_bucket_name(s->src_bucket_name, relaxed_names);
    if (ret)
      return ret;
  }
  return 0;
}

int RGWHandler_REST_S3::init(RGWRados *store, struct req_state *s,
                             rgw::io::BasicClient *cio)
{
  int ret;

  s->dialect = "s3";
  
  ret = validate_tenant_name(s->bucket_tenant);
  if (ret)
    return ret;
  bool relaxed_names = s->cct->_conf->rgw_relaxed_s3_bucket_names;
  if (!s->bucket_name.empty()) {
    ret = valid_s3_bucket_name(s->bucket_name, relaxed_names);
    if (ret)
      return ret;
    ret = validate_object_name(s->object.name);
    if (ret)
      return ret;
  }

  const char *cacl = s->info.env->get("HTTP_X_AMZ_ACL");
  if (cacl)
    s->canned_acl = cacl;

  s->has_acl_header = s->info.env->exists_prefix("HTTP_X_AMZ_GRANT");

  const char *copy_source = s->info.env->get("HTTP_X_AMZ_COPY_SOURCE");

  if (copy_source && !s->info.env->get("HTTP_X_AMZ_COPY_SOURCE_RANGE")) {
    ret = RGWCopyObj::parse_copy_location(copy_source,
                                          s->init_state.src_bucket,
                                          s->src_object);
    if (!ret) {
      ldout(s->cct, 0) << "failed to parse copy location" << dendl;
      return -EINVAL; // XXX why not -ERR_INVALID_BUCKET_NAME or -ERR_BAD_URL?
    }
  }

  return RGWHandler_REST::init(store, s, cio);
}

/*
 * Try to validate S3 auth against keystone s3token interface
 */
int RGW_Auth_S3_Keystone_ValidateToken::validate_s3token(
  const string& auth_id, const string& auth_token, const string& auth_sign) {
  /* prepare keystone url */
  string keystone_url = cct->_conf->rgw_keystone_url;
  if (keystone_url[keystone_url.size() - 1] != '/') {
    keystone_url.append("/");
  }

  if (KeystoneService::get_api_version() == KeystoneApiVersion::VER_3) {
    keystone_url.append("v3/s3tokens");
  } else {
    keystone_url.append("v2.0/s3tokens");
  }

  /* get authentication token for Keystone. */
  string admin_token_id;
  int r = KeystoneService::get_keystone_admin_token(cct, admin_token_id);
  if (r < 0) {
    ldout(cct, 2) << "s3 keystone: cannot get token for keystone access" << dendl;
    return r;
  }

  /* set required headers for keystone request */
  append_header("X-Auth-Token", admin_token_id);
  append_header("Content-Type", "application/json");

  /* check if we want to verify keystone's ssl certs */
  set_verify_ssl(cct->_conf->rgw_keystone_verify_ssl);

  /* encode token */
  bufferlist token_buff;
  bufferlist token_encoded;
  token_buff.append(auth_token);
  token_buff.encode_base64(token_encoded);
  token_encoded.append((char)0);

  /* create json credentials request body */
  JSONFormatter credentials(false);
  credentials.open_object_section("");
  credentials.open_object_section("credentials");
  credentials.dump_string("access", auth_id);
  credentials.dump_string("token", token_encoded.c_str());
  credentials.dump_string("signature", auth_sign);
  credentials.close_section();
  credentials.close_section();

  std::stringstream os;
  credentials.flush(os);
  set_tx_buffer(os.str());

  /* send request */
  int ret = process("POST", keystone_url.c_str());
  if (ret < 0) {
    dout(2) << "s3 keystone: token validation ERROR: " << rx_buffer.c_str()
            << dendl;
    return -EPERM;
  }

  /* if the supplied signature is wrong, we will get 401 from Keystone */
  if (get_http_status() == HTTP_STATUS_UNAUTHORIZED) {
    return -ERR_SIGNATURE_NO_MATCH;
  }

  /* now parse response */
  if (response.parse(cct, string(), rx_buffer) < 0) {
    dout(2) << "s3 keystone: token parsing failed" << dendl;
    return -EPERM;
  }

  /* check if we have a valid role */
  bool found = false;
  for (const auto& role : accepted_roles) {
    if (response.has_role(role) == true) {
      found = true;
      break;
    }
  }

  if (!found) {
    ldout(cct, 5) << "s3 keystone: user does not hold a matching role;"
                     " required roles: "
                  << cct->_conf->rgw_keystone_accepted_roles << dendl;
    return -ERR_INVALID_ACCESS_KEY;
  }

  /* everything seems fine, continue with this user */
  ldout(cct, 5) << "s3 keystone: validated token: " << response.get_project_name()
                << ":" << response.get_user_name()
                << " expires: " << response.get_expires() << dendl;
  return 0;
}

static void init_anon_user(struct req_state *s)
{
  rgw_get_anon_user(*(s->user));
  s->perm_mask = RGW_PERM_FULL_CONTROL;
}

/*
 * verify that a signed request comes from the keyholder
 * by checking the signature against our locally-computed version
 *
 * it tries AWS v4 before AWS v2
 */
int RGW_Auth_S3::authorize(RGWRados *store, struct req_state *s)
{

  /* neither keystone and rados enabled; warn and exit! */
  if (!store->ctx()->_conf->rgw_s3_auth_use_rados &&
      !store->ctx()->_conf->rgw_s3_auth_use_keystone &&
      !store->ctx()->_conf->rgw_s3_auth_use_ldap) {
    dout(0) << "WARNING: no authorization backend enabled! Users will never authenticate." << dendl;
    return -EPERM;
  }

  if (s->op == OP_OPTIONS) {
    init_anon_user(s);
    return 0;
  }

  if (!s->http_auth || !(*s->http_auth)) {

    /* AWS4 */

    string algorithm = s->info.args.get("X-Amz-Algorithm");
    if (algorithm.size()) {
      if (algorithm != "AWS4-HMAC-SHA256") {
        return -EPERM;
      }
      return authorize_v4(store, s);
    }

    /* AWS2 */

    string auth_id = s->info.args.get("AWSAccessKeyId");
    if (auth_id.size()) {
      return authorize_v2(store, s);
    }

    /* anonymous access */

    init_anon_user(s);
    return 0;

  } else {

    /* AWS4 */

    if (!strncmp(s->http_auth, "AWS4-HMAC-SHA256", 16)) {
      return authorize_v4(store, s);
    }

    /* AWS2 */

    if (!strncmp(s->http_auth, "AWS ", 4)) {
      return authorize_v2(store, s);
    }

  }

  return -EINVAL;
}

int RGW_Auth_S3::authorize_aws4_auth_complete(RGWRados *store, struct req_state *s)
{
  return authorize_v4_complete(store, s, "", false);
}

int RGW_Auth_S3::authorize_v4_complete(RGWRados *store, struct req_state *s, const string& request_payload, bool unsigned_payload)
{
  size_t pos;

  /* craft canonical request */

  string canonical_req;
  string canonical_req_hash;

  rgw_create_s3_v4_canonical_request(s, s->aws4_auth->canonical_uri, s->aws4_auth->canonical_qs,
      s->aws4_auth->canonical_hdrs, s->aws4_auth->signed_hdrs, request_payload, unsigned_payload,
      canonical_req, canonical_req_hash);

  /* Validate x-amz-sha256 */

  if (s->aws4_auth_needs_complete) {
    const char *expected_request_payload_hash = s->info.env->get("HTTP_X_AMZ_CONTENT_SHA256");
    if (expected_request_payload_hash &&
        s->aws4_auth->payload_hash.compare(expected_request_payload_hash) != 0) {
      ldout(s->cct, 10) << "ERROR: x-amz-content-sha256 does not match" << dendl;
      return -ERR_AMZ_CONTENT_SHA256_MISMATCH;
    }
  }

  /*
   * create a string to sign
   *
   * http://docs.aws.amazon.com/general/latest/gr/sigv4-create-string-to-sign.html
   */

  string string_to_sign;

  rgw_create_s3_v4_string_to_sign(s->cct, "AWS4-HMAC-SHA256", s->aws4_auth->date, s->aws4_auth->credential_scope,
      canonical_req_hash, string_to_sign);

  /*
   * calculate the AWS signature
   *
   * http://docs.aws.amazon.com/general/latest/gr/sigv4-calculate-signature.html
   */

  string cs_aux = s->aws4_auth->credential_scope;

  string date_cs = cs_aux;
  pos = date_cs.find("/");
  date_cs = date_cs.substr(0, pos);
  cs_aux = cs_aux.substr(pos + 1, cs_aux.length());

  string region_cs = cs_aux;
  pos = region_cs.find("/");
  region_cs = region_cs.substr(0, pos);
  cs_aux = cs_aux.substr(pos + 1, cs_aux.length());

  string service_cs = cs_aux;
  pos = service_cs.find("/");
  service_cs = service_cs.substr(0, pos);

  int err = rgw_calculate_s3_v4_aws_signature(s, s->aws4_auth->access_key_id, date_cs,
      region_cs, service_cs, string_to_sign, s->aws4_auth->new_signature);

  ldout(s->cct, 10) << "----------------------------- Verifying signatures" << dendl;
  ldout(s->cct, 10) << "Signature     = " << s->aws4_auth->signature << dendl;
  ldout(s->cct, 10) << "New Signature = " << s->aws4_auth->new_signature << dendl;
  ldout(s->cct, 10) << "-----------------------------" << dendl;

  if (err) {
    return err;
  }

  s->aws4_auth->seed_signature = s->aws4_auth->new_signature;

  return 0;

}

static inline bool is_base64_for_content_md5(unsigned char c) {
  return (isalnum(c) || isspace(c) || (c == '+') || (c == '/') || (c == '='));
}

static bool char_needs_aws4_escaping(char c)
{
  if ((c >= 'a' && c <= 'z') ||
      (c >= 'A' && c <= 'Z') ||
      (c >= '0' && c <= '9')) {
    return false;
  }

  switch (c) {
    case '-':
    case '_':
    case '.':
    case '~':
      return false;
  }
  return true;
}

static void aws4_uri_encode(const string& src, string& dst)
{
  const char *p = src.c_str();
  for (unsigned i = 0; i < src.size(); i++, p++) {
    if (char_needs_aws4_escaping(*p)) {
      rgw_uri_escape_char(*p, dst);
      continue;
    }

    dst.append(p, 1);
  }
}

static std::array<string, 3> aws4_presigned_required_keys = { "Credential", "SignedHeaders", "Signature" };

/*
 * handle v4 signatures (rados auth only)
 */
int RGW_Auth_S3::authorize_v4(RGWRados *store, struct req_state *s)
{
  string::size_type pos;
  bool using_qs;

  uint64_t now_req = 0;
  uint64_t now = ceph_clock_now(s->cct);

  /* v4 requires rados auth */
  if (!store->ctx()->_conf->rgw_s3_auth_use_rados) {
    return -EPERM;
  }

  string algorithm = "AWS4-HMAC-SHA256";

  try {
    s->aws4_auth = std::unique_ptr<rgw_aws4_auth>(new rgw_aws4_auth);
  } catch (std::bad_alloc&) {
    return -ENOMEM;
  }

  if ((!s->http_auth) || !(*s->http_auth)) {

    /* auth ships with req params ... */

    /* look for required params */

    using_qs = true;
    s->aws4_auth->credential = s->info.args.get("X-Amz-Credential");
    if (s->aws4_auth->credential.size() == 0) {
      return -EPERM;
    }

    s->aws4_auth->date = s->info.args.get("X-Amz-Date");
    struct tm date_t;
    if (!parse_iso8601(s->aws4_auth->date.c_str(), &date_t, NULL, false))
      return -EPERM;

    s->aws4_auth->expires = s->info.args.get("X-Amz-Expires");
    if (s->aws4_auth->expires.size() != 0) {
      /* X-Amz-Expires provides the time period, in seconds, for which
         the generated presigned URL is valid. The minimum value
         you can set is 1, and the maximum is 604800 (seven days) */
      time_t exp = atoll(s->aws4_auth->expires.c_str());
      if ((exp < 1) || (exp > 604800)) {
        dout(10) << "NOTICE: exp out of range, exp = " << exp << dendl;
        return -EPERM;
      }
      /* handle expiration in epoch time */
      now_req = (uint64_t)timegm(&date_t);
      if (now >= now_req + exp) {
        dout(10) << "NOTICE: now = " << now << ", now_req = " << now_req << ", exp = " << exp << dendl;
        return -EPERM;
      }
    }

    if ( (now_req < now - RGW_AUTH_GRACE_MINS * 60) ||
         (now_req > now + RGW_AUTH_GRACE_MINS * 60) ) {
      dout(10) << "NOTICE: request time skew too big." << dendl;
      dout(10) << "now_req = " << now_req << " now = " << now << "; now - RGW_AUTH_GRACE_MINS=" << now - RGW_AUTH_GRACE_MINS * 60 << "; now + RGW_AUTH_GRACE_MINS=" << now + RGW_AUTH_GRACE_MINS * 60 << dendl;
      return -ERR_REQUEST_TIME_SKEWED;
    }

    s->aws4_auth->signedheaders = s->info.args.get("X-Amz-SignedHeaders");
    if (s->aws4_auth->signedheaders.size() == 0) {
      return -EPERM;
    }

    s->aws4_auth->signature = s->info.args.get("X-Amz-Signature");
    if (s->aws4_auth->signature.size() == 0) {
      return -EPERM;
    }

  } else {

    /* auth ships in headers ... */

    /* ------------------------- handle Credential header */

    using_qs = false;

    string auth_str = s->http_auth;

#define AWS4_HMAC_SHA256_STR "AWS4-HMAC-SHA256"
#define CREDENTIALS_PREFIX_LEN (sizeof(AWS4_HMAC_SHA256_STR) - 1)
    uint64_t min_len = CREDENTIALS_PREFIX_LEN + 1;
    if (auth_str.length() < min_len) {
      ldout(store->ctx(), 10) << "credentials string is too short" << dendl;
      return -EINVAL;
    }

    list<string> auth_list;
    get_str_list(auth_str.substr(min_len), ",", auth_list);

    map<string, string> kv;

    for (string& s : auth_list) {
      string key, val;
      int ret = parse_key_value(s, key, val);
      if (ret < 0) {
        ldout(store->ctx(), 10) << "NOTICE: failed to parse auth header (s=" << s << ")" << dendl;
        return -EINVAL;
      }
      kv[key] = std::move(val);
    }

    for (string& k : aws4_presigned_required_keys) {
      if (kv.find(k) == kv.end()) {
        ldout(store->ctx(), 10) << "NOTICE: auth header missing key: " << k << dendl;
        return -EINVAL;
      }
    }

    s->aws4_auth->credential = std::move(kv["Credential"]);
    s->aws4_auth->signedheaders = std::move(kv["SignedHeaders"]);
    s->aws4_auth->signature = std::move(kv["Signature"]);

    /* sig hex str */
    dout(10) << "v4 signature format = " << s->aws4_auth->signature << dendl;

    /* ------------------------- handle x-amz-date header */

    /* grab date */

    const char *d = s->info.env->get("HTTP_X_AMZ_DATE");
    struct tm t;
    if (!parse_iso8601(d, &t, NULL, false)) {
      dout(10) << "error reading date via http_x_amz_date" << dendl;
      return -EACCES;
    }
    s->aws4_auth->date = d;
  }

  /* AKIAIVKTAZLOCF43WNQD/AAAAMMDD/region/host/aws4_request */
  dout(10) << "v4 credential format = " << s->aws4_auth->credential << dendl;

  if (std::count(s->aws4_auth->credential.begin(), s->aws4_auth->credential.end(), '/') != 4) {
    return -EINVAL;
  }

  /* credential must end with 'aws4_request' */
  if (s->aws4_auth->credential.find("aws4_request") == std::string::npos) {
    return -EINVAL;
  }

  /* grab access key id */

  pos = s->aws4_auth->credential.find("/");
  s->aws4_auth->access_key_id = s->aws4_auth->credential.substr(0, pos);

  dout(10) << "access key id = " << s->aws4_auth->access_key_id << dendl;

  /* grab credential scope */

  s->aws4_auth->credential_scope = s->aws4_auth->credential.substr(pos + 1, s->aws4_auth->credential.length());

  dout(10) << "credential scope = " << s->aws4_auth->credential_scope << dendl;

  /* grab user information */

  if (rgw_get_user_info_by_access_key(store, s->aws4_auth->access_key_id, *s->user) < 0) {
    dout(10) << "error reading user info, uid=" << s->aws4_auth->access_key_id
              << " can't authenticate" << dendl;
    return -ERR_INVALID_ACCESS_KEY;
  }

  /*
   * create a canonical request
   *
   * http://docs.aws.amazon.com/general/latest/gr/sigv4-create-canonical-request.html
   */

  /* craft canonical uri */

  /* here code should normalize via rfc3986 but S3 does **NOT** do path normalization
   * that SigV4 typically does. this code follows the same approach that boto library
   * see auth.py:canonical_uri(...) */

  s->aws4_auth->canonical_uri = s->info.request_uri_aws4;

  if (s->aws4_auth->canonical_uri.empty()) {
    s->aws4_auth->canonical_uri = "/";
  } else {
    boost::replace_all(s->aws4_auth->canonical_uri, "+", "%20");
  }

  /* craft canonical query string */

  s->aws4_auth->canonical_qs = s->info.request_params;

  if (!s->aws4_auth->canonical_qs.empty()) {

    /* handle case when query string exists. Step 3 in
     * http://docs.aws.amazon.com/general/latest/gr/sigv4-create-canonical-request.html */

    map<string, string> canonical_qs_map;
    istringstream cqs(s->aws4_auth->canonical_qs);
    string keyval;

    while (getline(cqs, keyval, '&')) {
      string key, val;
      istringstream kv(keyval);
      getline(kv, key, '=');
      getline(kv, val, '=');
      if (!using_qs || key != "X-Amz-Signature") {
        string encoded_key;
        string encoded_val;
        if (key != "X-Amz-Credential") {
          string key_decoded;
          url_decode(key, key_decoded);
          if (key.length() != key_decoded.length()) {
            encoded_key = key;
          } else {
            aws4_uri_encode(key, encoded_key);
          }
          string val_decoded;
          url_decode(val, val_decoded);
          if (val.length() != val_decoded.length()) {
            encoded_val = val;
          } else {
            aws4_uri_encode(val, encoded_val);
          }
        } else {
          encoded_key = key;
          encoded_val = val;
        }
        canonical_qs_map[encoded_key] = encoded_val;
      }
    }

    s->aws4_auth->canonical_qs = "";

    map<string, string>::iterator last = canonical_qs_map.end();
    --last;

    for (map<string, string>::iterator it = canonical_qs_map.begin();
        it != canonical_qs_map.end(); ++it) {
      s->aws4_auth->canonical_qs.append(it->first + "=" + it->second);
      if (it != last) {
        s->aws4_auth->canonical_qs.append("&");
      }
    }

  }

  /* craft canonical headers */

  map<string, string> canonical_hdrs_map;
  istringstream sh(s->aws4_auth->signedheaders);
  string token;
  string port = s->info.env->get("SERVER_PORT", "");
  string secure_port = s->info.env->get("SERVER_PORT_SECURE", "");

  while (getline(sh, token, ';')) {
    string token_env = "HTTP_" + token;
    transform(token_env.begin(), token_env.end(), token_env.begin(), ::toupper);
    replace(token_env.begin(), token_env.end(), '-', '_');
    if (token_env == "HTTP_CONTENT_LENGTH") {
      token_env = "CONTENT_LENGTH";
    }
    if (token_env == "HTTP_CONTENT_TYPE") {
      token_env = "CONTENT_TYPE";
    }
    const char *t = s->info.env->get(token_env.c_str());
    if (!t) {
      dout(10) << "warning env var not available" << dendl;
      continue;
    }
    if (token_env == "HTTP_CONTENT_MD5") {
      for (const char *p = t; *p; p++) {
	if (!is_base64_for_content_md5(*p)) {
	  dout(0) << "NOTICE: bad content-md5 provided (not base64), aborting request p=" << *p << " " << (int)*p << dendl;
	  return -EPERM;
	}
      }
    }
    string token_value = string(t);
    if (using_qs && (token == "host")) {
      if (!secure_port.empty()) {
	if (secure_port != "443")
	  token_value = token_value + ":" + secure_port;
      } else if (!port.empty()) {
	if (secure_port != "80")
	  token_value = token_value + ":" + port;
      }
    }
    canonical_hdrs_map[token] = rgw_trim_whitespace(token_value);
  }

  for (map<string, string>::iterator it = canonical_hdrs_map.begin();
      it != canonical_hdrs_map.end(); ++it) {
    s->aws4_auth->canonical_hdrs.append(it->first + ":" + it->second + "\n");
  }

  dout(10) << "canonical headers format = " << s->aws4_auth->canonical_hdrs << dendl;

  /* craft signed headers */

  s->aws4_auth->signed_hdrs = s->aws4_auth->signedheaders;

  /* handle request payload */

  s->aws4_auth->payload_hash = "";

  string request_payload;

  bool unsigned_payload = false;
  s->aws4_auth_streaming_mode = false;

  if (using_qs) {
    /* query parameters auth */
    unsigned_payload = true;
  } else {
    /* header auth */
    const char *request_payload_hash = s->info.env->get("HTTP_X_AMZ_CONTENT_SHA256");
    if (request_payload_hash) {
      unsigned_payload = string("UNSIGNED-PAYLOAD").compare(request_payload_hash) == 0;
      if (!unsigned_payload) {
        s->aws4_auth_streaming_mode = string("STREAMING-AWS4-HMAC-SHA256-PAYLOAD").compare(request_payload_hash) == 0;
      }
    }
  }

  /* from rfc2616 - 4.3 Message Body
   *
   * "The presence of a message-body in a request is signaled by the inclusion of a
   *  Content-Length or Transfer-Encoding header field in the request's message-headers."
   */
  bool body_available = s->content_length != 0 || s->info.env->get("HTTP_TRANSFER_ENCODING") != NULL;

  if (unsigned_payload || !body_available) {

    /* requests lacking of body or shipping with 'UNSIGNED-PAYLOAD' are authenticated now */

    /* complete aws4 auth */

    int err = authorize_v4_complete(store, s, request_payload, unsigned_payload);
    if (err) {
      return err;
    }

    /* verify signature */

    if (s->aws4_auth->signature != s->aws4_auth->new_signature) {
      return -ERR_SIGNATURE_NO_MATCH;
    }

    /* authorization ok */

    dout(10) << "v4 auth ok" << dendl;

    /* aws4 auth completed */

    s->aws4_auth_needs_complete = false;

  } else {

    /* aws4 auth not completed... delay aws4 auth */

    if (!s->aws4_auth_streaming_mode) {

      dout(10) << "delaying v4 auth" << dendl;

      /* payload in a single chunk */

      switch (s->op_type)
      {
        case RGW_OP_CREATE_BUCKET:
        case RGW_OP_PUT_OBJ:
        case RGW_OP_PUT_ACLS:
        case RGW_OP_PUT_CORS:
        case RGW_OP_COMPLETE_MULTIPART:
        case RGW_OP_SET_BUCKET_VERSIONING:
        case RGW_OP_DELETE_MULTI_OBJ:
        case RGW_OP_ADMIN_SET_METADATA:
        case RGW_OP_SET_BUCKET_WEBSITE:
          break;
        default:
          dout(10) << "ERROR: AWS4 completion for this operation NOT IMPLEMENTED" << dendl;
          return -ERR_NOT_IMPLEMENTED;
      }

      s->aws4_auth_needs_complete = true;

    } else {

      dout(10) << "body content detected in multiple chunks" << dendl;

      /* payload in multiple chunks */

      switch(s->op_type)
      {
        case RGW_OP_PUT_OBJ:
          break;
        default:
          dout(10) << "ERROR: AWS4 completion for this operation NOT IMPLEMENTED (streaming mode)" << dendl;
          return -ERR_NOT_IMPLEMENTED;
      }

      /* calculate seed */

      int err = authorize_v4_complete(store, s, "", unsigned_payload);
      if (err) {
        return err;
      }

      /* verify seed signature */

      if (s->aws4_auth->signature != s->aws4_auth->new_signature) {
        dout(10) << "ERROR: AWS4 seed signature does NOT match!" << dendl;
        return -ERR_SIGNATURE_NO_MATCH;
      }

      dout(10) << "aws4 seed signature ok... delaying v4 auth" << dendl;

      s->aws4_auth_needs_complete = false;

    }

  }

  map<string, RGWAccessKey>::iterator iter = s->user->access_keys.find(s->aws4_auth->access_key_id);
  if (iter == s->user->access_keys.end()) {
    dout(0) << "ERROR: access key not encoded in user info" << dendl;
    return -EPERM;
  }

  RGWAccessKey& k = iter->second;

  if (!k.subuser.empty()) {
    map<string, RGWSubUser>::iterator uiter = s->user->subusers.find(k.subuser);
    if (uiter == s->user->subusers.end()) {
      dout(0) << "NOTICE: could not find subuser: " << k.subuser << dendl;
      return -EPERM;
    }
    RGWSubUser& subuser = uiter->second;
    s->perm_mask = subuser.perm_mask;
  } else {
    s->perm_mask = RGW_PERM_FULL_CONTROL;
  }

  if (s->user->system) {
    s->system_request = true;
    dout(20) << "system request" << dendl;
    s->info.args.set_system();
    string euid = s->info.args.get(RGW_SYS_PARAM_PREFIX "uid");
    rgw_user effective_uid(euid);
    RGWUserInfo effective_user;
    if (!effective_uid.empty()) {
      int ret = rgw_get_user_info_by_uid(store, effective_uid, effective_user);
      if (ret < 0) {
        ldout(s->cct, 0) << "User lookup failed!" << dendl;
        return -EACCES;
      }
      *(s->user) = effective_user;
    }
  }

  // populate the owner info
  s->owner.set_id(s->user->user_id);
  s->owner.set_name(s->user->display_name);

  return 0;
}

/*
 * handle v2 signatures
 */
int RGW_Auth_S3::authorize_v2(RGWRados *store, struct req_state *s)
{
  bool qsr = false;
  string auth_id;
  string auth_sign;

  time_t now;
  time(&now);

  if (!s->http_auth || !(*s->http_auth)) {
    auth_id = s->info.args.get("AWSAccessKeyId");
    auth_sign = s->info.args.get("Signature");
    string date = s->info.args.get("Expires");
    time_t exp = atoll(date.c_str());
    if (now >= exp)
      return -EPERM;
    qsr = true;
  } else {
    string auth_str(s->http_auth + 4);
    int pos = auth_str.rfind(':');
    if (pos < 0)
      return -EINVAL;
    auth_id = auth_str.substr(0, pos);
    auth_sign = auth_str.substr(pos + 1);
  }

  /* try keystone auth first */
  int external_auth_result = -ERR_INVALID_ACCESS_KEY;
  if (store->ctx()->_conf->rgw_s3_auth_use_keystone
      && !store->ctx()->_conf->rgw_keystone_url.empty()) {
    dout(20) << "s3 keystone: trying keystone auth" << dendl;

    RGW_Auth_S3_Keystone_ValidateToken keystone_validator(store->ctx());
    string token;

    if (!rgw_create_s3_canonical_header(s->info,
                                        &s->header_time, token, qsr)) {
      dout(10) << "failed to create auth header\n" << token << dendl;
      external_auth_result = -EPERM;
    } else {
      external_auth_result = keystone_validator.validate_s3token(auth_id, token,
							    auth_sign);
      if (external_auth_result == 0) {
	// Check for time skew first
	time_t req_sec = s->header_time.sec();

	if ((req_sec < now - RGW_AUTH_GRACE_MINS * 60 ||
	     req_sec > now + RGW_AUTH_GRACE_MINS * 60) && !qsr) {
	  ldout(s->cct, 10) << "req_sec=" << req_sec << " now=" << now
                            << "; now - RGW_AUTH_GRACE_MINS="
                            << now - RGW_AUTH_GRACE_MINS * 60
                            << "; now + RGW_AUTH_GRACE_MINS="
                            << now + RGW_AUTH_GRACE_MINS * 60
                            << dendl;

	  ldout(s->cct, 0)  << "NOTICE: request time skew too big now="
                            << utime_t(now, 0)
                            << " req_time=" << s->header_time
                            << dendl;
	  return -ERR_REQUEST_TIME_SKEWED;
	}

        string project_id = keystone_validator.response.get_project_id();
        s->user->user_id = project_id;
        s->user->display_name = keystone_validator.response.get_project_name(); // wow.

        rgw_user uid(project_id);
        /* try to store user if it not already exists */
        if (rgw_get_user_info_by_uid(store, uid, *(s->user)) < 0) {
          int ret = rgw_store_user_info(store, *(s->user), NULL, NULL, real_time(), true);
          if (ret < 0)
            dout(10) << "NOTICE: failed to store new user's info: ret="
		     << ret << dendl;
        }

        s->perm_mask = RGW_PERM_FULL_CONTROL;
      }
    }
  }

  S3AuthFactory aplfact(store, s->account_name);
  RGWS3V2Extractor extr(s);
  RGWLDAPAuthEngine ldap(s->cct, store, extr, &aplfact);

  if (ldap.is_applicable()) {
    try {
      auto applier = ldap.authenticate();
      if (! applier) {
        external_auth_result = -EACCES;
      } else {
        try {
          applier->load_acct_info(*s->user);
          s->perm_mask = applier->get_perm_mask();
          applier->modify_request_state(s);
          s->auth_identity = std::move(applier);
          external_auth_result = 0;
        } catch (int err) {
          ldout(s->cct, 5) << "applier threw err=" << err << dendl;
          external_auth_result = err;
        }
      }
    } catch (int err) {
      ldout(s->cct, 5) << "ldap auth engine threw err=" << err << dendl;
      external_auth_result = err;
    }
  }

  /* now try rados backend, but only if keystone did not succeed */
  if (external_auth_result < 0) {
      RGWS3V2LocalAuthEngine localauth(s, store, extr, &aplfact);

      if (! localauth.is_applicable()) {
        return external_auth_result;
      }
      try {
        auto applier = localauth.authenticate();
        if (! applier) {
          return external_auth_result;
        }
        try {
          applier->load_acct_info(*s->user);
          s->perm_mask = applier->get_perm_mask();
          applier->modify_request_state(s);
          s->auth_identity = std::move(applier);
        } catch (int err) {
          ldout(s->cct, 5) << "applier threw err=" << err << dendl;
          return err;
        }
      } catch (int err) {
          ldout(s->cct, 5) << "local auth engine threw err=" << err << dendl;
          return err;
      }
    if (s->user->system) {
      s->system_request = true;
      dout(20) << "system request" << dendl;
      s->info.args.set_system();
      string effective_uid = s->info.args.get(RGW_SYS_PARAM_PREFIX "uid");
      RGWUserInfo effective_user;
      if (!effective_uid.empty()) {
        rgw_user euid(effective_uid);
        int ret = rgw_get_user_info_by_uid(store, euid, effective_user);
        if (ret < 0) {
          ldout(s->cct, 0) << "User lookup failed!" << dendl;
          return -EACCES;
        }
        *(s->user) = effective_user;
      }
    }

  } /* if external_auth_result < 0 */

  // populate the owner info
  s->owner.set_id(s->user->user_id);
  s->owner.set_name(s->user->display_name);

  return  0;
}

int RGWHandler_Auth_S3::init(RGWRados *store, struct req_state *state,
                             rgw::io::BasicClient *cio)
{
  int ret = RGWHandler_REST_S3::init_from_header(state, RGW_FORMAT_JSON,
						     true);
  if (ret < 0)
    return ret;

  return RGWHandler_REST::init(store, state, cio);
}

RGWHandler_REST* RGWRESTMgr_S3::get_handler(struct req_state* const s,
                                            const std::string& frontend_prefix)
{
  bool is_s3website = enable_s3website && (s->prot_flags & RGW_REST_WEBSITE);
  int ret =
    RGWHandler_REST_S3::init_from_header(s,
					is_s3website ? RGW_FORMAT_HTML :
					RGW_FORMAT_XML, true);
  if (ret < 0)
    return NULL;

  RGWHandler_REST* handler;
  // TODO: Make this more readable
  if (is_s3website) {
    if (s->init_state.url_bucket.empty()) {
      handler = new RGWHandler_REST_Service_S3Website;
    } else if (s->object.empty()) {
      handler = new RGWHandler_REST_Bucket_S3Website;
    } else {
      handler = new RGWHandler_REST_Obj_S3Website;
    }
  } else {
    if (s->init_state.url_bucket.empty()) {
      handler = new RGWHandler_REST_Service_S3;
    } else if (s->object.empty()) {
      handler = new RGWHandler_REST_Bucket_S3;
    } else {
      handler = new RGWHandler_REST_Obj_S3;
    }
  }

  ldout(s->cct, 20) << __func__ << " handler=" << typeid(*handler).name()
		    << dendl;
  return handler;
}

int RGWHandler_REST_S3Website::retarget(RGWOp* op, RGWOp** new_op) {
  *new_op = op;
  ldout(s->cct, 10) << __func__ << "Starting retarget" << dendl;

  if (!(s->prot_flags & RGW_REST_WEBSITE))
    return 0;

  RGWObjectCtx& obj_ctx = *static_cast<RGWObjectCtx *>(s->obj_ctx);
  int ret = store->get_bucket_info(obj_ctx, s->bucket_tenant,
				  s->bucket_name, s->bucket_info, NULL,
				  &s->bucket_attrs);
  if (ret < 0) {
      // TODO-FUTURE: if the bucket does not exist, maybe expose it here?
      return -ERR_NO_SUCH_BUCKET;
  }
  if (!s->bucket_info.has_website) {
      // TODO-FUTURE: if the bucket has no WebsiteConfig, expose it here
      return -ERR_NO_SUCH_WEBSITE_CONFIGURATION;
  }

  rgw_obj_key new_obj;
  s->bucket_info.website_conf.get_effective_key(s->object.name, &new_obj.name);
  ldout(s->cct, 10) << "retarget get_effective_key " << s->object << " -> "
		    << new_obj << dendl;

  RGWBWRoutingRule rrule;
  bool should_redirect =
    s->bucket_info.website_conf.should_redirect(new_obj.name, 0, &rrule);

  if (should_redirect) {
    const string& hostname = s->info.env->get("HTTP_HOST", "");
    const string& protocol =
      (s->info.env->get("SERVER_PORT_SECURE") ? "https" : "http");
    int redirect_code = 0;
    rrule.apply_rule(protocol, hostname, s->object.name, &s->redirect,
		    &redirect_code);
    // APply a custom HTTP response code
    if (redirect_code > 0)
      s->err->http_ret = redirect_code; // Apply a custom HTTP response code
    ldout(s->cct, 10) << "retarget redirect code=" << redirect_code
		      << " proto+host:" << protocol << "://" << hostname
		      << " -> " << s->redirect << dendl;
    return -ERR_WEBSITE_REDIRECT;
  }

  /*
   * FIXME: if s->object != new_obj, drop op and create a new op to handle
   * operation. Or remove this comment if it's not applicable anymore
   */

  s->object = new_obj;

  return 0;
}

RGWOp* RGWHandler_REST_S3Website::op_get()
{
  return get_obj_op(true);
}

RGWOp* RGWHandler_REST_S3Website::op_head()
{
  return get_obj_op(false);
}

int RGWHandler_REST_S3Website::serve_errordoc(int http_ret, const string& errordoc_key) {
  int ret = 0;
  s->formatter->reset(); /* Try to throw it all away */

  std::shared_ptr<RGWGetObj_ObjStore_S3Website> getop( (RGWGetObj_ObjStore_S3Website*) op_get() );
  if (getop.get() == NULL) {
    return -1; // Trigger double error handler
  }
  getop->init(store, s, this);
  getop->range_str = NULL;
  getop->if_mod = NULL;
  getop->if_unmod = NULL;
  getop->if_match = NULL;
  getop->if_nomatch = NULL;
  s->object = errordoc_key;

  ret = init_permissions(getop.get());
  if (ret < 0) {
    ldout(s->cct, 20) << "serve_errordoc failed, init_permissions ret=" << ret << dendl;
    return -1; // Trigger double error handler
  }

  ret = read_permissions(getop.get());
  if (ret < 0) {
    ldout(s->cct, 20) << "serve_errordoc failed, read_permissions ret=" << ret << dendl;
    return -1; // Trigger double error handler
  }

  if (http_ret) {
     getop->set_custom_http_response(http_ret);
  }

  ret = getop->init_processing();
  if (ret < 0) {
    ldout(s->cct, 20) << "serve_errordoc failed, init_processing ret=" << ret << dendl;
    return -1; // Trigger double error handler
  }

  ret = getop->verify_op_mask();
  if (ret < 0) {
    ldout(s->cct, 20) << "serve_errordoc failed, verify_op_mask ret=" << ret << dendl;
    return -1; // Trigger double error handler
  }

  ret = getop->verify_permission();
  if (ret < 0) {
    ldout(s->cct, 20) << "serve_errordoc failed, verify_permission ret=" << ret << dendl;
    return -1; // Trigger double error handler
  }

  ret = getop->verify_params();
  if (ret < 0) {
    ldout(s->cct, 20) << "serve_errordoc failed, verify_params ret=" << ret << dendl;
    return -1; // Trigger double error handler
  }

  // No going back now
  getop->pre_exec();
  /*
   * FIXME Missing headers:
   * With a working errordoc, the s3 error fields are rendered as HTTP headers,
   *   x-amz-error-code: NoSuchKey
   *   x-amz-error-message: The specified key does not exist.
   *   x-amz-error-detail-Key: foo
   */
  getop->execute();
  getop->complete();
  return 0;

}

int RGWHandler_REST_S3Website::error_handler(int err_no,
					    string* error_content) {
  int new_err_no = -1;
  rgw_http_errors::const_iterator r = rgw_http_s3_errors.find(err_no > 0 ? err_no : -err_no);
  int http_error_code = -1;

  if (r != rgw_http_s3_errors.end()) {
    http_error_code = r->second.first;
  }
  ldout(s->cct, 10) << "RGWHandler_REST_S3Website::error_handler err_no=" << err_no << " http_ret=" << http_error_code << dendl;

  RGWBWRoutingRule rrule;
  bool should_redirect =
    s->bucket_info.website_conf.should_redirect(s->object.name, http_error_code,
						&rrule);

  if (should_redirect) {
    const string& hostname = s->info.env->get("HTTP_HOST", "");
    const string& protocol =
      (s->info.env->get("SERVER_PORT_SECURE") ? "https" : "http");
    int redirect_code = 0;
    rrule.apply_rule(protocol, hostname, s->object.name, &s->redirect,
		    &redirect_code);
    // Apply a custom HTTP response code
    if (redirect_code > 0)
      s->err->http_ret = redirect_code; // Apply a custom HTTP response code
    ldout(s->cct, 10) << "error handler redirect code=" << redirect_code
		      << " proto+host:" << protocol << "://" << hostname
		      << " -> " << s->redirect << dendl;
    return -ERR_WEBSITE_REDIRECT;
  } else if (err_no == -ERR_WEBSITE_REDIRECT) {
    // Do nothing here, this redirect will be handled in abort_early's ERR_WEBSITE_REDIRECT block
    // Do NOT fire the ErrorDoc handler
  } else if (!s->bucket_info.website_conf.error_doc.empty()) {
    /* This serves an entire page!
       On success, it will return zero, and no further content should be sent to the socket
       On failure, we need the double-error handler
     */
    new_err_no = RGWHandler_REST_S3Website::serve_errordoc(http_error_code, s->bucket_info.website_conf.error_doc);
    if (new_err_no && new_err_no != -1) {
      err_no = new_err_no;
    }
  } else {
    ldout(s->cct, 20) << "No special error handling today!" << dendl;
  }

  return err_no;
}

RGWOp* RGWHandler_REST_Obj_S3Website::get_obj_op(bool get_data)
{
  /** If we are in website mode, then it is explicitly impossible to run GET or
   * HEAD on the actual directory. We must convert the request to run on the
   * suffix object instead!
   */
  RGWGetObj_ObjStore_S3Website* op = new RGWGetObj_ObjStore_S3Website;
  op->set_get_data(get_data);
  return op;
}

RGWOp* RGWHandler_REST_Bucket_S3Website::get_obj_op(bool get_data)
{
  /** If we are in website mode, then it is explicitly impossible to run GET or
   * HEAD on the actual directory. We must convert the request to run on the
   * suffix object instead!
   */
  RGWGetObj_ObjStore_S3Website* op = new RGWGetObj_ObjStore_S3Website;
  op->set_get_data(get_data);
  return op;
}

RGWOp* RGWHandler_REST_Service_S3Website::get_obj_op(bool get_data)
{
  /** If we are in website mode, then it is explicitly impossible to run GET or
   * HEAD on the actual directory. We must convert the request to run on the
   * suffix object instead!
   */
  RGWGetObj_ObjStore_S3Website* op = new RGWGetObj_ObjStore_S3Website;
  op->set_get_data(get_data);
  return op;
}

rgw::LDAPHelper* RGWLDAPAuthEngine::ldh = nullptr;
std::mutex RGWLDAPAuthEngine::mtx;

void RGWLDAPAuthEngine::init(CephContext* const cct)
{
  if (! ldh) {
    std::lock_guard<std::mutex> lck(mtx);
    if (! ldh) {
      const string& ldap_uri = cct->_conf->rgw_ldap_uri;
      const string& ldap_binddn = cct->_conf->rgw_ldap_binddn;
      const string& ldap_searchdn = cct->_conf->rgw_ldap_searchdn;
      const string& ldap_searchfilter = cct->_conf->rgw_ldap_searchfilter;
      const string& ldap_dnattr = cct->_conf->rgw_ldap_dnattr;
      std::string ldap_bindpw = parse_rgw_ldap_bindpw(cct);

      ldh = new rgw::LDAPHelper(ldap_uri, ldap_binddn, ldap_bindpw,
                                ldap_searchdn, ldap_searchfilter, ldap_dnattr);

      ldh->init();
      ldh->bind();
    }
  }
}

RGWRemoteAuthApplier::acl_strategy_t RGWLDAPAuthEngine::get_acl_strategy() const
{
  //This is based on the assumption that the default acl strategy in
  // get_perms_from_aclspec, will take care. Extra acl spec is not required.
  return nullptr;
}

RGWRemoteAuthApplier::AuthInfo
RGWLDAPAuthEngine::get_creds_info(const rgw::RGWToken& token) const noexcept
{
  using acct_privilege_t = RGWRemoteAuthApplier::AuthInfo::acct_privilege_t;

  return RGWRemoteAuthApplier::AuthInfo {
    rgw_user(token.id),
    token.id,
    RGW_PERM_FULL_CONTROL,
    acct_privilege_t::IS_PLAIN_ACCT,
    TYPE_LDAP
  };
}

bool RGWLDAPAuthEngine::is_applicable() const noexcept
{
  if (! RGWS3V2AuthEngine::is_applicable()) {
    return false;
  }

  if (! cct->_conf->rgw_s3_auth_use_ldap ||
      cct->_conf->rgw_ldap_uri.empty()) {
    return false;
  }

  return true;
}

RGWAuthApplier::aplptr_t RGWLDAPAuthEngine::authenticate() const
{
  if(! base64_token.valid()) {
    return nullptr;
  }
  //TODO: Uncomment, when we have a migration plan in place.
  //Check if a user of type other than 'ldap' is already present, if yes, then
  //return error.
  /*RGWUserInfo user_info;
  user_info.user_id = base64_token.id;
  if (rgw_get_user_info_by_uid(store, user_info.user_id, user_info) >= 0) {
    if (user_info.type != TYPE_LDAP) {
      ldout(cct, 10) << "ERROR: User id of type: " << user_info.type << " is already present" << dendl;
      return nullptr;
    }
  }*/

  if (ldh->auth(base64_token.id, base64_token.key) != 0) {
    return nullptr;
  }

  return apl_factory->create_apl_remote(cct, get_acl_strategy(), get_creds_info(base64_token));
}

void RGWS3V2Extractor::get_auth_keys(std::string& access_key_id,
                                      std::string& signature,
                                      std::string& expires,
                                      bool& qsr) const
{
  qsr = false;
  if (! s->http_auth || !(*s->http_auth)) {
    access_key_id = s->info.args.get("AWSAccessKeyId");
    signature = s->info.args.get("Signature");
    expires = s->info.args.get("Expires");
    qsr = true;
  } else {
    string auth_str(s->http_auth + 4);
    int pos = auth_str.rfind(':');
    if (pos >= 0) {
      access_key_id = auth_str.substr(0, pos);
      signature = auth_str.substr(pos + 1);
    }
  }
}

bool RGWS3V2LocalAuthEngine::is_applicable() const noexcept
{
  if (! RGWS3V2AuthEngine::is_applicable()) {
    return false;
  }
  if (! s->cct->_conf->rgw_s3_auth_use_rados) {
    return false;
  }

  return true;
}

RGWAuthApplier::aplptr_t RGWS3V2LocalAuthEngine::authenticate() const
{
  if (access_key_id.empty() || signature.empty()) {
    ldout(s->cct, 5) << "access_key_id or signature is empty" << dendl;
    throw -EINVAL;
  }

  time_t now;
  time(&now);
  if (! expires.empty()) {
    time_t exp = atoll(expires.c_str());
    if (now >= exp) {
      throw -EPERM;
    }
  }
  /* get the user info */
  string access_key = access_key_id;
  if (rgw_get_user_info_by_access_key(store, access_key, *(s->user)) < 0) {
      ldout(s->cct, 5) << "error reading user info, uid=" << access_key_id
              << " can't authenticate" << dendl;
      return nullptr;
  }
  //TODO: Uncomment, when we have a migration plan in place.
  /*else {
    if (s->user->type != TYPE_RGW) {
      ldout(cct, 10) << "ERROR: User id of type: " << s->user->type << " is present" << dendl;
      throw -EPERM;
    }
  }*/

  /* now verify  signature */
  string auth_hdr;
  if (! rgw_create_s3_canonical_header(s->info, &s->header_time, auth_hdr,
        qsr)) {
    ldout(s->cct, 10) << "failed to create auth header\n" << auth_hdr << dendl;
    throw -EPERM;
  }
  ldout(s->cct, 10) << "auth_hdr:\n" << auth_hdr << dendl;

  time_t req_sec = s->header_time.sec();
  if ((req_sec < now - RGW_AUTH_GRACE_MINS * 60 ||
      req_sec > now + RGW_AUTH_GRACE_MINS * 60) && !qsr) {
    ldout(s->cct, 10) << "req_sec=" << req_sec << " now=" << now
             << "; now - RGW_AUTH_GRACE_MINS=" << now - RGW_AUTH_GRACE_MINS * 60
             << "; now + RGW_AUTH_GRACE_MINS=" << now + RGW_AUTH_GRACE_MINS * 60
             << dendl;
    ldout(s->cct, 0)  << "NOTICE: request time skew too big now=" << utime_t(now, 0)
             << " req_time=" << s->header_time
             << dendl;
    throw -ERR_REQUEST_TIME_SKEWED;
  }

  map<string, RGWAccessKey>::iterator iter = s->user->access_keys.find(access_key_id);
  if (iter == s->user->access_keys.end()) {
    ldout(s->cct, 0) << "ERROR: access key not encoded in user info" << dendl;
    throw -EPERM;
  }
  RGWAccessKey& k = iter->second;

  string digest;
  int ret = rgw_get_s3_header_digest(auth_hdr, k.key, digest);
  if (ret < 0) {
    throw -EPERM;
  }

  ldout(s->cct, 15) << "calculated digest=" << digest << dendl;
  ldout(s->cct, 15) << "auth_sign=" << signature << dendl;
  ldout(s->cct, 15) << "compare=" << signature.compare(digest) << dendl;

  if (signature != digest) {
    throw -ERR_SIGNATURE_NO_MATCH;
  }

  return apl_factory->create_apl_local(cct, *(s->user), k.subuser);
}
