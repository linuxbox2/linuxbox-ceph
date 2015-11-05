// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <errno.h>
#include <limits.h>

#include "common/Formatter.h"
#include "common/utf8.h"
#include "include/str_list.h"
#include "rgw_common.h"
#include "rgw_rados.h"
#include "rgw_formats.h"
#include "rgw_op.h"
#include "rgw_rest.h"
#include "rgw_rest_swift.h"
#include "rgw_rest_s3.h"
#include "rgw_swift_auth.h"
#include "rgw_cors_s3.h"

#include "rgw_client_io.h"
#include "rgw_resolve.h"

#define dout_subsys ceph_subsys_rgw


struct rgw_http_status_code {
  int code;
  const char *name;
};

const static struct rgw_http_status_code http_codes[] = {
  { 100, "Continue" },
  { 200, "OK" },
  { 201, "Created" },
  { 202, "Accepted" },
  { 204, "No Content" },
  { 205, "Reset Content" },
  { 206, "Partial Content" },
  { 207, "Multi Status" },
  { 208, "Already Reported" },
  { 300, "Multiple Choices" },
  { 302, "Found" },
  { 303, "See Other" },
  { 304, "Not Modified" },
  { 305, "User Proxy" },
  { 306, "Switch Proxy" },
  { 307, "Temporary Redirect" },
  { 308, "Permanent Redirect" },
  { 400, "Bad Request" },
  { 401, "Unauthorized" },
  { 402, "Payment Required" },
  { 403, "Forbidden" },
  { 404, "Not Found" },
  { 405, "Method Not Allowed" },
  { 406, "Not Acceptable" },
  { 407, "Proxy Authentication Required" },
  { 408, "Request Timeout" },
  { 409, "Conflict" },
  { 410, "Gone" },
  { 411, "Length Required" },
  { 412, "Precondition Failed" },
  { 413, "Request Entity Too Large" },
  { 414, "Request-URI Too Long" },
  { 415, "Unsupported Media Type" },
  { 416, "Requested Range Not Satisfiable" },
  { 417, "Expectation Failed" },
  { 422, "Unprocessable Entity" },
  { 500, "Internal Server Error" },
  { 501, "Not Implemented" },
  { 0, NULL },
};

struct rgw_http_attr {
  const char *rgw_attr;
  const char *http_attr;
};

/*
 * mapping between rgw object attrs and output http fields
 */
static struct rgw_http_attr rgw_to_http_attr_list[] = {
  { RGW_ATTR_CONTENT_TYPE, "Content-Type"},
  { RGW_ATTR_CONTENT_LANG, "Content-Language"},
  { RGW_ATTR_EXPIRES, "Expires"},
  { RGW_ATTR_CACHE_CONTROL, "Cache-Control"},
  { RGW_ATTR_CONTENT_DISP, "Content-Disposition"},
  { RGW_ATTR_CONTENT_ENC, "Content-Encoding"},
  { RGW_ATTR_USER_MANIFEST, "X-Object-Manifest"},
  { NULL, NULL},
};


struct generic_attr {
  const char *http_header;
  const char *rgw_attr;
};

/*
 * mapping between http env fields and rgw object attrs
 */
struct generic_attr generic_attrs[] = {
  { "CONTENT_TYPE", RGW_ATTR_CONTENT_TYPE },
  { "HTTP_CONTENT_LANGUAGE", RGW_ATTR_CONTENT_LANG },
  { "HTTP_EXPIRES", RGW_ATTR_EXPIRES },
  { "HTTP_CACHE_CONTROL", RGW_ATTR_CACHE_CONTROL },
  { "HTTP_CONTENT_DISPOSITION", RGW_ATTR_CONTENT_DISP },
  { "HTTP_CONTENT_ENCODING", RGW_ATTR_CONTENT_ENC },
  { NULL, NULL },
};

map<string, string> rgw_to_http_attrs;
static map<string, string> generic_attrs_map;
map<int, const char *> http_status_names;

/*
 * make attrs look_like_this
 * converts dashes to underscores
 */
string lowercase_underscore_http_attr(const string& orig)
{
  const char *s = orig.c_str();
  char buf[orig.size() + 1];
  buf[orig.size()] = '\0';

  for (size_t i = 0; i < orig.size(); ++i, ++s) {
    switch (*s) {
      case '-':
        buf[i] = '_';
        break;
      default:
        buf[i] = tolower(*s);
    }
  }
  return string(buf);
}

/*
 * make attrs LOOK_LIKE_THIS
 * converts dashes to underscores
 */
string uppercase_underscore_http_attr(const string& orig)
{
  const char *s = orig.c_str();
  char buf[orig.size() + 1];
  buf[orig.size()] = '\0';

  for (size_t i = 0; i < orig.size(); ++i, ++s) {
    switch (*s) {
      case '-':
        buf[i] = '_';
        break;
      default:
        buf[i] = toupper(*s);
    }
  }
  return string(buf);
}

/*
 * make attrs look-like-this
 * converts underscores to dashes
 */
string lowercase_dash_http_attr(const string& orig)
{
  const char *s = orig.c_str();
  char buf[orig.size() + 1];
  buf[orig.size()] = '\0';

  for (size_t i = 0; i < orig.size(); ++i, ++s) {
    switch (*s) {
      case '_':
        buf[i] = '-';
        break;
      default:
        buf[i] = tolower(*s);
    }
  }
  return string(buf);
}

/*
 * make attrs Look-Like-This
 * converts underscores to dashes
 */
string camelcase_dash_http_attr(const string& orig)
{
  const char *s = orig.c_str();
  char buf[orig.size() + 1];
  buf[orig.size()] = '\0';

  bool last_sep = true;

  for (size_t i = 0; i < orig.size(); ++i, ++s) {
    switch (*s) {
      case '_':
        buf[i] = '-';
        last_sep = true;
        break;
      default:
        if (last_sep)
          buf[i] = toupper(*s);
        else
          buf[i] = tolower(*s);
        last_sep = false;
    }
  }
  return string(buf);
}

/* avoid duplicate hostnames in hostnames list */
static set<string> hostnames_set;

void rgw_rest_init(CephContext *cct, list<string>& hostnames)
{
  for (struct rgw_http_attr *attr = rgw_to_http_attr_list; attr->rgw_attr; attr++) {
    rgw_to_http_attrs[attr->rgw_attr] = attr->http_attr;
  }

  for (struct generic_attr *gen_attr = generic_attrs; gen_attr->http_header; gen_attr++) {
    generic_attrs_map[gen_attr->http_header] = gen_attr->rgw_attr;
  }

  list<string> extended_http_attrs;
  get_str_list(cct->_conf->rgw_extended_http_attrs, extended_http_attrs);

  list<string>::iterator iter;
  for (iter = extended_http_attrs.begin(); iter != extended_http_attrs.end(); ++iter) {
    string rgw_attr = RGW_ATTR_PREFIX;
    rgw_attr.append(lowercase_underscore_http_attr(*iter));

    rgw_to_http_attrs[rgw_attr] = camelcase_dash_http_attr(*iter);

    string http_header = "HTTP_";
    http_header.append(uppercase_underscore_http_attr(*iter));

    generic_attrs_map[http_header] = rgw_attr;
  }

  for (const struct rgw_http_status_code *h = http_codes; h->code; h++) {
    http_status_names[h->code] = h->name;
  }

  if (!cct->_conf->rgw_dns_name.empty()) {
    hostnames_set.insert(cct->_conf->rgw_dns_name);
  }
  hostnames_set.insert(hostnames.begin(),  hostnames.end());
  /* TODO: We should have a sanity check that no hostname matches the end of
   * any other hostname, otherwise we will get ambigious results from
   * rgw_find_host_in_domains.
   * Eg: 
   * Hostnames: [A, B.A]
   * Inputs: [Z.A, X.B.A]
   * Z.A clearly splits to subdomain=Z, domain=Z
   * X.B.A ambigously splits to both {X, B.A} and {X.B, A}
   */
}

static bool str_ends_with(const string& s, const string& suffix, size_t *pos)
{
  size_t len = suffix.size();
  if (len > (size_t)s.size()) {
    return false;
  }

  ssize_t p = s.size() - len;
  if (pos) {
    *pos = p;
  }

  return s.compare(p, len, suffix) == 0;
}

static bool rgw_find_host_in_domains(const string& host, string *domain, string *subdomain)
{
  set<string>::iterator iter;
  for (iter = hostnames_set.begin(); iter != hostnames_set.end(); ++iter) {
    size_t pos;
    if (!str_ends_with(host, *iter, &pos))
      continue;

    if (pos == 0) {
      *domain = host;
      subdomain->clear();
    } else {
      if (host[pos - 1] != '.') {
        continue;
      }

      *domain = host.substr(pos);
      *subdomain = host.substr(0, pos - 1);
    }
    return true;
  }
  return false;
}

static void dump_status(struct req_state *s, const char *status, const char *status_name)
{
  int r = s->cio->send_status(status, status_name);
  if (r < 0) {
    ldout(s->cct, 0) << "ERROR: s->cio->send_status() returned err=" << r << dendl;
  }
}

void rgw_flush_formatter_and_reset(struct req_state *s, Formatter *formatter)
{
  std::ostringstream oss;
  formatter->flush(oss);
  std::string outs(oss.str());
  if (!outs.empty() && s->op != OP_HEAD) {
    s->cio->write(outs.c_str(), outs.size());
  }

  s->formatter->reset();
}

void rgw_flush_formatter(struct req_state *s, Formatter *formatter)
{
  std::ostringstream oss;
  formatter->flush(oss);
  std::string outs(oss.str());
  if (!outs.empty() && s->op != OP_HEAD) {
    s->cio->write(outs.c_str(), outs.size());
  }
}

void dump_errno(struct req_state *s)
{
  char buf[32];
  snprintf(buf, sizeof(buf), "%d", s->err->http_ret_E);
  dump_status(s, buf, http_status_names[s->err->http_ret_E]);
}

void dump_string_header(struct req_state *s, const char *name, const char *val)
{
  int r = s->cio->print("%s: %s\r\n", name, val);
  if (r < 0) {
    ldout(s->cct, 0) << "ERROR: s->cio->print() returned err=" << r << dendl;
  }
}

void dump_content_length(struct req_state *s, uint64_t len)
{
  int r = s->cio->send_content_length(len);
  if (r < 0) {
    ldout(s->cct, 0) << "ERROR: s->cio->print() returned err=" << r << dendl;
  }
  r = s->cio->print("Accept-Ranges: %s\r\n", "bytes");
  if (r < 0) {
    ldout(s->cct, 0) << "ERROR: s->cio->print() returned err=" << r << dendl;
  }
}

void dump_etag(struct req_state *s, const char *etag)
{
  int r;
  if (s->prot_flags & RGW_REST_SWIFT)
    r = s->cio->print("etag: %s\r\n", etag);
  else
    r = s->cio->print("ETag: \"%s\"\r\n", etag);
  if (r < 0) {
    ldout(s->cct, 0) << "ERROR: s->cio->print() returned err=" << r << dendl;
  }
}

void dump_pair(struct req_state *s, const char *key, const char *value)
{
  if ( (strlen(key) > 0) && (strlen(value) > 0))
    s->cio->print("%s: %s\r\n", key, value);
}

void dump_redirect(struct req_state *s, const string& redirect)
{
  if (redirect.empty())
    return;

  s->cio->print("Location: %s\r\n", redirect.c_str());
}

void dump_time_header(struct req_state *s, const char *name, time_t t)
{

  char timestr[TIME_BUF_SIZE];
  struct tm result;
  struct tm *tmp = gmtime_r(&t, &result);
  if (tmp == NULL)
    return;

  if (strftime(timestr, sizeof(timestr), "%a, %d %b %Y %H:%M:%S %Z", tmp) == 0)
    return;

  int r = s->cio->print("%s: %s\r\n", name, timestr);
  if (r < 0) {
    ldout(s->cct, 0) << "ERROR: s->cio->print() returned err=" << r << dendl;
  }
}

void dump_last_modified(struct req_state *s, time_t t)
{
  dump_time_header(s, "Last-Modified", t);
}

void dump_epoch_header(struct req_state *s, const char *name, time_t t)
{
  char buf[32];
  snprintf(buf, sizeof(buf), "%lld", (long long)t);

  int r = s->cio->print("%s: %s\r\n", name, buf);
  if (r < 0) {
    ldout(s->cct, 0) << "ERROR: s->cio->print() returned err=" << r << dendl;
  }
}

void dump_time(struct req_state *s, const char *name, time_t *t)
{
  char buf[TIME_BUF_SIZE];
  struct tm result;
  struct tm *tmp = gmtime_r(t, &result);
  if (tmp == NULL)
    return;

  if (strftime(buf, sizeof(buf), "%Y-%m-%dT%T.000Z", tmp) == 0)
    return;

  s->formatter->dump_string(name, buf);
}

void dump_start(struct req_state *s)
{
  if (!s->content_started) {
    if (s->format == RGW_FORMAT_XML)
      s->formatter->write_raw_data(XMLFormatter::XML_1_DTD);
    s->content_started = true;
  }
}

void dump_trans_id(req_state *s)
{
  if (s->prot_flags & RGW_REST_SWIFT) {
    s->cio->print("X-Trans-Id: %s\r\n", s->trans_id.c_str());
  }
  else {
if (s->trans_id.length())	// XXX fix formatting - if this is useful.
    s->cio->print("x-amz-request-id: %s\r\n", s->trans_id.c_str());
  }
}

void end_header(struct req_state *s, boost::function<void()> dump_more, const char *content_type, const int64_t proposed_content_length,
		bool force_content_type)
{
  string ctype;

  dump_trans_id(s);

  if (dump_more) {
    dump_more();
  }

  if (s->prot_flags & RGW_REST_SWIFT && !content_type) {
    force_content_type = true;
  }

  /* do not send content type if content length is zero
     and the content type was not set by the user */
  if (force_content_type || (!content_type &&  s->formatter->get_len()  != 0) || s->is_err()){
    switch (s->format) {
    case RGW_FORMAT_XML:
      ctype = "application/xml";
      break;
    case RGW_FORMAT_JSON:
      ctype = "application/json";
      break;
    default:
      ctype = "text/plain";
      break;
    }
    if (s->prot_flags & RGW_REST_SWIFT)
      ctype.append("; charset=utf-8");
    content_type = ctype.c_str();
  }
  if (s->is_err()) {
    dump_start(s);
    s->err->dump(s->formatter);
    dump_content_length(s, s->formatter->get_len());
  } else {
    if (proposed_content_length != NO_CONTENT_LENGTH) {
      dump_content_length(s, proposed_content_length);
    }
  }

  int r;
  if (content_type) {
      r = s->cio->print("Content-type: %s\r\n", content_type);
      if (r < 0) {
	ldout(s->cct, 0) << "ERROR: s->cio->print() returned err=" << r << dendl;
      }
  }
  r = s->cio->complete_header();
  if (r < 0) {
    ldout(s->cct, 0) << "ERROR: s->cio->complete_header() returned err=" << r << dendl;
  }

  s->cio->set_account(true);
  rgw_flush_formatter_and_reset(s, s->formatter);
}

void abort_early(struct req_state *s, boost::function<void()> dump_more, int err_no)
{
  if (!s->formatter) {
    s->formatter = new JSONFormatter;
    s->format = RGW_FORMAT_JSON;
  }
  s->set_req_state_err(err_no);
  dump_errno(s);
  dump_bucket_from_state(s);
  if (err_no == -ERR_PERMANENT_REDIRECT && !s->region_endpoint.empty()) {
    string dest_uri = s->region_endpoint;
    /*
     * reqest_uri is always start with slash, so we need to remove
     * the unnecessary slash at the end of dest_uri.
     */
    if (dest_uri[dest_uri.size() - 1] == '/') {
      dest_uri = dest_uri.substr(0, dest_uri.size() - 1);
    }
    dest_uri += s->info.request_uri;
    dest_uri += "?";
    dest_uri += s->info.request_params;

    dump_redirect(s, dest_uri);
  }
  end_header(s, dump_more);
  rgw_flush_formatter_and_reset(s, s->formatter);
  perfcounter->inc(l_rgw_failed_req);
}

void dump_continue(struct req_state *s)
{
  s->cio->send_100_continue();
}

void dump_range(struct req_state *s, uint64_t ofs, uint64_t end, uint64_t total)
{
  char range_buf[128];

  /* dumping range into temp buffer first, as libfcgi will fail to digest %lld */
  snprintf(range_buf, sizeof(range_buf), "%lld-%lld/%lld", (long long)ofs, (long long)end, (long long)total);
  int r = s->cio->print("Content-Range: bytes %s\r\n", range_buf);
  if (r < 0) {
    ldout(s->cct, 0) << "ERROR: s->cio->print() returned err=" << r << dendl;
  }
}

int RESTArgs::get_string(struct req_state *s, const string& name, const string& def_val, string *val, bool *existed)
{
  bool exists;
  *val = s->info.args.get(name, &exists);

  if (existed)
    *existed = exists;

  if (!exists) {
    *val = def_val;
    return 0;
  }

  return 0;
}

int RESTArgs::get_uint64(struct req_state *s, const string& name, uint64_t def_val, uint64_t *val, bool *existed)
{
  bool exists;
  string sval = s->info.args.get(name, &exists);

  if (existed)
    *existed = exists;

  if (!exists) {
    *val = def_val;
    return 0;
  }

  int r = stringtoull(sval, val);
  if (r < 0)
    return r;

  return 0;
}

int RESTArgs::get_int64(struct req_state *s, const string& name, int64_t def_val, int64_t *val, bool *existed)
{
  bool exists;
  string sval = s->info.args.get(name, &exists);

  if (existed)
    *existed = exists;

  if (!exists) {
    *val = def_val;
    return 0;
  }

  int r = stringtoll(sval, val);
  if (r < 0)
    return r;

  return 0;
}

int RESTArgs::get_uint32(struct req_state *s, const string& name, uint32_t def_val, uint32_t *val, bool *existed)
{
  bool exists;
  string sval = s->info.args.get(name, &exists);

  if (existed)
    *existed = exists;

  if (!exists) {
    *val = def_val;
    return 0;
  }

  int r = stringtoul(sval, val);
  if (r < 0)
    return r;

  return 0;
}

int RESTArgs::get_int32(struct req_state *s, const string& name, int32_t def_val, int32_t *val, bool *existed)
{
  bool exists;
  string sval = s->info.args.get(name, &exists);

  if (existed)
    *existed = exists;

  if (!exists) {
    *val = def_val;
    return 0;
  }

  int r = stringtol(sval, val);
  if (r < 0)
    return r;

  return 0;
}

int RESTArgs::get_time(struct req_state *s, const string& name, const utime_t& def_val, utime_t *val, bool *existed)
{
  bool exists;
  string sval = s->info.args.get(name, &exists);

  if (existed)
    *existed = exists;

  if (!exists) {
    *val = def_val;
    return 0;
  }

  uint64_t epoch, nsec;

  int r = utime_t::parse_date(sval, &epoch, &nsec);
  if (r < 0)
    return r;

  *val = utime_t(epoch, nsec);

  return 0;
}

int RESTArgs::get_epoch(struct req_state *s, const string& name, uint64_t def_val, uint64_t *epoch, bool *existed)
{
  bool exists;
  string date = s->info.args.get(name, &exists);

  if (existed)
    *existed = exists;

  if (!exists) {
    *epoch = def_val;
    return 0;
  }

  int r = utime_t::parse_date(date, epoch, NULL);
  if (r < 0)
    return r;

  return 0;
}

int RESTArgs::get_bool(struct req_state *s, const string& name, bool def_val, bool *val, bool *existed)
{
  bool exists;
  string sval = s->info.args.get(name, &exists);

  if (existed)
    *existed = exists;

  if (!exists) {
    *val = def_val;
    return 0;
  }

  const char *str = sval.c_str();

  if (sval.empty() ||
      strcasecmp(str, "true") == 0 ||
      sval.compare("1") == 0) {
    *val = true;
    return 0;
  }

  if (strcasecmp(str, "false") != 0 &&
      sval.compare("0") != 0) {
    *val = def_val;
    return -EINVAL;
  }

  *val = false;
  return 0;
}


void RGWRESTFlusher::do_start(int ret)
{
  s->set_req_state_err(ret); /* no going back from here */
  dump_errno(s);
  dump_start(s);
  end_header(s, op);
  rgw_flush_formatter_and_reset(s, s->formatter);
}

void RGWRESTFlusher::do_flush()
{
  rgw_flush_formatter(s, s->formatter);
}

static int read_all_chunked_input(req_state *s, char **pdata, int *plen, int max_read)
{
#define READ_CHUNK 4096
#define MAX_READ_CHUNK (128 * 1024)
  int need_to_read = READ_CHUNK;
  int total = need_to_read;
  char *data = (char *)malloc(total + 1);
  if (!data)
    return -ENOMEM;

  int read_len = 0, len = 0;
  do {
    int r = s->cio->read(data + len, need_to_read, &read_len);
    if (r < 0) {
      free(data);
      return r;
    }

    len += read_len;

    if (read_len == need_to_read) {
      if (need_to_read < MAX_READ_CHUNK)
	need_to_read *= 2;

      if (total > max_read) {
        free(data);
        return -ERANGE;
      }
      total += need_to_read;

      void *p = realloc(data, total + 1);
      if (!p) {
        free(data);
        return -ENOMEM;
      }
      data = (char *)p;
    } else {
      break;
    }

  } while (true);
  data[len] = '\0';

  *pdata = data;
  *plen = len;

  return 0;
}

int rgw_rest_read_all_input(struct req_state *s, char **pdata, int *plen, int max_len)
{
  size_t cl = 0;
  int len = 0;
  char *data = NULL;

  if (s->length)
    cl = atoll(s->length);
  if (cl) {
    if (cl > (size_t)max_len) {
      return -ERANGE;
    }
    data = (char *)malloc(cl + 1);
    if (!data) {
       return -ENOMEM;
    }
    int ret = s->cio->read(data, cl, &len);
    if (ret < 0) {
      free(data);
      return ret;
    }
    data[len] = '\0';
  } else if (!s->length) {
    const char *encoding = s->info.env->get("HTTP_TRANSFER_ENCODING");
    if (!encoding || strcmp(encoding, "chunked") != 0)
      return -ERR_LENGTH_REQUIRED;

    int ret = read_all_chunked_input(s, &data, &len, max_len);
    if (ret < 0)
      return ret;
  }

  *plen = len;
  *pdata = data;

  return 0;
}

static http_op op_from_method(const char *method)
{
  if (!method)
    return OP_UNKNOWN;
  if (strcmp(method, "GET") == 0)
    return OP_GET;
  if (strcmp(method, "PUT") == 0)
    return OP_PUT;
  if (strcmp(method, "DELETE") == 0)
    return OP_DELETE;
  if (strcmp(method, "HEAD") == 0)
    return OP_HEAD;
  if (strcmp(method, "POST") == 0)
    return OP_POST;
  if (strcmp(method, "COPY") == 0)
    return OP_COPY;
  if (strcmp(method, "OPTIONS") == 0)
    return OP_OPTIONS;

  return OP_UNKNOWN;
}

void RGWRESTMgr::register_resource(string resource, RGWRESTMgr *mgr)
{
  string r = "/";
  r.append(resource);

  /* do we have a resource manager registered for this entry point? */
  map<string, RGWRESTMgr *>::iterator iter = resource_mgrs.find(r);
  if (iter != resource_mgrs.end()) {
    delete iter->second;
  }
  resource_mgrs[r] = mgr;
  resources_by_size.insert(pair<size_t, string>(r.size(), r));

  /* now build default resource managers for the path (instead of nested entry points)
   * e.g., if the entry point is /auth/v1.0/ then we'd want to create a default
   * manager for /auth/
   */

  size_t pos = r.find('/', 1);

  while (pos != r.size() - 1 && pos != string::npos) {
    string s = r.substr(0, pos);

    iter = resource_mgrs.find(s);
    if (iter == resource_mgrs.end()) { /* only register it if one does not exist */
      resource_mgrs[s] = new RGWRESTMgr; /* a default do-nothing manager */
      resources_by_size.insert(pair<size_t, string>(s.size(), s));
    }

    pos = r.find('/', pos + 1);
  }
}

void RGWRESTMgr::register_default_mgr(RGWRESTMgr *mgr)
{
  delete default_mgr;
  default_mgr = mgr;
}

RGWRESTMgr *RGWRESTMgr::get_resource_mgr(struct req_state *s, const string& uri, string *out_uri)
{
  *out_uri = uri;

  if (resources_by_size.empty())
    return this;

  multimap<size_t, string>::reverse_iterator iter;

  for (iter = resources_by_size.rbegin(); iter != resources_by_size.rend(); ++iter) {
    string& resource = iter->second;
    if (uri.compare(0, iter->first, resource) == 0 &&
	(uri.size() == iter->first ||
	 uri[iter->first] == '/')) {
      string suffix = uri.substr(iter->first);
      return resource_mgrs[resource]->get_resource_mgr(s, suffix, out_uri);
    }
  }

  if (default_mgr)
    return default_mgr;

  return this;
}

RGWRESTMgr::~RGWRESTMgr()
{
  map<string, RGWRESTMgr *>::iterator iter;
  for (iter = resource_mgrs.begin(); iter != resource_mgrs.end(); ++iter) {
    delete iter->second;
  }
  delete default_mgr;
}

static int64_t parse_content_length(const char *content_length)
{
  int64_t len = -1;

  if (*content_length == '\0') {
    len = 0;
  } else {
    string err;
    len = strict_strtoll(content_length, 10, &err);
    if (!err.empty()) {
      len = -1;
    }
  }

  return len;
}
int RGWREST::preprocess(struct req_state *s, RGWClientIO *cio)
{
  req_info& info = s->info;

  s->cio = cio;
  if (info.host.size()) {
    ldout(s->cct, 10) << "host=" << info.host << dendl;
    string domain;
    string subdomain;
    bool in_hosted_domain = rgw_find_host_in_domains(info.host, &domain,
						     &subdomain);
    ldout(s->cct, 20) << "subdomain=" << subdomain << " domain=" << domain
		      << " in_hosted_domain=" << in_hosted_domain << dendl;

    if (g_conf->rgw_resolve_cname && !in_hosted_domain) {
      string cname;
      bool found;
      int r = rgw_resolver->resolve_cname(info.host, cname, &found);
      if (r < 0) {
	ldout(s->cct, 0) << "WARNING: rgw_resolver->resolve_cname() returned r=" << r << dendl;
      }
      if (found) {
        ldout(s->cct, 5) << "resolved host cname " << info.host << " -> "
			 << cname << dendl;
        in_hosted_domain = rgw_find_host_in_domains(cname, &domain, &subdomain);
        ldout(s->cct, 20) << "subdomain=" << subdomain << " domain=" << domain
			  << " in_hosted_domain=" << in_hosted_domain << dendl;
      }
    }

    if (in_hosted_domain && !subdomain.empty()) {
      string encoded_bucket = "/";
      encoded_bucket.append(subdomain);
      if (s->info.request_uri[0] != '/')
        encoded_bucket.append("/'");
      encoded_bucket.append(s->info.request_uri);
      s->info.request_uri = encoded_bucket;
    }

    if (!domain.empty()) {
      s->info.domain = domain;
    }
  }

  if (s->info.domain.empty()) {
    s->info.domain = s->cct->_conf->rgw_dns_name;
  }

  url_decode(s->info.request_uri, s->decoded_uri);

  /* FastCGI specification, section 6.3
   * http://www.fastcgi.com/devkit/doc/fcgi-spec.html#S6.3
   * ===
   * The Authorizer application receives HTTP request information from the Web
   * server on the FCGI_PARAMS stream, in the same format as a Responder. The
   * Web server does not send CONTENT_LENGTH, PATH_INFO, PATH_TRANSLATED, and
   * SCRIPT_NAME headers.
   * ===
   * Ergo if we are in Authorizer role, we MUST look at HTTP_CONTENT_LENGTH
   * instead of CONTENT_LENGTH for the Content-Length.
   *
   * There is one slight wrinkle in this, and that's older versions of 
   * nginx/lighttpd/apache setting BOTH headers. As a result, we have to check
   * both headers and can't always simply pick A or B.
   */
  const char* content_length = info.env->get("CONTENT_LENGTH");
  const char* http_content_length = info.env->get("HTTP_CONTENT_LENGTH");
  if (!http_content_length != !content_length) {
    /* Easy case: one or the other is missing */
    s->length = (content_length ? content_length : http_content_length);
  } else if (s->cct->_conf->rgw_content_length_compat && content_length && http_content_length) {
    /* Hard case: Both are set, we have to disambiguate */
    int64_t content_length_i, http_content_length_i;

    content_length_i = parse_content_length(content_length);
    http_content_length_i = parse_content_length(http_content_length);

    // Now check them:
    if (http_content_length_i < 0) {
      // HTTP_CONTENT_LENGTH is invalid, ignore it
    } else if (content_length_i < 0) {
      // CONTENT_LENGTH is invalid, and HTTP_CONTENT_LENGTH is valid
      // Swap entries
      content_length = http_content_length;
    } else {
      // both CONTENT_LENGTH and HTTP_CONTENT_LENGTH are valid
      // Let's pick the larger size
      if (content_length_i < http_content_length_i) {
	// prefer the larger value
	content_length = http_content_length;
      }
    }
    s->length = content_length;
    // End of: else if (s->cct->_conf->rgw_content_length_compat && content_length &&
    // http_content_length)
  } else {
    /* no content length was defined */
    s->length = NULL;
  }


  if (s->length) {
    if (*s->length == '\0') {
      s->content_length = 0;
    } else {
      string err;
      s->content_length = strict_strtoll(s->length, 10, &err);
      if (!err.empty()) {
        ldout(s->cct, 10) << "bad content length, aborting" << dendl;
        return -EINVAL;
      }
    }
  }

  if (s->content_length < 0) {
    ldout(s->cct, 10) << "negative content length, aborting" << dendl;
    return -EINVAL;
  }

  map<string, string>::iterator giter;
  for (giter = generic_attrs_map.begin(); giter != generic_attrs_map.end(); ++giter) {
    const char *env = info.env->get(giter->first.c_str());
    if (env) {
      s->generic_attrs[giter->second] = env;
    }
  }

  s->http_auth = info.env->get("HTTP_AUTHORIZATION");

  if (g_conf->rgw_print_continue) {
    const char *expect = info.env->get("HTTP_EXPECT");
    s->expect_cont = (expect && !strcasecmp(expect, "100-continue"));
  }
  s->op = op_from_method(info.method);

  info.init_meta_info(&s->has_bad_meta);

  return 0;
}

RGWHandler *RGWREST::get_handler(RGWRados *store, struct req_state *s, RGWClientIO *cio,
				 RGWRESTMgr **pmgr, int *init_error)
{
  RGWHandler *handler;

  *init_error = preprocess(s, cio);
  if (*init_error < 0)
    return NULL;

  RGWRESTMgr *m = mgr.get_resource_mgr(s, s->decoded_uri, &s->relative_uri);
  if (!m) {
    *init_error = -ERR_METHOD_NOT_ALLOWED;
    return NULL;
  }

  if (pmgr)
    *pmgr = m;

  handler = m->get_handler(s);
  if (!handler) {
    *init_error = -ERR_METHOD_NOT_ALLOWED;
    return NULL;
  }
  *init_error = handler->init(store, s, cio);
  if (*init_error < 0) {
    m->put_handler(handler);
    return NULL;
  }

  return handler;
}

