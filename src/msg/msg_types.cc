
#include "msg_types.h"

#include <arpa/inet.h>
#include <stdlib.h>
#include <string.h>
#include <netdb.h>

#include "common/Formatter.h"

void entity_name_t::dump(Formatter *f) const
{
  f->dump_string("type", type_str());
  f->dump_unsigned("num", num());
}


void entity_addr_t::dump(Formatter *f) const
{
  switch(transport_type) {
  case TRANSPORT_SIMPLE_MESSENGER:
    break;
  default:
    f->dump_unsigned("transport", (unsigned)transport_type);
  }
  f->dump_unsigned("nonce", nonce);
  f->dump_stream("addr") << addr;
}

void entity_name_t::generate_test_instances(list<entity_name_t*>& o)
{
  o.push_back(new entity_name_t(entity_name_t::MON()));
  o.push_back(new entity_name_t(entity_name_t::MON(1)));
  o.push_back(new entity_name_t(entity_name_t::OSD(1)));
  o.push_back(new entity_name_t(entity_name_t::CLIENT(1)));
}

void entity_addr_t::generate_test_instances(list<entity_addr_t*>& o)
{
  o.push_back(new entity_addr_t());
  entity_addr_t *a = new entity_addr_t();
  a->set_nonce(1);
  o.push_back(a);
  entity_addr_t *b = new entity_addr_t();
  b->set_nonce(5);
  b->set_family(AF_INET);
  b->set_in4_quad(0, 127);
  b->set_in4_quad(1, 0);
  b->set_in4_quad(2, 1);
  b->set_in4_quad(3, 2);
  b->set_port(2);
  o.push_back(b);
}

/*
 * simplified grammar:
 *
 * entityaddr: type addr portno nonce ;
 * type : "sm://" | "rdma://" | "xtcp://" | ;
 * addr: "[" address "]" | address ;
 * address: xx"."xx"."xx"."xx | xx":"xx":"xx":"xx":"xx":"xx ;
 * port : ":"xx | ;
 * nonce : "/"xx | ;
 * xx: RE"[0-9a-fA-F]*";
 */
bool entity_addr_t::parse(const char *s, const char **end)
{
  unsigned type = 0;
  memset(this, 0, sizeof(*this));
  const char *start = s;

  if (!strncmp(start, "sm://", 5)) {
    type = TRANSPORT_SIMPLE_MESSENGER;
    start += 5;
  } else if (!strncmp(start, "rdma://", 7)) {
    type = TRANSPORT_ACCELIO_RDMA;
    start += 7;
  } else if (!strncmp(start, "xtcp://", 7)) {
    type = TRANSPORT_ACCELIO_TCP;
    start += 7;
  }
  bool brackets = false;
  if (*start == '[') {
    start++;
    brackets = true;
  }
  
  // inet_pton() requires a null terminated input, so let's fill two
  // buffers, one with ipv4 allowed characters, and one with ipv6, and
  // then see which parses.
  char buf4[39];
  char *o = buf4;
  const char *p = start;
  while (o < buf4 + sizeof(buf4) &&
	 *p && ((*p == '.') ||
		(*p >= '0' && *p <= '9'))) {
    *o++ = *p++;
  }
  *o = 0;

  char buf6[64];  // actually 39 + null is sufficient.
  o = buf6;
  p = start;
  while (o < buf6 + sizeof(buf6) &&
	 *p && ((*p == ':') ||
		(*p >= '0' && *p <= '9') ||
		(*p >= 'a' && *p <= 'f') ||
		(*p >= 'A' && *p <= 'F'))) {
    *o++ = *p++;
  }
  *o = 0;
  //cout << "buf4 is '" << buf4 << "', buf6 is '" << buf6 << "'" << std::endl;

  // ipv4?
  struct in_addr a4;
  struct in6_addr a6;
  if (inet_pton(AF_INET, buf4, &a4)) {
    addr4.sin_addr.s_addr = a4.s_addr;
    addr.ss_family = AF_INET;
    p = start + strlen(buf4);
  } else if (inet_pton(AF_INET6, buf6, &a6)) {
    addr.ss_family = AF_INET6;
    memcpy(&addr6.sin6_addr, &a6, sizeof(a6));
    p = start + strlen(buf6);
  } else {
    return false;
  }

  if (brackets) {
    if (*p != ']')
      return false;
    p++;
  }
  
  //cout << "p is " << *p << std::endl;
  if (*p == ':') {
    // parse a port, too!
    p++;
    int port = atoi(p);
    set_port(port);
    while (*p && *p >= '0' && *p <= '9')
      p++;
  }

  if (*p == '/') {
    // parse nonce, too
    p++;
    int non = atoi(p);
    set_nonce(non);
    while (*p && *p >= '0' && *p <= '9')
      p++;
  }

  if (type) set_transport_type(type);

  if (end)
    *end = p;

  //cout << *this << std::endl;
  return true;
}


// -------
// entity_addrvec_t

void entity_addrvec_t::encode(bufferlist& bl, uint64_t features) const
{
  if ((features & CEPH_FEATURE_MSG_ADDR2) == 0) {
    // encode a single legacy entity_addr_t for unfeatured peers
    if (v.size() > 0) {
      ::encode(v[0], bl, 0);
    } else {
      ::encode(entity_addr_t(), bl, 0);
    }
    return;
  }
  ::encode((__u8)2, bl);
  ::encode(v, bl, features);
}

void entity_addrvec_t::decode(bufferlist::iterator& bl)
{
  __u8 marker;
  ::decode(marker, bl);
  if (marker == 0) {
    // legacy!
    ::decode(marker, bl);
    __u16 rest;
    ::decode(rest, bl);
    entity_addr_t addr;
    addr.transport_type = ((__u32)marker << 16) + rest;
    ::decode(addr.nonce, bl);
    ::decode(addr.addr, bl);
    v.clear();
    v.push_back(addr);
    return;
  }
  if (marker > 2)
    throw buffer::malformed_input("entity_addrvec_t marker > 2");
  ::decode(v, bl);
}

void entity_addrvec_t::dump(Formatter *f) const
{
  f->open_array_section("addrs");
  for (vector<entity_addr_t>::const_iterator p = v.begin();
       p != v.end(); ++p) {
    f->open_object_section("addr");
    p->dump(f);
    f->close_section();
  }
  f->close_section();
}

void entity_addrvec_t::generate_test_instances(list<entity_addrvec_t*>& ls)
{
  ls.push_back(new entity_addrvec_t());
  ls.push_back(new entity_addrvec_t());
  ls.back()->v.push_back(entity_addr_t());
  ls.push_back(new entity_addrvec_t());
  ls.back()->v.push_back(entity_addr_t());
  ls.back()->v.push_back(entity_addr_t());
}


ostream& operator<<(ostream& out, const sockaddr_storage &ss)
{
  char buf[NI_MAXHOST] = { 0 };
  char serv[NI_MAXSERV] = { 0 };
  size_t hostlen;

  if (ss.ss_family == AF_INET)
    hostlen = sizeof(struct sockaddr_in);
  else if (ss.ss_family == AF_INET6)
    hostlen = sizeof(struct sockaddr_in6);
  else
    hostlen = sizeof(struct sockaddr_storage);
  getnameinfo((struct sockaddr *)&ss, hostlen, buf, sizeof(buf),
	      serv, sizeof(serv),
	      NI_NUMERICHOST | NI_NUMERICSERV);
  if (ss.ss_family == AF_INET6)
    return out << '[' << buf << "]:" << serv;
  return out //<< ss.ss_family << ":"
	     << buf << ':' << serv;
}

ostream& operator<<(ostream& out, entity_addr_t::type_tt tt)
{
  switch(tt) {
  case entity_addr_t::TRANSPORT_SIMPLE_MESSENGER:
    return out;
  case entity_addr_t::TRANSPORT_ACCELIO_RDMA:
    return out << "rdma://";
  case entity_addr_t::TRANSPORT_ACCELIO_TCP:
    return out << "xtcp://";
  default:
    return out << "?" << ((unsigned)tt) << "?://";
  }
}

ostream& operator<<(ostream& out, const entity_addrvec_t &addr)
{
  const char *sep = "";
  for (std::vector<entity_addr_t>::const_iterator i = addr.v.begin();
	i != addr.v.end();
	++i) {
    out << sep << *i;
    sep = ",";
  }
  return out;
}

bool entity_addrvec_t::contains(entity_addr_t&a)
{
  for (std::vector<entity_addr_t>::const_iterator i = this.v.begin();
	i != this.v.end();
	++i) {
    if (probably_equals(*i, a))
	return true;
  }
  return false;
}

bool entity_addrvec_t::contains_any_of(entity_addrvec_t&ls)
{
  for (std::vector<entity_addr_t>::const_iterator i = ls.v.begin();
	i != ls.v.end();
	++i) {
    if (contains(*i)) return true;
  }
  return false;
}
