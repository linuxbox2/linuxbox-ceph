// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <errno.h>
#include "test/librados/test.h"
#include "test/librados/TestCase.h"

using namespace librados;

std::string RadosTest::pool_name;
std::string RadosTest::nspace;
rados_t RadosTest::s_cluster = NULL;

void RadosTest::SetUpTestCase()
{
  pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool(pool_name, &s_cluster));
}

void RadosTest::TearDownTestCase()
{
  ASSERT_EQ(0, destroy_one_pool(pool_name, &s_cluster));
}

void RadosTest::SetUp()
{
  cluster = RadosTest::s_cluster;
  ASSERT_EQ(0, rados_ioctx_create(cluster, pool_name.c_str(), &ioctx));
  nspace = get_temp_pool_name();
  rados_ioctx_set_namespace(ioctx, nspace.c_str());
  ASSERT_FALSE(rados_ioctx_pool_requires_alignment(ioctx));
}

void RadosTest::TearDown()
{
  cleanup_default_namespace(ioctx);
  rados_ioctx_destroy(ioctx);
}

void RadosTest::cleanup_default_namespace(rados_ioctx_t ioctx)
{
  // remove all objects from the default namespace to avoid polluting
  // other tests
  rados_ioctx_set_namespace(ioctx, "");
  rados_list_ctx_t list_ctx;
  ASSERT_EQ(0, rados_objects_list_open(ioctx, &list_ctx));
  int r;
  const char *entry = NULL;
  const char *key = NULL;
  while ((r = rados_objects_list_next(list_ctx, &entry, &key)) != -ENOENT) {
    ASSERT_EQ(0, r);
    rados_ioctx_locator_set_key(ioctx, key);
    ASSERT_EQ(0, rados_remove(ioctx, entry));
  }
  rados_objects_list_close(list_ctx);
}

std::string RadosTestPP::pool_name;
Rados RadosTestPP::s_cluster;

void RadosTestPP::SetUpTestCase()
{
  pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool_pp(pool_name, s_cluster));
}

void RadosTestPP::TearDownTestCase()
{
  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, s_cluster));
}

void RadosTestPP::SetUp()
{
  ASSERT_EQ(0, cluster.ioctx_create(pool_name.c_str(), ioctx));
  ns = get_temp_pool_name();
  ioctx.set_namespace(ns);
  ASSERT_FALSE(ioctx.pool_requires_alignment());
}

void RadosTestPP::TearDown()
{
  cleanup_default_namespace(ioctx);
  ioctx.close();
}

void RadosTestPP::cleanup_default_namespace(librados::IoCtx ioctx)
{
  // remove all objects from the default namespace to avoid polluting
  // other tests
  ioctx.set_namespace("");
  for (ObjectIterator it = ioctx.objects_begin();
       it != ioctx.objects_end(); ++it) {
    ioctx.locator_set_key(it->second);
    ASSERT_EQ(0, ioctx.remove(it->first));
  }
}

std::string RadosTestParamPP::pool_name;
std::string RadosTestParamPP::cache_pool_name;
Rados RadosTestParamPP::s_cluster;

void RadosTestParamPP::SetUpTestCase()
{
  pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_pool_pp(pool_name, s_cluster));
}

void RadosTestParamPP::TearDownTestCase()
{
  if (cache_pool_name.length()) {
    // tear down tiers
    bufferlist inbl;
    ASSERT_EQ(0, s_cluster.mon_command(
      "{\"prefix\": \"osd tier remove-overlay\", \"pool\": \"" + pool_name +
      "\"}",
      inbl, NULL, NULL));
    ASSERT_EQ(0, s_cluster.mon_command(
      "{\"prefix\": \"osd tier remove\", \"pool\": \"" + pool_name +
      "\", \"tierpool\": \"" + cache_pool_name + "\"}",
      inbl, NULL, NULL));
    ASSERT_EQ(0, s_cluster.mon_command(
      "{\"prefix\": \"osd pool delete\", \"pool\": \"" + cache_pool_name +
      "\", \"pool2\": \"" + cache_pool_name + "\", \"sure\": \"--yes-i-really-really-mean-it\"}",
      inbl, NULL, NULL));
    cache_pool_name = "";
  }
  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, s_cluster));
}

void RadosTestParamPP::SetUp()
{
  if (strcmp(GetParam(), "cache") == 0 && cache_pool_name.empty()) {
    cache_pool_name = get_temp_pool_name();
    bufferlist inbl;
    ASSERT_EQ(0, cluster.mon_command(
      "{\"prefix\": \"osd pool create\", \"pool\": \"" + cache_pool_name +
      "\", \"pg_num\": 4}",
      inbl, NULL, NULL));
    ASSERT_EQ(0, cluster.mon_command(
      "{\"prefix\": \"osd tier add\", \"pool\": \"" + pool_name +
      "\", \"tierpool\": \"" + cache_pool_name +
      "\", \"force_nonempty\": \"--force-nonempty\" }",
      inbl, NULL, NULL));
    ASSERT_EQ(0, cluster.mon_command(
      "{\"prefix\": \"osd tier set-overlay\", \"pool\": \"" + pool_name +
      "\", \"overlaypool\": \"" + cache_pool_name + "\"}",
      inbl, NULL, NULL));
    ASSERT_EQ(0, cluster.mon_command(
      "{\"prefix\": \"osd tier cache-mode\", \"pool\": \"" + cache_pool_name +
      "\", \"mode\": \"writeback\"}",
      inbl, NULL, NULL));
    cluster.wait_for_latest_osdmap();
  }

  ASSERT_EQ(0, cluster.ioctx_create(pool_name.c_str(), ioctx));
  ns = get_temp_pool_name();
  ioctx.set_namespace(ns);
  ASSERT_FALSE(ioctx.pool_requires_alignment());
}

void RadosTestParamPP::TearDown()
{
  cleanup_default_namespace(ioctx);
  ioctx.close();
}

void RadosTestParamPP::cleanup_default_namespace(librados::IoCtx ioctx)
{
  // remove all objects from the default namespace to avoid polluting
  // other tests
  ioctx.set_namespace("");
  for (ObjectIterator it = ioctx.objects_begin();
       it != ioctx.objects_end(); ++it) {
    ioctx.locator_set_key(it->second);
    ASSERT_EQ(0, ioctx.remove(it->first));
  }
}

std::string RadosTestEC::pool_name;
rados_t RadosTestEC::s_cluster = NULL;

void RadosTestEC::SetUpTestCase()
{
  pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_ec_pool(pool_name, &s_cluster));
}

void RadosTestEC::TearDownTestCase()
{
  ASSERT_EQ(0, destroy_one_ec_pool(pool_name, &s_cluster));
}

void RadosTestEC::SetUp()
{
  cluster = RadosTestEC::s_cluster;
  ASSERT_EQ(0, rados_ioctx_create(cluster, pool_name.c_str(), &ioctx));
  std::string nspace = get_temp_pool_name();
  rados_ioctx_set_namespace(ioctx, nspace.c_str());
  ASSERT_TRUE(rados_ioctx_pool_requires_alignment(ioctx));
  alignment = rados_ioctx_pool_required_alignment(ioctx);
  ASSERT_NE((unsigned)0, alignment);
}

void RadosTestEC::TearDown()
{
  cleanup_default_namespace(ioctx);
  rados_ioctx_destroy(ioctx);
}

std::string RadosTestECPP::pool_name;
Rados RadosTestECPP::s_cluster;

void RadosTestECPP::SetUpTestCase()
{
  pool_name = get_temp_pool_name();
  ASSERT_EQ("", create_one_ec_pool_pp(pool_name, s_cluster));
}

void RadosTestECPP::TearDownTestCase()
{
  ASSERT_EQ(0, destroy_one_ec_pool_pp(pool_name, s_cluster));
}

void RadosTestECPP::SetUp()
{
  ASSERT_EQ(0, cluster.ioctx_create(pool_name.c_str(), ioctx));
  ns = get_temp_pool_name();
  ioctx.set_namespace(ns);
  ASSERT_TRUE(ioctx.pool_requires_alignment());
  alignment = ioctx.pool_required_alignment();
  ASSERT_NE((unsigned)0, alignment);
}

void RadosTestECPP::TearDown()
{
  cleanup_default_namespace(ioctx);
  ioctx.close();
}

