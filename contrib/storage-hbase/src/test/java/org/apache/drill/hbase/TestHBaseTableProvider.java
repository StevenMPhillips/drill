/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.hbase;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.drill.exec.store.hbase.DrillHBaseConstants;
import org.apache.drill.exec.store.hbase.config.HBasePStoreProvider;
import org.apache.drill.exec.store.sys.PStore;
import org.apache.drill.exec.store.sys.PStoreConfig;
import org.apache.drill.exec.store.sys.PStoreRegistry;
import org.apache.hadoop.hbase.HConstants;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.hive12.common.collect.Maps;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValueFactory;

public class TestHBaseTableProvider extends BaseHBaseTest {

  private static HBasePStoreProvider provider;

  @BeforeClass // mask HBase cluster start function
  public static void setUpBeforeTestHBaseTableProvider() throws Exception {
    Map<String, String> hbaseProps = Maps.newHashMap();
    hbaseProps.put(HConstants.ZOOKEEPER_QUORUM, storagePluginConfig.getZookeeperQuorum());
    hbaseProps.put(DrillHBaseConstants.HBASE_ZOOKEEPER_PORT, storagePluginConfig.getZookeeperport());
    Config newConfig = bit.getContext().getConfig()
        .withValue(DrillHBaseConstants.SYS_STORE_PROVIDER_HBASE_CONFIG, ConfigValueFactory.fromMap(hbaseProps))
        .withValue(DrillHBaseConstants.SYS_STORE_PROVIDER_HBASE_TABLE, ConfigValueFactory.fromAnyRef("drill_store"));
    PStoreRegistry registry = new PStoreRegistry(bit.getCoordinator(), newConfig);
    provider = new HBasePStoreProvider(registry);
    provider.start();
  }

  @Test
  public void testTableProvider() throws IOException {
    PStore<String> hbaseStore = provider.getPStore(PStoreConfig.newJacksonBuilder(config.getMapper(), String.class).name("hbase").build());
    hbaseStore.put("", "v0");
    hbaseStore.put("k1", "v1");
    hbaseStore.put("k2", "v2");
    hbaseStore.put("k3", "v3");
    hbaseStore.put("k4", "v4");
    hbaseStore.put("k5", "v5");
    hbaseStore.put(".test", "testValue");

    assertEquals("v0", hbaseStore.get(""));
    assertEquals("testValue", hbaseStore.get(".test"));

    int rowCount = 0;
    for (Entry<String, String> entry : hbaseStore) {
      rowCount++;
      System.out.println(entry.getKey() + "=" + entry.getValue());
    }
    assertEquals(7, rowCount);

    PStore<String> hbaseTestStore = provider.getPStore(PStoreConfig.newJacksonBuilder(config.getMapper(), String.class).name("hbase.test").build());
    hbaseTestStore.put("", "v0");
    hbaseTestStore.put("k1", "v1");
    hbaseTestStore.put("k2", "v2");
    hbaseTestStore.put("k3", "v3");
    hbaseTestStore.put("k4", "v4");
    hbaseTestStore.put(".test", "testValue");

    assertEquals("v0", hbaseStore.get(""));
    assertEquals("testValue", hbaseStore.get(".test"));

    rowCount = 0;
    for (Entry<String, String> entry : hbaseTestStore) {
      rowCount++;
      System.out.println(entry.getKey() + "=" + entry.getValue());
    }
    assertEquals(6, rowCount);
  }

  @AfterClass
  public static void tearDownTestHBaseTableProvider() {
    provider.close();
  }
}
