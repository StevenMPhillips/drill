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

import java.util.List;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.util.FileUtils;
import org.apache.drill.exec.client.DrillClient;
import org.apache.drill.exec.pop.PopUnitTestBase;
import org.apache.drill.exec.proto.UserProtos;
import org.apache.drill.exec.record.RecordBatchLoader;
import org.apache.drill.exec.rpc.user.QueryResultBatch;
import org.apache.drill.exec.server.Drillbit;
import org.apache.drill.exec.server.RemoteServiceSet;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.base.Charsets;
import com.google.common.base.Stopwatch;
import com.google.common.io.Files;

public class HBaseRecordReaderTest extends PopUnitTestBase {
  static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(HBaseRecordReaderTest.class);

  private static HBaseAdmin admin;

  private static final String tableName = "testTable";

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    System.out.println("HBaseStorageHandlerTest: setUpBeforeClass()");
    admin = new HBaseAdmin(HBaseTestsSuite.getConf());
    TestTableGenerator.generateHBaseTable(admin, tableName, 2, 1000);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    System.out.println("HBaseStorageHandlerTest: tearDownAfterClass()");
    admin.disableTable(tableName);
    admin.deleteTable(tableName);
    //admin.close();
  }

  @Test
  public void testLocalDistributed() throws Exception {
    String planName = "/hbase/hbase_scan_screen_physical.json";
    testHBaseFullEngineRemote(planName, 1, 1, 10);
  }

  // specific tests should call this method,
  // but it is not marked as a test itself intentionally
  public void testHBaseFullEngineRemote(
      String planFile,
      int numberOfTimesRead /* specified in json plan */,
      int numberOfRowGroups,
      int recordsPerRowGroup) throws Exception{

    RemoteServiceSet serviceSet = RemoteServiceSet.getLocalServiceSet();

    DrillConfig config = DrillConfig.create();

    try(Drillbit bit1 = new Drillbit(config, serviceSet);
        DrillClient client = new DrillClient(config, serviceSet.getCoordinator());) {
      bit1.run();
      client.connect();
      RecordBatchLoader batchLoader = new RecordBatchLoader(client.getAllocator());
      Stopwatch watch = new Stopwatch().start();
      List<QueryResultBatch> result = client.runQuery(
          UserProtos.QueryType.PHYSICAL,
          HBaseTestsSuite.getPlanText(planFile));

    } catch (Exception e) {
      logger.error(e.getMessage(), e);
      throw e;
    }
  }

}
