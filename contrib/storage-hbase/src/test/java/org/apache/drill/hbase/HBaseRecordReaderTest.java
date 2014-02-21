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

import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import mockit.Injectable;
import mockit.NonStrictExpectations;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.client.DrillClient;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.fn.FunctionImplementationRegistry;
import org.apache.drill.exec.memory.TopLevelAllocator;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.impl.OperatorCreatorRegistry;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.pop.PopUnitTestBase;
import org.apache.drill.exec.proto.BitControl;
import org.apache.drill.exec.proto.UserProtos;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.RecordBatchLoader;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.rpc.user.QueryResultBatch;
import org.apache.drill.exec.rpc.user.UserServer;
import org.apache.drill.exec.server.Drillbit;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.server.RemoteServiceSet;
import org.apache.drill.exec.store.hbase.HBaseRecordReader;
import org.apache.drill.exec.store.hbase.HBaseSubScan;
import org.apache.drill.exec.util.VectorUtil;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

public class HBaseRecordReaderTest extends PopUnitTestBase {
  static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(HBaseRecordReaderTest.class);

  private static HBaseAdmin admin;

  private static final String tableName = "testTable";

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    System.out.println("HBaseStorageHandlerTest: setUpBeforeClass()");
    HBaseTestsSuite.setUp();
    admin = new HBaseAdmin(HBaseTestsSuite.getConf());
    TestTableGenerator.generateHBaseTable(admin, tableName, 2, 1000);
    HTable table = new HTable(HBaseTestsSuite.getConf(), tableName);
    Put p = new Put("r1".getBytes());
    p.add("f".getBytes(), "c1".getBytes(), "1".getBytes());
    p.add("f".getBytes(), "c2".getBytes(), "2".getBytes());
    p.add("f".getBytes(), "c3".getBytes(), "3".getBytes());
    p.add("f".getBytes(), "c4".getBytes(), "4".getBytes());
    p.add("f".getBytes(), "c5".getBytes(), "5".getBytes());
    p.add("f".getBytes(), "c6".getBytes(), "6".getBytes());
    table.put(p);
    p = new Put("r2".getBytes());
    p.add("f".getBytes(), "c1".getBytes(), "1".getBytes());
    p.add("f".getBytes(), "c2".getBytes(), "2".getBytes());
    p.add("f".getBytes(), "c3".getBytes(), "3".getBytes());
    p.add("f".getBytes(), "c4".getBytes(), "4".getBytes());
    p.add("f".getBytes(), "c5".getBytes(), "5".getBytes());
    p.add("f".getBytes(), "c6".getBytes(), "6".getBytes());
    table.put(p);
    p = new Put("r3".getBytes());
    p.add("f".getBytes(), "c1".getBytes(), "1".getBytes());
    p.add("f".getBytes(), "c3".getBytes(), "2".getBytes());
    p.add("f".getBytes(), "c5".getBytes(), "3".getBytes());
    p.add("f".getBytes(), "c7".getBytes(), "4".getBytes());
    p.add("f".getBytes(), "c8".getBytes(), "5".getBytes());
    p.add("f".getBytes(), "c9".getBytes(), "6".getBytes());
    table.put(p);
    table.flushCommits();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    System.out.println("HBaseStorageHandlerTest: tearDownAfterClass()");
    admin.disableTable(tableName);
    admin.deleteTable(tableName);
    HBaseTestsSuite.tearDown();
    //admin.close();
  }

  @Test
  public void testRecordReader(@Injectable final DrillbitContext bitContext, @Injectable UserServer.UserClientConnection connection) throws Exception {
    final DrillConfig c = DrillConfig.create();

    new NonStrictExpectations(){{
      bitContext.getMetrics(); result = new MetricRegistry();
      bitContext.getAllocator(); result = new TopLevelAllocator();
      bitContext.getOperatorCreatorRegistry(); result = new OperatorCreatorRegistry(c);
    }};


    FunctionImplementationRegistry registry = new FunctionImplementationRegistry(c);
    FragmentContext context = new FragmentContext(bitContext, BitControl.PlanFragment.getDefaultInstance(), connection, registry);

    HBaseSubScan.HBaseSubScanReadEntry e = new HBaseSubScan.HBaseSubScanReadEntry(tableName, "a", "z");
    List<String> columns = Lists.newArrayList();
//    columns.add("f:c1");
//    columns.add("f:c2");
    Configuration conf = HBaseConfiguration.create();
    HBaseRecordReader reader = new HBaseRecordReader(conf, e, columns, false, context);

    VectorContainer container = new VectorContainer();
    MockMutator mutator = new MockMutator(container);
    reader.setup(mutator);
    int count = 0;
    while ((count = reader.next()) > 0) {
      container.setRecordCount(count);
      VectorUtil.showVectorAccessibleContent(container);
    }
  }

  private class MockMutator implements OutputMutator {
    List<MaterializedField> removedFields = com.beust.jcommander.internal.Lists.newArrayList();
    List<ValueVector> addFields = com.beust.jcommander.internal.Lists.newArrayList();
    private VectorContainer container;

    public MockMutator(VectorContainer container) {
      this.container  = container;
    }

    @Override
    public void removeField(MaterializedField field) throws SchemaChangeException {
      removedFields.add(field);
    }

    @Override
    public void addField(ValueVector vector) throws SchemaChangeException {
      addFields.add(vector);
      container.add(vector);
    }

    @Override
    public void removeAllFields() {
      addFields.clear();
    }

    @Override
    public void setNewSchema() throws SchemaChangeException {
      container.buildSchema(BatchSchema.SelectionVectorMode.NONE);
    }

    List<MaterializedField> getRemovedFields() {
      return removedFields;
    }

    List<ValueVector> getAddFields() {
      return addFields;
    }

    VectorContainer getContainer() {
      return container;
    }
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

      int recordCount = 0;
      for (QueryResultBatch b : result) {
        batchLoader.load(b.getHeader().getDef(), b.getData());
        VectorUtil.showVectorAccessibleContent(batchLoader);
        recordCount += batchLoader.getRecordCount();
      }

      Assert.assertEquals(3, recordCount);

    } catch (Exception e) {
      logger.error(e.getMessage(), e);
      throw e;
    }
  }

}
