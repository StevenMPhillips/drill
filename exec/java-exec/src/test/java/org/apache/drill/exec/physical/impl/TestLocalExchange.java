/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.physical.impl;

import org.apache.drill.exec.client.DrillClient;
import org.apache.drill.exec.pop.PopUnitTestBase;
import org.apache.drill.exec.proto.UserBitShared.QueryType;
import org.apache.drill.exec.rpc.user.QueryResultBatch;
import org.apache.drill.exec.server.Drillbit;
import org.apache.drill.exec.server.RemoteServiceSet;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class TestLocalExchange extends PopUnitTestBase {

  @Test
  public void test() throws Exception {
    RemoteServiceSet serviceSet = RemoteServiceSet.getLocalServiceSet();

    try (Drillbit bit1 = new Drillbit(CONFIG, serviceSet);
         Drillbit bit2 = new Drillbit(CONFIG, serviceSet);
         DrillClient client = new DrillClient(CONFIG, serviceSet.getCoordinator());) {

      bit1.run();
      bit2.run();
      client.connect();

      List<QueryResultBatch> results = client.runQuery(QueryType.SQL, "alter session set `planner.slice_target`=1");
      for (QueryResultBatch b : results) {
        b.release();
      }

      results = client.runQuery(QueryType.SQL,
      //    "select position_title, count(*) from cp.`employee.json` group by position_title order by position_title");
          "select * from cp.`region.json` r join cp.`employee.json` e on r.region_id = e.employee_id");
      int count = 0;
      for (QueryResultBatch b : results) {
        count += b.getHeader().getRowCount();
        b.release();
      }
      //assertEquals(18, count);
      assertEquals(108, count);
    }
  }
}
