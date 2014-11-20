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
package org.apache.drill.exec.nested;

import org.apache.drill.BaseTestQuery;
import org.junit.Test;

public class TestFastComplexSchema extends BaseTestQuery {

  @Test
  public void test() throws Exception {
//   test("select r.r_name, t1.f from cp.`tpch/region.parquet` r join (select flatten(x) as f from (select convert_from('[0, 1]', 'json') as x from cp.`tpch/region.parquet`)) t1 on t1.f = r.r_regionkey");
    test("SELECT r.r_name, \n" +
            "       t1.f \n" +
            "FROM   cp.`tpch/region.parquet` r \n" +
            "       JOIN (SELECT flatten(x) AS f \n" +
            "             FROM   (SELECT Convert_from('[0, 1]', 'json') AS x \n" +
            "                     FROM   cp.`tpch/region.parquet`)) t1 \n" +
            "         ON t1.f = r.r_regionkey");
  }
}
