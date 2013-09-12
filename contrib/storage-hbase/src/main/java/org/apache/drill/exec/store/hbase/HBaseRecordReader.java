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
package org.apache.drill.exec.store.hbase;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.store.RecordReader;
import org.apache.drill.exec.store.hbase.HBaseRowGroupScan.HBaseRowGroupReadEntry;

public class HBaseRecordReader implements RecordReader {

  private HBaseStorageEngineConfig engineConfig = null;
  private HBaseRowGroupReadEntry rowGroupReadEntry = null;

  public HBaseRecordReader(HBaseStorageEngineConfig engineConfig, HBaseRowGroupReadEntry e) {
    this.engineConfig = engineConfig;
    this.rowGroupReadEntry = e;
  }

  @Override
  public void setup(OutputMutator output) throws ExecutionSetupException {
    System.out.println("");
  }

  @Override
  public int next() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public void cleanup() {
    System.out.println("");
  }
}
