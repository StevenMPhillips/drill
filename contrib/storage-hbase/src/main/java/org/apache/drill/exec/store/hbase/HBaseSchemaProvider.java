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

import java.io.IOException;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.physical.ReadEntryWithPath;
import org.apache.drill.exec.store.SchemaProvider;
import org.apache.hadoop.conf.Configuration;

import com.beust.jcommander.internal.Lists;

public class HBaseSchemaProvider implements SchemaProvider{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HBaseSchemaProvider.class);

  public static final String HADOOP_DEFAULT_NAME = "fs.default.name";
  final HBaseStorageEngineConfig configuration;
  final Configuration conf;

  public HBaseSchemaProvider(HBaseStorageEngineConfig configuration, DrillConfig config) throws IOException{
    this.configuration = configuration;
    this.conf = new Configuration();
  }

  @Override
  public Object getSelectionBaseOnName(String tableName) {
    try{
//      if(!fs.exists(new Path(tableName))) return null;
      ReadEntryWithPath re = new ReadEntryWithPath(tableName);
      return Lists.newArrayList(re);
    }catch(Exception e){
      logger.warn(String.format("Failure while checking table name %s.", tableName), e);
      return null;
    }
  }
}
