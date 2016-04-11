/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.store.parquet2;

import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableList;
import mockit.Injectable;
import mockit.NonStrictExpectations;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.memory.RootAllocator;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.server.options.OptionManager;
import org.apache.drill.exec.store.TestOutputMutator;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.drill.exec.store.parquet.RowGroupReadEntry;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.junit.Test;

import java.io.IOException;

public class TestReader {

  private static final String FILE = "/tmp/nomura_parquet/0_0_0.parquet";
  private static final BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);

  @Test
  public void q(@Injectable final FragmentContext context,
                @Injectable final OperatorContext oContext,
                @Injectable final RowGroupReadEntry entry) throws IOException, ExecutionSetupException {
    new NonStrictExpectations() {{
      context.getOptions(); result = null;
      oContext.getAllocator(); result = allocator;
      entry.getPath(); result = FILE;
      entry.getRowGroupIndex(); result = 0;
    }};
    Configuration conf = new Configuration();
    DrillFileSystem fs = new DrillFileSystem(conf);

    ParquetMetadata footer = ParquetFileReader.readFooter(conf, new Path(FILE));
    DrillParquetReader reader = new DrillParquetReader(context, footer, entry, ImmutableList.of(SchemaPath.getSimplePath("collatItems").getChild("INTEX$LOAN_INFO_CUR_CONTRIB_BALANCE")), fs);
    TestOutputMutator mutator = new TestOutputMutator(allocator);
    reader.setup(oContext, mutator);
    int count;
    int total = 0;
    while ((count = reader.next()) > 0) {
      mutator.getContainer().zeroVectors();
      mutator.allocate(1000);
      total += count;
      for (VectorWrapper v : mutator.getContainer()) {
        v.getValueVector().allocateNew();
      }
    }
    System.out.println(total);
  }
}
