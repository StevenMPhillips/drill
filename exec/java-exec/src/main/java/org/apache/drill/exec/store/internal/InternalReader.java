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
package org.apache.drill.exec.store.internal;

import com.google.common.collect.Queues;
import io.netty.buffer.DrillBuf;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.memory.OutOfMemoryException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.physical.impl.sort.RecordBatchData;
import org.apache.drill.exec.proto.UserBitShared;
import org.apache.drill.exec.proto.UserBitShared.SerializedField;
import org.apache.drill.exec.proto.helper.QueryIdHelper;
import org.apache.drill.exec.record.MaterializedField.Key;
import org.apache.drill.exec.record.RecordBatchLoader;
import org.apache.drill.exec.record.TransferPair;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.store.AbstractRecordReader;
import org.apache.drill.exec.store.dfs.easy.EasySubScan;
import org.apache.drill.exec.store.sys.PStoreProvider.DistributedLatch;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Queue;

/**
 * Created by sphillips on 11/1/14.
 */
public class InternalReader extends AbstractRecordReader {


  private OutputMutator output;
  private OperatorContext oContext;
  private FSDataInputStream input;
  private List<SchemaPath> columns;
  private boolean starRequested;
  private RecordBatchLoader loader;
  private List<TransferPair> transfers;
  private FragmentContext context;
  private EasySubScan scan;
  private DistributedLatch latch;
  private Queue<RecordBatchData> batches = Queues.newLinkedBlockingQueue();

  public InternalReader(EasySubScan scan, FileSystem fs, Path path, List<SchemaPath> columns, FragmentContext context) throws ExecutionSetupException {
    try {
      this.input = fs.open(path);
      this.columns = columns;
      this.starRequested = containsStar();
      this.context = context;
      this.scan = scan;
    } catch (IOException e) {
      throw new ExecutionSetupException(e);
    }
  }

  private boolean containsStar() {
    for (SchemaPath expr : this.columns) {
      if (expr.getRootSegment().getPath().equals("*")) {
        return true;
      }
    }
    return false;
  }

  private boolean fieldSelected(SchemaPath field) {
    if (starRequested) {
      return true;
    }
    for (SchemaPath expr : this.columns) {
      if ( expr.contains(field)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public void allocate(Map<Key, ValueVector> vectorMap) throws OutOfMemoryException {
    // no op
  }

  @Override
  public void setup(OutputMutator output) throws ExecutionSetupException {
    this.output = output;
  }

  @Override
  public void setOperatorContext(OperatorContext operatorContext) {
    this.oContext = operatorContext;
    this.loader = new RecordBatchLoader(oContext.getAllocator());
  }

  @Override
  public int next() {
    if (latch == null) {
      latch = context.getDrillbitContext().getPersistentStoreProvider().getDistributedLatch(QueryIdHelper.getQueryId(context.getHandle().getQueryId()), scan.getWidth());
      load();
      latch.countDown();

      try {
        latch.await();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }

    try {
      RecordBatchData rbd = batches.poll();
      if (rbd == null) {
        return 0;
      }
      transfer(rbd);
      return rbd.getRecordCount();
    } catch (SchemaChangeException e) {
      throw new RuntimeException(e);
    }
  }

  private void load() {
    while (true) {
      UserBitShared.RecordBatchDef batchDef = null;
      try {
        batchDef = UserBitShared.RecordBatchDef.parseDelimitedFrom(input);
        if (batchDef == null) {
          return;
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      int recordCount = batchDef.getRecordCount();

      List<SerializedField> fieldList = batchDef.getFieldList();
      int length = 0;
      for (SerializedField field : fieldList) {
        length += field.getBufferLength();
      }
      try {
//      ByteBuffer buffer = CompatibilityUtil.getBuf(input, length);
//      DrillBuf buf = DrillBuf.wrapByteBuffer(buffer);
//      buf.setAllocator(oContext.getAllocator());
        DrillBuf buf = oContext.getAllocator().buffer(length);
        buf.writeBytes(input, length);
        boolean newSchema = loader.load(batchDef, buf);
        buf.release();
        RecordBatchData rbd = new RecordBatchData(loader);
        batches.add(rbd);
      } catch (IOException | SchemaChangeException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private void transfer(RecordBatchData rbd) throws SchemaChangeException {
    for (VectorWrapper w : rbd.getContainer()) {
      TransferPair tp = w.getValueVector().makeTransferPair(output.addField(w.getField(), w.getVectorClass()));
      tp.transfer();
    }
  }

  @Override
  public void cleanup() {
    if (input != null) {
      try {
        input.close();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
