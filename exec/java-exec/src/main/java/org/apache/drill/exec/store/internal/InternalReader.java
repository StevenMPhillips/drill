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

import com.google.common.collect.Lists;
import io.netty.buffer.DrillBuf;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.memory.OutOfMemoryException;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.proto.UserBitShared;
import org.apache.drill.exec.proto.UserBitShared.SerializedField;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.MaterializedField.Key;
import org.apache.drill.exec.record.RecordBatchLoader;
import org.apache.drill.exec.record.TransferPair;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.record.selection.SelectionVector2;
import org.apache.drill.exec.store.AbstractRecordReader;
import org.apache.drill.exec.store.RecordReader;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import parquet.hadoop.util.CompatibilityUtil;
import parquet.org.apache.thrift.meta_data.FieldMetaData;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

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

  public InternalReader(FileSystem fs, Path path, List<SchemaPath> columns) throws ExecutionSetupException {
    try {
      this.input = fs.open(path);
      this.columns = columns;
      this.starRequested = containsStar();
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
    UserBitShared.RecordBatchDef batchDef = null;
    try {
      batchDef = UserBitShared.RecordBatchDef.parseDelimitedFrom(input);
      if (batchDef == null) {
        return 0;
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
      ByteBuffer buffer = CompatibilityUtil.getBuf(input, length);
      DrillBuf buf = DrillBuf.wrapByteBuffer(buffer);
      buf.setAllocator(oContext.getAllocator());
      boolean newSchema = loader.load(batchDef, buf);
      if (newSchema) {
        newSchema();
      }
      for (TransferPair tp : transfers) {
        tp.transfer();
      }
    } catch (IOException | SchemaChangeException e) {
      throw new RuntimeException(e);
    }
    /*
    for (SerializedField metaData : fieldList) {
      int dataLength = metaData.getBufferLength();
      MaterializedField field = MaterializedField.create(metaData);
      DrillBuf buf;
      try {
        if (!fieldSelected(field.getPath())) {
          input.seek(input.getPos() + dataLength);
          continue;
        }
        ByteBuffer buffer = CompatibilityUtil.getBuf(input, dataLength);
        buf = DrillBuf.wrapByteBuffer(buffer);
        buf.setAllocator(oContext.getAllocator());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      ValueVector vector = null;
      try {
        vector = output.addField(field, (Class<ValueVector>) TypeHelper.getValueVectorClass(field.getType().getMinorType(), field.getDataMode()));
      } catch (SchemaChangeException e) {
        throw new RuntimeException(e);
      }
      vector.load(metaData, buf);
      buf.release();
    }
    */
    return recordCount;
  }

  private void newSchema() throws SchemaChangeException {
    if (transfers != null) {
      transfers.clear();
    }
    transfers = Lists.newArrayList();
    for (VectorWrapper w : loader) {
      TransferPair tp = w.getValueVector().makeTransferPair(output.addField(w.getField(), w.getVectorClass()));
      transfers.add(tp);
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
