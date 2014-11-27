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
package org.apache.drill.exec.physical.impl.validate;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import com.google.common.collect.Lists;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.proto.SchemaUserBitShared.RecordBatchDef;
import org.apache.drill.exec.proto.UserBitShared;
import org.apache.drill.exec.proto.UserBitShared.SerializedField;
import org.apache.drill.exec.proto.helper.QueryIdHelper;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.TypedFieldId;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.record.WritableBatch;
import org.apache.drill.exec.record.selection.SelectionVector2;
import org.apache.drill.exec.record.selection.SelectionVector4;
import org.apache.drill.exec.store.sys.serialize.ProtoSerializer;
import org.apache.drill.exec.util.Utilities;
import org.apache.drill.exec.vector.VectorValidator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class IteratorValidatorBatchIterator implements RecordBatch {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(IteratorValidatorBatchIterator.class);

  static final boolean VALIDATE_VECTORS = false;
  static final boolean PRINT_BATCH_DATA = true;

  static final ProtoSerializer protoSerializer = new ProtoSerializer(RecordBatchDef.WRITE, RecordBatchDef.MERGE);

  private IterOutcome state = IterOutcome.NOT_YET;
  private final RecordBatch incoming;
  private boolean first = true;
  private FSDataOutputStream fos;

  public IteratorValidatorBatchIterator(RecordBatch incoming) {
    this.incoming = incoming;
    if (PRINT_BATCH_DATA) {
      String logLocation = getContext().getConfig().getString(ExecConstants.TRACE_DUMP_DIRECTORY);

      String fileName = Utilities.getFileNameForQueryFragment(incoming.getContext(), logLocation, String.format("%d", incoming.getOperatorId()));

    /* Create the log file we will dump to and initialize the file descriptors */
      try {
        Configuration conf = new Configuration();
        conf.set(FileSystem.FS_DEFAULT_NAME_KEY, getContext().getConfig().getString(ExecConstants.TRACE_DUMP_FILESYSTEM));
        FileSystem fs = FileSystem.get(conf);

      /* create the file */
        fos = fs.create(new Path(fileName));
      } catch (IOException e) {
        throw new RuntimeException("Unable to create file: " + fileName + " check permissions or if directory exists", e);
      }
    }
  }

  private void validateReadState() {
    switch (state) {
    case OK:
    case OK_NEW_SCHEMA:
      return;
    default:
      throw new IllegalStateException(
          String
              .format(
                  "You tried to do a batch data read operation when you were in a state of %s.  You can only do this type of operation when you are in a state of OK or OK_NEW_SCHEMA.",
                  state.name()));
    }
  }

  private int batchNumber = 0;
  private void printBatchInfo() {
    List<SerializedField> metadata = Lists.newArrayList();
    for (MaterializedField field : incoming.getSchema()) {
      metadata.add(field.getAsBuilder().build());
    }
    UserBitShared.RecordBatchDef batchDef = UserBitShared.RecordBatchDef.newBuilder().addAllField(metadata).setRecordCount(incoming.getRecordCount())
            .setCarriesTwoByteSelectionVector(incoming.getSchema().getSelectionVectorMode() == SelectionVectorMode.TWO_BYTE).build();
    try {
      byte[] bytes = protoSerializer.serialize(batchDef);
      String batchDefString = new String(bytes);
      String queryId = QueryIdHelper.getQueryId(getContext().getHandle().getQueryId());
      int majorFragmentId = getContext().getHandle().getMajorFragmentId();
      int minorFragmentId = getContext().getHandle().getMinorFragmentId();
      String s = String.format("{\"queryId\":\"%s\",\"majorFragmentId\":%d,\"minorFragmentId\":%d,\"operatorId\":%d,\"batchNumber\":%d,\"batchDef\":%s}",
              queryId, majorFragmentId, minorFragmentId, incoming.getOperatorId(), batchNumber++, batchDefString);
      logger.debug(s);
      fos.write(s.getBytes());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Iterator<VectorWrapper<?>> iterator() {
    validateReadState();
    return incoming.iterator();
  }

  @Override
  public FragmentContext getContext() {
    return incoming.getContext();
  }

  @Override
  public BatchSchema getSchema() {
    return incoming.getSchema();
  }

  @Override
  public IterOutcome buildSchema() throws SchemaChangeException {
    state = incoming.buildSchema();
    return state;
  }

  @Override
  public int getRecordCount() {
    validateReadState();
    return incoming.getRecordCount();
  }

  @Override
  public void kill(boolean sendUpstream) {
    incoming.kill(sendUpstream);
  }

  @Override
  public SelectionVector2 getSelectionVector2() {
    validateReadState();
    return incoming.getSelectionVector2();
  }

  @Override
  public SelectionVector4 getSelectionVector4() {
    validateReadState();
    return incoming.getSelectionVector4();
  }

  @Override
  public TypedFieldId getValueVectorId(SchemaPath path) {
    validateReadState();
    return incoming.getValueVectorId(path);
  }

  @Override
  public VectorWrapper<?> getValueAccessorById(Class<?> clazz, int... ids) {
//    validateReadState(); TODO fix this
    return incoming.getValueAccessorById(clazz, ids);
  }

  @Override
  public IterOutcome next() {
    if (state == IterOutcome.NONE ) {
      throw new IllegalStateException("The incoming iterator has previously moved to a state of NONE. You should not be attempting to call next() again.");
    }
    state = incoming.next();
    if (first) {
      first = !first;
    }

    if (state == IterOutcome.OK || state == IterOutcome.OK_NEW_SCHEMA) {
      BatchSchema schema = incoming.getSchema();
      if (schema.getFieldCount() == 0) {
        throw new IllegalStateException ("Incoming batch has an empty schema. This is not allowed.");
      }
      if (incoming.getRecordCount() > MAX_BATCH_SIZE) {
        throw new IllegalStateException (String.format("Incoming batch of %s has size %d, which is beyond the limit of %d",  incoming.getClass().getName(), incoming.getRecordCount(), MAX_BATCH_SIZE));
      }

      if (PRINT_BATCH_DATA) {
        printBatchInfo();
      }

      if (VALIDATE_VECTORS) {
        VectorValidator.validate(incoming);
      }
    }

    return state;
  }

  @Override
  public WritableBatch getWritableBatch() {
    validateReadState();
    return incoming.getWritableBatch();
  }

  @Override
  public void cleanup() {
    incoming.cleanup();
    if (fos != null) {
      try {
        fos.close();
      } catch (IOException e) {
        logger.warn("Exception",e);
      }
    }
  }

  @Override
  public int getOperatorId() {
    return -1;
  }

  @Override
  public VectorContainer getOutgoingContainer() {
    throw new UnsupportedOperationException(String.format(" You should not call getOutgoingContainer() for class %s", this.getClass().getCanonicalName()));
  }

}
