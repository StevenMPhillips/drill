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
package org.apache.drill.exec.physical.impl.ur;

import com.google.common.collect.Lists;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.memory.OutOfMemoryException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.UnionTypeReducer;
import org.apache.drill.exec.record.AbstractSingleRecordBatch;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.TransferPair;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.complex.impl.EmbeddedVector;

import java.util.List;

public class UnionTypeReducerBatch extends AbstractSingleRecordBatch<UnionTypeReducer> {

  List<TransferPair> transferPairs = Lists.newArrayList();

  public UnionTypeReducerBatch(UnionTypeReducer popConfig, FragmentContext context, RecordBatch incoming) throws OutOfMemoryException {
    super(popConfig, context, incoming);
  }

  @Override
  protected boolean setupNewSchema() throws SchemaChangeException {
    container.clear();
    transferPairs.clear();
    for (VectorWrapper w : incoming) {
      ValueVector v;
      if (w.getValueVector() instanceof EmbeddedVector && ((EmbeddedVector) w.getValueVector()).isSingleType()) {
        v = ((EmbeddedVector) w.getValueVector()).getSingleVector();
      } else {
        v = w.getValueVector();
      }
      TransferPair p = v.getTransferPair(new FieldReference(w.getField().clone().getPath()));
      container.add(p.getTo());
      transferPairs.add(p);
    }
    container.buildSchema(SelectionVectorMode.NONE);
    return true;
  }

  @Override
  protected IterOutcome doWork() {
    for (TransferPair p : transferPairs) {
      p.transfer();
    }
//    for (VectorWrapper w : container) {
//      w.getValueVector().getMutator().setValueCount(incoming.getRecordCount());
//    }
    return IterOutcome.OK;
  }

  @Override
  public int getRecordCount() {
    return incoming.getRecordCount();
  }
}
