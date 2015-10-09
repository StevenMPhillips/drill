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
package org.apache.drill.exec.vector.complex.impl;

import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.TransferPair;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.complex.AbstractMapVector;
import org.apache.drill.exec.vector.complex.MapVector;
import org.apache.drill.exec.vector.complex.writer.FieldWriter;

import java.lang.reflect.InvocationTargetException;

public class PromotableWriter extends AbstractPromotableFieldWriter {

  private final AbstractMapVector parentContainer;

  private enum State {
    SINGLE, UNION
  }

  private MinorType type;
  private ValueVector vector;
  private UnionVector unionVector;
  private State state;
  private FieldWriter writer;

  public PromotableWriter(ValueVector v, AbstractMapVector parentContainer) {
    super(null);
    this.parentContainer = parentContainer;
    if (v instanceof UnionVector) {
      state = State.UNION;
      unionVector = (UnionVector) v;
      writer = new UnionWriter(unionVector);
    } else {
      state = State.SINGLE;
      vector = v;
      type = v.getField().getType().getMinorType();
      Class writerClass = TypeHelper.getWriterImpl(v.getField().getType().getMinorType(), v.getField().getDataMode());
      Class vectorClass = TypeHelper.getValueVectorClass(v.getField().getType().getMinorType(), v.getField().getDataMode());
      try {
        writer = (FieldWriter) writerClass.getConstructor(vectorClass, AbstractFieldWriter.class).newInstance(vector, null);
      } catch (ReflectiveOperationException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public void setPosition(int index) {
    super.setPosition(index);
    getWriter().setPosition(index);
  }

  protected FieldWriter getWriter(MinorType type) {
    if (state == State.UNION) {
      return writer;
    }
    if (type != this.type) {
      return promoteToUnion();
    }
    return writer;
  }

  protected FieldWriter getWriter() {
    return getWriter(type);
  }

  private FieldWriter promoteToUnion() {
    String name = vector.getField().getLastName();
    TransferPair tp = vector.getTransferPair(new FieldReference(vector.getField().getType().getMinorType().name().toLowerCase()));
    tp.transfer();
    unionVector = parentContainer.addOrGet(name, Types.optional(MinorType.UNION), UnionVector.class);
    unionVector.addVector(tp.getTo());
    writer = new UnionWriter(unionVector);
    writer.setPosition(idx());
    for (int i = 0; i < idx(); i++) {
      unionVector.getMutator().setType(i, vector.getField().getType().getMinorType());
    }
    vector = null;
    state = State.UNION;
    return writer;
  }

  @Override
  public void allocate() {
    getWriter().allocate();
  }

  @Override
  public void clear() {
    getWriter().clear();
  }

  @Override
  public MaterializedField getField() {
    return getWriter().getField();
  }

  @Override
  public int getValueCapacity() {
    return getWriter().getValueCapacity();
  }

  @Override
  public void close() throws Exception {
    getWriter().close();
  }
}
