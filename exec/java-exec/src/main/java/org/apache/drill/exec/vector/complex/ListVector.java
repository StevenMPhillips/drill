/*******************************************************************************

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
 ******************************************************************************/
package org.apache.drill.exec.vector.complex;

import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.memory.OutOfMemoryRuntimeException;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.TransferPair;
import org.apache.drill.exec.util.CallBack;
import org.apache.drill.exec.util.JsonStringArrayList;
import org.apache.drill.exec.vector.UInt4Vector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.complex.impl.ComplexCopier;
import org.apache.drill.exec.vector.complex.impl.UnionListReader;
import org.apache.drill.exec.vector.complex.impl.UnionListWriter;
import org.apache.drill.exec.vector.complex.impl.UnionVector;
import org.apache.drill.exec.vector.complex.reader.FieldReader;
import org.apache.drill.exec.vector.complex.writer.FieldWriter;

import java.util.List;

public class ListVector extends BaseRepeatedValueVector {

  UInt4Vector offsets;
  Mutator mutator = new Mutator();
  Accessor accessor = new Accessor();
  UnionListWriter writer;
  UnionListReader reader;

  public ListVector(MaterializedField field, BufferAllocator allocator, CallBack callBack) {
    super(field, allocator, new UnionVector(field, allocator, callBack));
    offsets = getOffsetVector();
    this.field.addChild(getDataVector().getField());
    this.writer = new UnionListWriter(this);
    this.reader = new UnionListReader(this);
  }

  public UnionListWriter getWriter() {
    return writer;
  }

  @Override
  public void allocateNew() throws OutOfMemoryRuntimeException {
    super.allocateNewSafe();
  }

  public void transferTo(ListVector target) {
    offsets.makeTransferPair(target.offsets).transfer();
    bits.makeTransferPair(target.bits).transfer();
    getDataVector().makeTransferPair(target.getDataVector()).transfer();
  }

  public void copyFrom(int inIndex, int outIndex, ListVector from) {
    FieldReader in = from.getReader();
    in.setPosition(inIndex);
    FieldWriter out = getWriter();
    out.setPosition(outIndex);
    ComplexCopier copier = new ComplexCopier(in, out);
    copier.write();
  }

  @Override
  public UnionVector getDataVector() {
    return (UnionVector) vector;
  }

  @Override
  public TransferPair getTransferPair(FieldReference ref) {
    return new TransferImpl(field.withPath(ref));
  }

  @Override
  public TransferPair makeTransferPair(ValueVector target) {
    return new TransferImpl((ListVector) target);
  }

  private class TransferImpl implements TransferPair {

    ListVector to;

    public TransferImpl(MaterializedField field) {
      to = new ListVector(field, allocator, null);
    }

    public TransferImpl(ListVector to) {
      this.to = to;
    }

    @Override
    public void transfer() {
      transferTo(to);
    }

    @Override
    public void splitAndTransfer(int startIndex, int length) {

    }

    @Override
    public ValueVector getTo() {
      return to;
    }

    @Override
    public void copyValueSafe(int from, int to) {
      this.to.copyFrom(from, to, ListVector.this);
    }
  }

  @Override
  public Accessor getAccessor() {
    return accessor;
  }

  @Override
  public Mutator getMutator() {
    return mutator;
  }

  @Override
  public FieldReader getReader() {
    return reader;
  }

  public class Accessor extends BaseRepeatedAccessor {

    @Override
    public Object getObject(int index) {
      if (bits.getAccessor().isNull(index)) {
        return null;
      }
      final List<Object> vals = new JsonStringArrayList<>();
      final UInt4Vector.Accessor offsetsAccessor = offsets.getAccessor();
      final int start = offsetsAccessor.get(index);
      final int end = offsetsAccessor.get(index + 1);
      final UnionVector.Accessor valuesAccessor = getDataVector().getAccessor();
      for(int i = start; i < end; i++) {
        vals.add(valuesAccessor.getObject(i));
      }
      return vals;
    }

    @Override
    public boolean isNull(int index) {
      return bits.getAccessor().get(index) == 0;
    }
  }

  public class Mutator extends BaseRepeatedMutator {
    public void setNotNull(int index) {
      bits.getMutator().setSafe(index, 1);
      lastSet = index + 1;
    }

    @Override
    public void startNewValue(int index) {
      for (int i = lastSet; i <= index; i++) {
        offsets.getMutator().setSafe(i + 1, offsets.getAccessor().get(i));
      }
      setNotNull(index);
      lastSet = index + 1;
    }

    @Override
    public void setValueCount(int valueCount) {
      // TODO: populate offset end points
      if (valueCount == 0) {
        offsets.getMutator().setValueCount(0);
      } else {
        for (int i = lastSet; i < valueCount; i++) {
          offsets.getMutator().setSafe(i + 1, offsets.getAccessor().get(i));
        }
        offsets.getMutator().setValueCount(valueCount + 1);
      }
      final int childValueCount = valueCount == 0 ? 0 : offsets.getAccessor().get(valueCount);
      vector.getMutator().setValueCount(childValueCount);
      bits.getMutator().setValueCount(valueCount);
    }
  }
}
