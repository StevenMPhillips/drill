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
package org.apache.drill.exec.vector.complex.impl;

import io.netty.buffer.DrillBuf;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.vector.UInt4Vector;
import org.apache.drill.exec.vector.complex.EmbeddedVector;
import org.apache.drill.exec.vector.complex.ListVector;
import org.apache.drill.exec.vector.complex.writer.BigIntWriter;
import org.apache.drill.exec.vector.complex.writer.VarCharWriter;

public class EmbeddedListWriter extends AbstractFieldWriter {

  ListVector vector;
  EmbeddedVector data;
  UInt4Vector offsets;
  private EmbeddedWriter writer;
  private boolean inMap = false;
  private String mapName;
  private int lastIndex = 0;

  public EmbeddedListWriter(ListVector vector) {
    super(null);
    this.vector = vector;
    this.data = (EmbeddedVector) vector.getDataVector();
    this.writer = new EmbeddedWriter(data);
    this.offsets = vector.getOffsetVector();
  }

  @Override
  public void allocate() {
    vector.allocateNew();
  }

  @Override
  public void clear() {
    vector.clear();
  }

  @Override
  public MaterializedField getField() {
    return null;
  }

  @Override
  public int getValueCapacity() {
    return vector.getValueCapacity();
  }

  @Override
  public void close() throws Exception {

  }

  @Override
  public BigIntWriter bigInt() {
    return this;
  }

  @Override
  public BigIntWriter bigInt(String name) {
    assert inMap;
    mapName = name;
    return this;
  }

  @Override
  public VarCharWriter varChar() {
    return this;
  }

  @Override
  public VarCharWriter varChar(String name) {
    assert inMap;
    mapName = name;
    return this;
  }

  @Override
  public MapWriter map() {
    inMap = true;
    return this;
  }

  @Override
  public ListWriter list() {
    final int nextOffset = offsets.getAccessor().get(idx() + 1);
    data.getMutator().setType(nextOffset, MinorType.MAP);
    offsets.getMutator().setSafe(idx() + 1, nextOffset + 1);
    writer.setPosition(nextOffset);
    return writer;
  }

  @Override
  public MapWriter map(String name) {
    MapWriter mapWriter = writer.map(name);
    return mapWriter;
  }

  @Override
  public void startList() {
//    for (int i = lastIndex + 1; i <= idx(); i++) {
      vector.getMutator().startNewValue(idx());
//    }
//    lastIndex = idx();
  }

  @Override
  public void endList() {

  }

  @Override
  public void start() {
    assert inMap;
    final int nextOffset = offsets.getAccessor().get(idx() + 1);
    data.getMutator().setType(nextOffset, MinorType.MAP);
    offsets.getMutator().setSafe(idx() + 1, nextOffset + 1);
    writer.setPosition(nextOffset);
  }

  @Override
  public void end() {
    if (inMap) {
      inMap = false;
    }
  }

  @Override
  public void writeBigInt(long value) {
    if (inMap) {
      final int nextOffset = offsets.getAccessor().get(idx() + 1);
      data.getMutator().setType(nextOffset, MinorType.MAP);
      writer.setPosition(nextOffset);
      BigIntWriter bigIntWriter = writer.bigInt(mapName);
      bigIntWriter.writeBigInt(value);
      offsets.getMutator().setSafe(idx() + 1, nextOffset + 1);
    } else {
      vector.getMutator().addSafe(idx(), value);
    }
  }

  @Override
  public void writeVarChar(int start, int end, DrillBuf buf) {
    if (inMap) {
      VarCharWriter varCharWriter = writer.varChar(mapName);
      final int nextOffset = offsets.getAccessor().get(idx() + 1);
      data.getMutator().setType(nextOffset, MinorType.MAP);
      varCharWriter.setPosition(nextOffset);
      varCharWriter.writeVarChar(start, end, buf);
      offsets.getMutator().setSafe(idx() + 1, nextOffset + 1);
    } else {
      vector.getMutator().addSafe(idx(), start, end, buf);
    }
  }

}
