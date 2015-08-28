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
package org.apache.drill.exec.vector.complex.impl;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.netty.buffer.DrillBuf;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.expr.holders.BigIntHolder;
import org.apache.drill.exec.expr.holders.IntHolder;
import org.apache.drill.exec.expr.holders.NullableIntHolder;
import org.apache.drill.exec.expr.holders.NullableVarCharHolder;
import org.apache.drill.exec.expr.holders.VarCharHolder;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.memory.TopLevelAllocator;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.vector.UInt4Vector;
import org.apache.drill.exec.vector.complex.EmbeddedVector;
import org.apache.drill.exec.vector.complex.reader.FieldReader;
import org.apache.drill.exec.vector.complex.writer.BaseWriter;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.ComplexWriter;
import org.apache.drill.exec.vector.complex.writer.BigIntWriter;
import org.apache.drill.exec.vector.complex.writer.BitWriter;
import org.apache.drill.exec.vector.complex.writer.FieldWriter;
import org.apache.drill.exec.vector.complex.writer.IntWriter;
import org.apache.drill.exec.vector.complex.writer.VarCharWriter;
import org.msgpack.core.MessagePacker;
import org.msgpack.core.buffer.MessageBufferOutput;
import org.msgpack.core.buffer.OutputStreamBufferOutput;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class EmbeddedWriter extends AbstractFieldWriter implements ComplexWriter {

  private EmbeddedVector data;
  private MapWriter mapWriter;
  private EmbeddedListWriter listWriter;
  private BigIntWriter bigIntWriter;
  private VarCharWriter varCharWriter;
  private List<BaseWriter> writers = Lists.newArrayList();

  private MinorType type;
  private BitWriter bitWriter;

  public EmbeddedWriter(BufferAllocator allocator) {
    super(null);
    data = new EmbeddedVector(MaterializedField.create("root", Types.required(MinorType.EMBEDDED)), allocator);
  }

  public EmbeddedWriter(EmbeddedVector vector) {
    super(null);
    data = vector;
  }

  private MapWriter getMapWriter(boolean create) {
    if (create && mapWriter == null) {
      mapWriter = new SingleMapWriter(data.getMap(), null);
      writers.add(mapWriter);
    }
    return mapWriter;
  }

  private ListWriter getListWriter(boolean create) {
    if (create && listWriter == null) {
      listWriter = new EmbeddedListWriter(data.getList());
      writers.add(listWriter);
    }
    return listWriter;
  }

  private BitWriter getBitWriter(boolean create) {
    if (create && bitWriter == null) {
      bitWriter = new NullableBitWriterImpl(data.getBit(), null);
      writers.add(bitWriter);
    }
    return bitWriter;
  }

  private BigIntWriter getBigIntWriter(boolean create) {
    if (create && bigIntWriter == null) {
      bigIntWriter = new NullableBigIntWriterImpl(data.getBigInt(), null);
      writers.add(bigIntWriter);
    }
    return bigIntWriter;
  }

  private VarCharWriter getVarCharWriter(boolean create) {
    if (create && varCharWriter == null) {
      varCharWriter = new NullableVarCharWriterImpl(data.getVarChar(), null);
      writers.add(varCharWriter);
    }
    return varCharWriter;
  }

  @Override
  public void allocate() {
    data.allocateNew();
  }

  @Override
  public void clear() {

  }

  @Override
  public void setPosition(int index) {
    super.setPosition(index);
    for (BaseWriter writer : writers) {
      writer.setPosition(index);
    }
  }

  @Override
  public MapWriter rootAsMap() {
    data.getMutator().setType(idx(), MinorType.MAP);
    return this;
  }

  public MapWriter asMap() {
    data.getMutator().setType(idx(), MinorType.MAP);
    return getMapWriter(true);
  }

  public ListWriter asList() {
    data.getMutator().setType(idx(), MinorType.LIST);
    return getListWriter(true);
  }

  public BigIntWriter asBigInt() {
    data.getMutator().setType(idx(), MinorType.BIGINT);
    return getBigIntWriter(true);
  }

  public VarCharWriter asVarChar() {
    data.getMutator().setType(idx(), MinorType.VARCHAR);
    return getVarCharWriter(true);
  }

  public BitWriter asBit() {
    data.getMutator().setType(idx(), MinorType.VARCHAR);
    return getBitWriter(true);
  }

  @Override
  public ListWriter rootAsList() {
    return null;
  }

  @Override
  public void setValueCount(int count) {
    data.getMutator().setValueCount(count);
  }

  @Override
  public void reset() {
  }

  @Override
  public void startList() {
    getListWriter(true).startList();
    data.getMutator().setType(idx(), MinorType.LIST);
  }

  @Override
  public void endList() {
    getListWriter(true).endList();
  }

  @Override
  public void start() {
    data.getMutator().setType(idx(), MinorType.MAP);
  }

  @Override
  public void end() {
  }

  @Override
  public BigIntWriter bigInt() {
    data.getMutator().setType(idx(), MinorType.LIST);
    getListWriter(true).setPosition(idx());
    return getListWriter(true).bigInt();
  }

  @Override
  public BitWriter bit() {
    data.getMutator().setType(idx(), MinorType.LIST);
    getListWriter(true).setPosition(idx());
    return getListWriter(true).bit();
  }

  @Override
  public VarCharWriter varChar() {
    data.getMutator().setType(idx(), MinorType.LIST);
    getListWriter(true).setPosition(idx());
    return getListWriter(true).varChar();
  }

  @Override
  public MapWriter map() {
    data.getMutator().setType(idx(), MinorType.LIST);
    getListWriter(true).setPosition(idx());
    return getListWriter(true).map();
  }

  @Override
  public ListWriter list() {
    data.getMutator().setType(idx(), MinorType.LIST);
    getListWriter(true).setPosition(idx());
    return getListWriter(true).list();
  }

  @Override
  public ListWriter list(String name) {
    data.getMutator().setType(idx(), MinorType.MAP);
    getMapWriter(true).setPosition(idx());
    return getMapWriter(true).list(name);
  }

  @Override
  public MapWriter map(String name) {
    data.getMutator().setType(idx(), MinorType.MAP);
    getMapWriter(true).setPosition(idx());
    return getMapWriter(true).map(name);
  }

  @Override
  public BigIntWriter bigInt(String name) {
    data.getMutator().setType(idx(), MinorType.MAP);
    getMapWriter(true).setPosition(idx());
    return getMapWriter(true).bigInt(name);
  }

  @Override
  public BitWriter bit(String name) {
    data.getMutator().setType(idx(), MinorType.MAP);
    getMapWriter(true).setPosition(idx());
    return getMapWriter(true).bit(name);
  }
  @Override
  public VarCharWriter varChar(String name) {
    data.getMutator().setType(idx(), MinorType.MAP);
    getMapWriter(true).setPosition(idx());
    return getMapWriter(true).varChar(name);
  }

  @Override
  public void writeBigInt(long value) {
    data.getMutator().setType(idx(), MinorType.BIGINT);
    getBigIntWriter(true).setPosition(idx());
    getBigIntWriter(true).writeBigInt(value);
  }

  @Override
  public void writeBit(int value) {
    data.getMutator().setType(idx(), MinorType.BIT);
    getBitWriter(true).setPosition(idx());
    getBitWriter(true).writeBit(value);
  }

  @Override
  public void writeVarChar(int start, int end, DrillBuf buf) {
    data.getMutator().setType(idx(), MinorType.VARCHAR);
    getVarCharWriter(true).setPosition(idx());
    getVarCharWriter(true).writeVarChar(start, end, buf);
  }

  @Override
  public MaterializedField getField() {
    return null;
  }

  @Override
  public int getValueCapacity() {
    return 0;
  }

  @Override
  public void close() throws Exception {

  }

  public EmbeddedVector getData() {
    return data;
  }


}
