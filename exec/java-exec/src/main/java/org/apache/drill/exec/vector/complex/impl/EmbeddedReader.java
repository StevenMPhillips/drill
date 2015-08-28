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
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.expr.holders.BigIntHolder;
import org.apache.drill.exec.expr.holders.ComplexHolder;
import org.apache.drill.exec.expr.holders.EmbeddedHolder;
import org.apache.drill.exec.expr.holders.IntHolder;
import org.apache.drill.exec.expr.holders.NullableBigIntHolder;
import org.apache.drill.exec.expr.holders.NullableVarCharHolder;
import org.apache.drill.exec.vector.BigIntVector;
import org.apache.drill.exec.vector.VarCharVector;
import org.apache.drill.exec.vector.complex.EmbeddedVector;
import org.apache.drill.exec.vector.complex.MapVector;
import org.apache.drill.exec.vector.complex.reader.BaseReader.ComplexReader;
import org.apache.drill.exec.vector.complex.reader.BigIntReader;
import org.apache.drill.exec.vector.complex.reader.FieldReader;
import org.apache.drill.exec.vector.complex.reader.VarCharReader;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.MapWriter;
import org.apache.drill.exec.vector.complex.writer.BigIntWriter;
import org.apache.drill.exec.vector.complex.writer.VarCharWriter;
import org.apache.hadoop.io.Text;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.core.buffer.InputStreamBufferInput;
import org.msgpack.core.buffer.MessageBufferInput;
import org.msgpack.value.ImmutableValue;
import org.msgpack.value.Value;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class EmbeddedReader extends AbstractFieldReader {

  public EmbeddedVector data;
  private SingleMapReaderImpl mapReader;
  private NullableBigIntReaderImpl bigIntReader;
  private final NullableBitReaderImpl bitReader;
  private VarCharReader varCharReader;

  public EmbeddedReader(EmbeddedVector data) {
    this.data = data;
    this.mapReader = (SingleMapReaderImpl) data.getMap().getReader();
    this.bigIntReader = new NullableBigIntReaderImpl(data.getBigInt());
    this.bitReader = new NullableBitReaderImpl(data.getBit());
    this.varCharReader = new NullableVarCharReaderImpl(data.getVarChar());
  }

  @Override
  public MajorType getType() {
    return Types.required(MinorType.valueOf(data.getTypeValue(idx())));
  }

  @Override
  public boolean isSet() {
    return false;
  }

  @Override
  public Long readLong() {
    bigIntReader.setPosition(idx());
    return bigIntReader.readLong();
  }

  @Override
  public Boolean readBoolean() {
    bitReader.setPosition(idx());
    return bitReader.readBoolean();
  }

  @Override
  public void read(NullableBigIntHolder holder) {
    bigIntReader.read(holder);
  }

  @Override
  public void read(int arrayIndex, NullableBigIntHolder holder) {
    bigIntReader.read(holder);
  }

  @Override
  public void copyAsValue(BigIntWriter writer) {
    bigIntReader.copyAsValue(writer);
  }

  @Override
  public void copyAsValue(VarCharWriter writer) {
    varCharReader.copyAsValue(writer);
  }

  @Override
  public void copyAsValue(MapWriter writer) {
    mapReader.copyAsValue(writer);
  }

  @Override
  public Object readObject() {
    return data.getAccessor().getObject(idx());
  }

  @Override
  public void reset() {

  }

  @Override
  public void setPosition(int index) {
    super.setPosition(index);
    bigIntReader.setPosition(index);
    varCharReader.setPosition(index);
    mapReader.setPosition(index);
    bitReader.setPosition(index);
  }

  @Override
  public void read(EmbeddedHolder holder) {
    holder.isSet = 1;
    holder.reader = this;
  }

  @Override
  public FieldReader reader(String name) {
    mapReader.setPosition(idx());
    return mapReader.reader(name);
  }

  @Override
  public Text readText() {
    varCharReader.setPosition(idx());
    return varCharReader.readText();
  }
}
