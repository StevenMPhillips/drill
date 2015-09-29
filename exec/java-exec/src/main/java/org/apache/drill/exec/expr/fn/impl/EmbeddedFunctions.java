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
package org.apache.drill.exec.expr.fn.impl;

import io.netty.buffer.DrillBuf;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.FunctionScope;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.NullHandling;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.holders.BigIntHolder;
import org.apache.drill.exec.expr.holders.BitHolder;
import org.apache.drill.exec.expr.holders.EmbeddedHolder;
import org.apache.drill.exec.expr.holders.IntHolder;
import org.apache.drill.exec.expr.holders.NullableBigIntHolder;
import org.apache.drill.exec.expr.holders.VarCharHolder;
import org.apache.drill.exec.vector.ValueHolderHelper;
import org.apache.drill.exec.vector.complex.impl.BigIntHolderReaderImpl;
import org.apache.drill.exec.vector.complex.impl.EmbeddedReader;
import org.apache.drill.exec.vector.complex.reader.BigIntReader;
import org.apache.drill.exec.vector.complex.reader.FieldReader;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.ComplexWriter;

import javax.inject.Inject;

public class EmbeddedFunctions {
  /*
  @FunctionTemplate(names = {"equal", "==", "="},
          scope = FunctionTemplate.FunctionScope.SIMPLE,
          nulls = NullHandling.NULL_IF_NULL)
  public static class EqualsEmbeddedVsBigInt implements DrillSimpleFunc {

    @Param
    FieldReader left;
    @Param BigIntHolder right;
    @Output
    BitHolder out;

    public void setup() {}

    public void eval() {

      out.value = org.apache.drill.exec.expr.fn.impl.EmbeddedFunctions.equalsBigInt(left, right.value);

    }
  }

  public static int equalsBigInt(FieldReader reader, long value) {
    BigIntHolder right = new BigIntHolder();
    right.value = value;

    BitHolder out = new BitHolder();

    MinorType type = reader.getType().getMinorType();

    switch(type) {
    case BIGINT: {
      BigIntHolder left = new BigIntHolder();
      left.value = reader.readLong();
      out.value = reader.readLong() == right.value ? 1 : 0;
      break;
    }
    case VARCHAR: {
      BigIntHolder left = new BigIntHolder();
      left.value = Long.parseLong(reader.readString());
      out.value = left.value == right.value ? 1 : 0;
    }
    }
    return out.value;
  }

  @FunctionTemplate(name = "add", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class EmbeddedBigIntAdd implements DrillSimpleFunc {

    @Param FieldReader in1;
    @Param BigIntHolder in2;
    @Output BigIntHolder out;

    public void setup() {
    }

    public void eval() {

    }
  }

  @FunctionTemplate(names = {"equal", "==", "="},
          scope = FunctionTemplate.FunctionScope.SIMPLE,
          nulls = NullHandling.NULL_IF_NULL)
  public static class EqualsEmbeddedVsInt implements DrillSimpleFunc {

    @Param
    FieldReader left;
    @Param
    IntHolder right;
    @Output
    BitHolder out;

    public void setup() {}

    public void eval() {

      out.value = org.apache.drill.exec.expr.fn.impl.EmbeddedFunctions.equalsInt(left, right.value);

    }
  }

  public static int equalsInt(FieldReader reader, int value) {
    IntHolder right = new IntHolder();
    right.value = value;

    BitHolder out = new BitHolder();

    MinorType type = reader.getType().getMinorType();

    switch(type) {
    case BIGINT: {
      BigIntHolder left = new BigIntHolder();
      left.value = reader.readLong();
      out.value = left.value == right.value ? 1 : 0;
      break;
    }
    case VARCHAR: {
      IntHolder left = new IntHolder();
      left.value = Integer.parseInt(reader.readString());
      out.value = left.value == right.value ? 1 : 0;
    }
    }
    return out.value;
  }

  @FunctionTemplate(names = {"isBigInt"},
          scope = FunctionTemplate.FunctionScope.SIMPLE,
          nulls = NullHandling.NULL_IF_NULL)
  public static class IsBigInt implements DrillSimpleFunc {

    @Param
    FieldReader input;
    @Output
    BitHolder out;

    public void setup() {}

    public void eval() {

      out.value = input.getType().getMinorType() == org.apache.drill.common.types.TypeProtos.MinorType.BIGINT ? 1 : 0;

    }
  }
  */

  @FunctionTemplate(names = {"fromType"},
          scope = FunctionTemplate.FunctionScope.SIMPLE,
          nulls = NullHandling.NULL_IF_NULL)
  public static class FromType implements DrillSimpleFunc {

    @Param
    IntHolder in;
    @Output
    VarCharHolder out;
    @Inject
    DrillBuf buffer;

    public void setup() {}

    public void eval() {

      VarCharHolder h = ValueHolderHelper.getVarCharHolder(buffer, org.apache.drill.common.types.MinorType.valueOf(in.value).toString());
      out.buffer = h.buffer;
      out.start = h.start;
      out.end = h.end;
    }
  }

  @FunctionTemplate(names = {"toType"},
          scope = FunctionTemplate.FunctionScope.SIMPLE,
          nulls = NullHandling.NULL_IF_NULL)
  public static class ToType implements DrillSimpleFunc {

    @Param
    VarCharHolder input;
    @Output
    IntHolder out;

    public void setup() {}

    public void eval() {

      out.value = input.getType().getMinorType().getNumber();
      byte[] b = new byte[input.end - input.start];
      input.buffer.getBytes(input.start, b, 0, b.length);
      String type = new String(b);
      out.value = org.apache.drill.common.types.MinorType.valueOf(type).getNumber();
    }
  }

  @FunctionTemplate(names = {"typeOf"},
          scope = FunctionTemplate.FunctionScope.SIMPLE,
          nulls = NullHandling.NULL_IF_NULL)
  public static class GetType implements DrillSimpleFunc {

    @Param
    FieldReader input;
    @Output
    IntHolder out;

    public void setup() {}

    public void eval() {

      out.value = input.getType().getMinorType().getNumber();

    }
  }

  /*
  @FunctionTemplate(names = {"isBigInt"},
          scope = FunctionTemplate.FunctionScope.SIMPLE,
          nulls = NullHandling.NULL_IF_NULL)
  public static class IsBigIntBigInt implements DrillSimpleFunc {

    @Param
    BigIntHolder input;
    @Output
    BitHolder out;

    public void setup() {}

    public void eval() {

      out.value = 1;

    }
  }
  */

  @SuppressWarnings("unused")
  @FunctionTemplate(name = "asBigInt", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls=NullHandling.NULL_IF_NULL)
  public static class CastEmbeddedBigInt implements DrillSimpleFunc{

    @Param EmbeddedHolder in;
    @Output
    BigIntHolder out;

    public void setup() {}

    public void eval() {
      in.reader.read(out);
    }
  }

  @SuppressWarnings("unused")
  @FunctionTemplate(name = "asVarChar", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls=NullHandling.NULL_IF_NULL)
  public static class CastEmbeddedVarChar implements DrillSimpleFunc{

    @Param EmbeddedHolder in;
    @Output
    VarCharHolder out;

    public void setup() {}

    public void eval() {
      in.reader.read(out);
    }
  }

  public static long castBigInt(FieldReader reader) {
    MinorType type = reader.getType().getMinorType();
    switch(type) {
    case VARCHAR:
      return Long.parseLong(reader.readText().toString());
    case BIGINT:
      return reader.readLong();
    default:
      return 0;
    }
  }

  @SuppressWarnings("unused")
  @FunctionTemplate(names = {"castEMBEDDED", "castToEmbedded"}, scope = FunctionTemplate.FunctionScope.SIMPLE, nulls=NullHandling.NULL_IF_NULL)
  public static class CastVarCharToEmbedded implements DrillSimpleFunc{

    @Param VarCharHolder in;
    @Output EmbeddedHolder out;

    public void setup() {}

    public void eval() {
      out.reader = new org.apache.drill.exec.vector.complex.impl.VarCharHolderReaderImpl(in);
      out.isSet = 1;
    }
  }

  @SuppressWarnings("unused")
  @FunctionTemplate(names = {"castEMBEDDED", "castToEmbedded"}, scope = FunctionTemplate.FunctionScope.SIMPLE, nulls=NullHandling.NULL_IF_NULL)
  public static class CastBigIntToEmbedded implements DrillSimpleFunc{

    @Param NullableBigIntHolder in;
    @Output EmbeddedHolder out;

    public void setup() {}

    public void eval() {
      out.reader = new org.apache.drill.exec.vector.complex.impl.NullableBigIntHolderReaderImpl(in);
      out.isSet = 1;
    }
  }

  @SuppressWarnings("unused")
  @FunctionTemplate(names = {"castEMBEDDED", "castToEmbedded"}, scope = FunctionTemplate.FunctionScope.SIMPLE, nulls=NullHandling.NULL_IF_NULL)
  public static class CastIntToEmbedded implements DrillSimpleFunc{

    @Param IntHolder in;
    @Output EmbeddedHolder out;

    public void setup() {}

    public void eval() {
      out.reader = new org.apache.drill.exec.vector.complex.impl.IntHolderReaderImpl(in);
      out.isSet = 1;
    }
  }

  @SuppressWarnings("unused")
  @FunctionTemplate(names = {"castEMBEDDED", "castToEmbedded"}, scope = FunctionTemplate.FunctionScope.SIMPLE, nulls=NullHandling.NULL_IF_NULL)
  public static class CastEmbeddedToEmbedded implements DrillSimpleFunc{

    @Param FieldReader in;
    @Output EmbeddedHolder out;

    public void setup() {}

    public void eval() {
      out.reader = in;
      out.isSet = in.isSet() ? 1 : 0;
    }
  }
}
