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
import org.apache.drill.exec.vector.complex.impl.BigIntHolderReaderImpl;
import org.apache.drill.exec.vector.complex.impl.EmbeddedReader;
import org.apache.drill.exec.vector.complex.reader.FieldReader;

public class EmbeddedFunctions {
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
  @FunctionTemplate(name = "castBIGINT", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls=NullHandling.NULL_IF_NULL)
  public static class CastBigIntInt implements DrillSimpleFunc{

    @Param FieldReader in;
    @Output
    BigIntHolder out;

    public void setup() {}

    public void eval() {
      out.value = org.apache.drill.exec.expr.fn.impl.EmbeddedFunctions.castBigInt(in);
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
}
