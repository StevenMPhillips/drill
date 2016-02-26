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
<@pp.dropOutputFile />

<#list drillOI.map as entry>
<@pp.changeOutputFile name="/org/apache/drill/exec/expr/fn/impl/hive/Drill${entry.drillType}ObjectInspector.java" />

<#include "/@includes/license.ftl" />

package org.apache.drill.exec.expr.fn.impl.hive;

import org.apache.arrow.vector.util.DecimalUtility;
import org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers;
import org.apache.arrow.vector.holders.*;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.HiveVarcharWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.*;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class Drill${entry.drillType}ObjectInspector {
<#assign seq = ["Required", "Optional"]>
<#list seq as mode>

  public static class ${mode} extends AbstractDrillPrimitiveObjectInspector implements ${entry.hiveOI} {
    public ${mode}() {
      super(TypeInfoFactory.${entry.hiveType?lower_case}TypeInfo);
    }

<#if entry.drillType == "VarChar">
    @Override
    public HiveVarcharWritable getPrimitiveWritableObject(Object o) {
    <#if mode == "Optional">
      if (o == null) {
        return null;
      }
      final NullableVarCharHolder h = (NullableVarCharHolder)o;
    <#else>
      final VarCharHolder h = (VarCharHolder)o;
    </#if>
      final HiveVarcharWritable valW = new HiveVarcharWritable();
      valW.set(StringFunctionHelpers.toStringFromUTF8(h.start, h.end, h.buffer), HiveVarchar.MAX_VARCHAR_LENGTH);
      return valW;
    }

    @Override
    public HiveVarchar getPrimitiveJavaObject(Object o) {
    <#if mode == "Optional">
      if (o == null) {
        return null;
      }
      final NullableVarCharHolder h = (NullableVarCharHolder)o;
    <#else>
      final VarCharHolder h = (VarCharHolder)o;
    </#if>
      final String s = StringFunctionHelpers.toStringFromUTF8(h.start, h.end, h.buffer);
      return new HiveVarchar(s, HiveVarchar.MAX_VARCHAR_LENGTH);
    }
<#elseif entry.drillType == "Var16Char">
    @Override
    public Text getPrimitiveWritableObject(Object o) {
    <#if mode == "Optional">
      if (o == null) {
        return null;
      }
      final NullableVar16CharHolder h = (NullableVar16CharHolder)o;
    <#else>
      final Var16CharHolder h = (Var16CharHolder)o;
    </#if>
      return new Text(StringFunctionHelpers.toStringFromUTF16(h.start, h.end, h.buffer));
    }

    @Override
    public String getPrimitiveJavaObject(Object o){
    <#if mode == "Optional">
      if (o == null) {
        return null;
      }
      final NullableVar16CharHolder h = (NullableVar16CharHolder)o;
    <#else>
      final Var16CharHolder h = (Var16CharHolder)o;
    </#if>
      return StringFunctionHelpers.toStringFromUTF16(h.start, h.end, h.buffer);
    }
<#elseif entry.drillType == "VarBinary">
    @Override
    public BytesWritable getPrimitiveWritableObject(Object o) {
    <#if mode == "Optional">
      if (o == null) {
        return null;
      }
      final NullableVarBinaryHolder h = (NullableVarBinaryHolder)o;
    <#else>
      final VarBinaryHolder h = (VarBinaryHolder)o;
    </#if>
      final byte[] buf = new byte[h.end-h.start];
      h.buffer.getBytes(h.start, buf, 0, h.end-h.start);
      return new BytesWritable(buf);
    }

    @Override
    public byte[] getPrimitiveJavaObject(Object o) {
    <#if mode == "Optional">
      if (o == null) {
        return null;
      }
      final NullableVarBinaryHolder h = (NullableVarBinaryHolder)o;
    <#else>
      final VarBinaryHolder h = (VarBinaryHolder)o;
    </#if>
      final byte[] buf = new byte[h.end-h.start];
      h.buffer.getBytes(h.start, buf, 0, h.end-h.start);
      return buf;
    }
<#elseif entry.drillType == "Bit">
    @Override
    public boolean get(Object o) {
    <#if mode == "Optional">
      return ((NullableBitHolder)o).value == 0 ? false : true;
    <#else>
      return ((BitHolder)o).value == 0 ? false : true;
    </#if>
    }

    @Override
    public BooleanWritable getPrimitiveWritableObject(Object o) {
    <#if mode == "Optional">
      if (o == null) {
        return null;
      }
      return new BooleanWritable(((NullableBitHolder)o).value == 0 ? false : true);
    <#else>
      return new BooleanWritable(((BitHolder)o).value == 0 ? false : true);
    </#if>
    }

    @Override
    public Boolean getPrimitiveJavaObject(Object o) {
    <#if mode == "Optional">
      if (o == null) {
        return null;
      }
      return new Boolean(((NullableBitHolder)o).value == 0 ? false : true);
    <#else>
      return new Boolean(((BitHolder)o).value == 0 ? false : true);
    </#if>
    }
<#elseif entry.drillType == "Decimal38Sparse">
    @Override
    public HiveDecimal getPrimitiveJavaObject(Object o){
    <#if mode == "Optional">
      if (o == null) {
        return null;
      }
      final NullableDecimal38SparseHolder h = (NullableDecimal38SparseHolder) o;
    <#else>
      final Decimal38SparseHolder h = (Decimal38SparseHolder) o;
    </#if>
      return HiveDecimal.create(DecimalUtility.getBigDecimalFromSparse(h.buffer, h.start, h.nDecimalDigits, h.scale));
    }

    @Override
    public HiveDecimalWritable getPrimitiveWritableObject(Object o) {
    <#if mode == "Optional">
      if (o == null) {
        return null;
      }
      final NullableDecimal38SparseHolder h = (NullableDecimal38SparseHolder) o;
    <#else>
      final Decimal38SparseHolder h = (Decimal38SparseHolder) o;
    </#if>
      return new HiveDecimalWritable(
          HiveDecimal.create(DecimalUtility.getBigDecimalFromSparse(h.buffer, h.start, h.nDecimalDigits, h.scale)));
    }

<#elseif entry.drillType == "TimeStamp">
    @Override
    public java.sql.Timestamp getPrimitiveJavaObject(Object o){
    <#if mode == "Optional">
      if (o == null) {
        return null;
      }
      final NullableTimeStampHolder h = (NullableTimeStampHolder) o;
    <#else>
      final TimeStampHolder h = (TimeStampHolder) o;
    </#if>
      return new java.sql.Timestamp(h.value);
    }

    @Override
    public TimestampWritable getPrimitiveWritableObject(Object o) {
    <#if mode == "Optional">
      if (o == null) {
        return null;
      }
      final NullableTimeStampHolder h = (NullableTimeStampHolder) o;
    <#else>
      final TimeStampHolder h = (TimeStampHolder) o;
    </#if>
      return new TimestampWritable(new java.sql.Timestamp(h.value));
    }

<#elseif entry.drillType == "Date">
    @Override
    public java.sql.Date getPrimitiveJavaObject(Object o){
    <#if mode == "Optional">
      if (o == null) {
        return null;
      }
      final NullableDateHolder h = (NullableDateHolder) o;
    <#else>
      final DateHolder h = (DateHolder) o;
    </#if>
      return new java.sql.Date(h.value);
    }

    @Override
    public DateWritable getPrimitiveWritableObject(Object o) {
    <#if mode == "Optional">
      if (o == null) {
        return null;
      }
      final NullableDateHolder h = (NullableDateHolder) o;
    <#else>
      final DateHolder h = (DateHolder) o;
    </#if>
      return new DateWritable(new java.sql.Date(h.value));
    }

<#else>
    @Override
    public ${entry.javaType} get(Object o){
    <#if mode == "Optional">
      return ((Nullable${entry.drillType}Holder)o).value;
    <#else>
      return ((${entry.drillType}Holder)o).value;
    </#if>
    }

<#if entry.drillType == "Int">
    @Override
    public Integer getPrimitiveJavaObject(Object o) {
    <#if mode == "Optional">
      if (o == null) {
        return null;
      }
    </#if>
      return new Integer(get(o));
    }
<#else>
    @Override
    public ${entry.javaType?cap_first} getPrimitiveJavaObject(Object o) {
    <#if mode == "Optional">
      if (o == null) {
        return null;
      }
      return new ${entry.javaType?cap_first}(((Nullable${entry.drillType}Holder)o).value);
    <#else>
      return new ${entry.javaType?cap_first}(((${entry.drillType}Holder)o).value);
    </#if>
    }
</#if>

    @Override
    public ${entry.javaType?cap_first}Writable getPrimitiveWritableObject(Object o) {
    <#if mode == "Optional">
      if (o == null) {
        return null;
      }
      final Nullable${entry.drillType}Holder h = (Nullable${entry.drillType}Holder) o;
    <#else>
      final ${entry.drillType}Holder h = (${entry.drillType}Holder)o;
    </#if>
      return new ${entry.javaType?cap_first}Writable(h.value);
    }
</#if>
  }
</#list>
}

</#list>

