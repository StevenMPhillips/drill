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
package org.apache.drill.exec.vector.complex.fn;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonGenerator;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.expr.holders.Float4Holder;
import org.apache.drill.exec.expr.holders.NullableBigIntHolder;
import org.apache.drill.exec.expr.holders.NullableBitHolder;
import org.apache.drill.exec.expr.holders.NullableFloat4Holder;
import org.apache.drill.exec.expr.holders.NullableFloat8Holder;
import org.apache.drill.exec.expr.holders.NullableIntHolder;
import org.apache.drill.exec.expr.holders.NullableVarCharHolder;
import org.apache.drill.exec.vector.NullableBitVector;
import org.apache.drill.exec.vector.complex.reader.BaseReader.ListReader;
import org.apache.drill.exec.vector.complex.reader.FieldReader;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.ListWriter;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.MapWriter;
import org.apache.drill.exec.vector.complex.writer.FieldWriter;

import java.io.IOException;
import java.io.OutputStream;

public class ComplexCopier {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ComplexCopier.class);

  FieldReader in;
  FieldWriter out;

  public ComplexCopier(FieldReader in, FieldWriter out) {
    this.in = in;
    this.out = out;
  }

  public void write() {
    writeValue(in, out);
  }

  private void writeValue(FieldReader reader, FieldWriter writer) {
    final DataMode m = reader.getType().getMode();
    final MinorType mt = reader.getType().getMinorType();

    switch(m){
    case OPTIONAL:
    case REQUIRED:


      switch (mt) {
      case FLOAT4:
        if (reader.isSet()) {
          NullableFloat4Holder float4Holder = new NullableFloat4Holder();
          reader.read(float4Holder);
          writer.writeFloat4(float4Holder.value);
        }
        break;
      case FLOAT8:
        if (reader.isSet()) {
          NullableFloat8Holder float8Holder = new NullableFloat8Holder();
          reader.read(float8Holder);
          writer.writeFloat8(float8Holder.value);
        }
        break;
      case INT:
        if (reader.isSet()) {
          NullableIntHolder intHolder = new NullableIntHolder();
          reader.read(intHolder);
          writer.writeFloat8(intHolder.value);
        }
        break;
      case SMALLINT:
        break;
      case TINYINT:
        break;
      case BIGINT:
        if (reader.isSet()) {
          NullableBigIntHolder bigIntHolder = new NullableBigIntHolder();
          reader.read(bigIntHolder);
          writer.writeBigInt(bigIntHolder.value);
        }
        break;
      case BIT:
        if (reader.isSet()) {
          NullableBitHolder bitHolder = new NullableBitHolder();
          reader.read(bitHolder);
          writer.writeBit(bitHolder.value);
        }
        break;

      case DATE:
        break;
      case TIME:
        break;
      case TIMESTAMP:
        break;
      case INTERVALYEAR:
      case INTERVALDAY:
      case INTERVAL:
        break;
      case DECIMAL28DENSE:
      case DECIMAL28SPARSE:
      case DECIMAL38DENSE:
      case DECIMAL38SPARSE:
      case DECIMAL9:
      case DECIMAL18:
        break;

      case LIST:
        writer.startList();
        while (reader.next()) {
          writeValue(reader.reader(), getListWriterForReader(reader.reader(), writer));
        }
        writer.endList();
        break;
      case MAP:
        writer.start();
        if (reader.isSet()) {
          for(String name : reader){
            FieldReader childReader = reader.reader(name);
            if(childReader.isSet()){
              writeValue(childReader, getMapWriterForReader(childReader, writer, name));
            }
          }
        }
        writer.end();
        break;
      case NULL:
      case LATE:
        break;

      case VAR16CHAR:
        break;
      case VARBINARY:
        break;
      case VARCHAR:
        if (reader.isSet()) {
          NullableVarCharHolder varCharHolder = new NullableVarCharHolder();
          reader.read(varCharHolder);
          writer.writeVarChar(varCharHolder.start, varCharHolder.end, varCharHolder.buffer);
        }
        break;

      }
      break;

    }

  }

  private FieldWriter getMapWriterForReader(FieldReader reader, MapWriter writer, String name) {
    switch (reader.getType().getMinorType()) {
    case INT:
      return (FieldWriter) writer.integer(name);
    case BIGINT:
      return (FieldWriter) writer.bigInt(name);
    case VARCHAR:
      return (FieldWriter) writer.varChar(name);
    case FLOAT4:
      return (FieldWriter) writer.float4(name);
    case FLOAT8:
      return (FieldWriter) writer.float8(name);
    case MAP:
      return (FieldWriter) writer.map(name);
    case LIST:
      return (FieldWriter) writer.list(name);
    default:
      throw new UnsupportedOperationException(reader.getType().toString());
    }
  }

  private FieldWriter getListWriterForReader(FieldReader reader, ListWriter writer) {
    switch (reader.getType().getMinorType()) {
    case INT:
      return (FieldWriter) writer.integer();
    case BIGINT:
      return (FieldWriter) writer.bigInt();
    case VARCHAR:
      return (FieldWriter) writer.varChar();
    case FLOAT4:
      return (FieldWriter) writer.float4();
    case FLOAT8:
      return (FieldWriter) writer.float8();
    case MAP:
      return (FieldWriter) writer.map();
    case LIST:
      return (FieldWriter) writer.list();
    default:
      throw new UnsupportedOperationException(reader.getType().toString());
    }
  }
}
