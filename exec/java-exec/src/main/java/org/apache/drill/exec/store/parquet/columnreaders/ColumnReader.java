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
package org.apache.drill.exec.store.parquet.columnreaders;

import io.netty.buffer.DrillBuf;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.vector.BaseDataValueVector;
import org.apache.drill.exec.vector.ValueVector;

import org.apache.drill.exec.vector.VarBinaryVector;
import org.apache.drill.exec.vector.VarCharVector;
import org.apache.drill.exec.vector.VarCharVector.Mutator;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.format.SchemaElement;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;

public abstract class ColumnReader<V extends ValueVector> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ColumnReader.class);

  final ParquetRecordReader parentReader;

  // Value Vector for this column
  final V valueVec;

  ColumnDescriptor getColumnDescriptor() {
    return columnDescriptor;
  }

  // column description from the parquet library
  final ColumnDescriptor columnDescriptor;
  // metadata of the column, from the parquet library
  final ColumnChunkMetaData columnChunkMetaData;
  // status information on the current page
  PageReader pageReader;

  final SchemaElement schemaElement;
  boolean usingDictionary;

  // quick reference to see if the field is fixed length (as this requires an instanceof)
  final boolean isFixedLength;

  // counter for the total number of values read from one or more pages
  // when a batch is filled all of these values should be the same for all of the columns
  int totalValuesRead;

  // counter for the values that have been read in this pass (a single call to the next() method)
  int valuesReadInCurrentPass;

  // length of single data value in bits, if the length is fixed
  int dataTypeLengthInBits;
  int bytesReadInCurrentPass;

  protected DrillBuf vectorData;
  // when reading definition levels for nullable columns, it is a one-way stream of integers
  // when reading var length data, where we don't know if all of the records will fit until we've read all of them
  // we must store the last definition level an use it in at the start of the next batch
  int currDefLevel;

  // variables for a single read pass
  long readStartInBytes = 0, readLength = 0, readLengthInBits = 0, recordsReadInThisIteration = 0;

  protected ColumnReader(ParquetRecordReader parentReader, int allocateSize, ColumnDescriptor descriptor,
      ColumnChunkMetaData columnChunkMetaData, boolean fixedLength, V v, SchemaElement schemaElement) throws ExecutionSetupException {
    this.parentReader = parentReader;
    this.columnDescriptor = descriptor;
    this.columnChunkMetaData = columnChunkMetaData;
    this.isFixedLength = fixedLength;
    this.schemaElement = schemaElement;
    this.valueVec =  v;
    this.pageReader = new PageReader(this, parentReader.getFileSystem(), parentReader.getHadoopPath(), columnChunkMetaData);

    if (columnDescriptor.getType() != PrimitiveType.PrimitiveTypeName.BINARY) {
      if (columnDescriptor.getType() == PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY) {
        dataTypeLengthInBits = columnDescriptor.getTypeLength() * 8;
      } else {
        dataTypeLengthInBits = ParquetRecordReader.getTypeLengthInBits(columnDescriptor.getType());
      }
    }

  }

  public int getRecordsReadInCurrentPass() {
    return valuesReadInCurrentPass;
  }

  public abstract void processPages(long recordsToReadInThisPass) throws IOException;

  public void clear() {
    valueVec.clear();
    pageReader.clear();
  }

  protected abstract void readField(long recordsToRead);

  public void reset() {
    readStartInBytes = 0;
    readLength = 0;
    readLengthInBits = 0;
    recordsReadInThisIteration = 0;
    bytesReadInCurrentPass = 0;
    vectorData = ((BaseDataValueVector) valueVec).getBuffer();
  }

  public int capacity() {
    return (int) (valueVec.getValueCapacity() * dataTypeLengthInBits / 8.0);
  }

  // copied out of parquet library, didn't want to deal with the uneeded throws statement they had declared
  public static int readIntLittleEndian(DrillBuf in, int offset) {
    int ch4 = in.getByte(offset) & 0xff;
    int ch3 = in.getByte(offset + 1) & 0xff;
    int ch2 = in.getByte(offset + 2) & 0xff;
    int ch1 = in.getByte(offset + 3) & 0xff;
    return ((ch1 << 24) + (ch2 << 16) + (ch3 << 8) + (ch4 << 0));
  }
}
