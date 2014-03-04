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
package org.apache.drill.exec.store.hbase;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.ExpressionPosition;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.store.RecordReader;
import org.apache.drill.exec.vector.NullableVarBinaryVector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.VarBinaryVector;
import org.apache.drill.exec.vector.allocator.VectorAllocator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class HBaseRecordReader implements RecordReader {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HBaseRecordReader.class);

  private static final String ROW_KEY = "row_key";
  private static final int TARGET_RECORD_COUNT = 4000;

  private List<String> columns;
  private OutputMutator outputMutator;
  private Scan scan;
  private ResultScanner resultScanner;
  private FragmentContext context;
  private List<byte[]> sortedFamilies = Lists.newArrayList();
  private List<byte[]> sortedQualifiers = Lists.newArrayList();
  private List<NullableVarBinaryVector> sortedColumns;
  private Result leftOver;
  private boolean done;
  private VarBinaryVector rowKeyVector;

  public HBaseRecordReader(Configuration conf, HBaseSubScan.HBaseSubScanReadEntry e, List<String> columns, FragmentContext context) {
    this.columns = columns;
    this.scan = new Scan(Bytes.toBytesBinary(e.getStartRow()), Bytes.toBytesBinary(e.getEndRow()));
    this.context = context;
    byte[] empty = {};
    if (columns != null && columns.size() != 0) {
      List<KeyValue> kvs = Lists.newArrayList();
      for (String column : columns) {
        if (column.equalsIgnoreCase(ROW_KEY)) {
          continue;
        }
        String[] familyQualifier = column.split(":", 2);
        assert familyQualifier.length > 0;
        byte[] family = familyQualifier[0].getBytes();
        byte[] qualifier = familyQualifier.length == 2 ? familyQualifier[1].getBytes() : empty;
        scan.addColumn(family, qualifier);

        kvs.add(new KeyValue(empty, family, qualifier, empty));
      }

      Collections.sort(kvs, KeyValue.COMPARATOR);

      for (KeyValue kv : kvs) {
        sortedFamilies.add(kv.getFamily());
        sortedQualifiers.add(kv.getQualifier());
      }
    } else {
      if (this.columns == null) {
        this.columns = Lists.newArrayList();
      }
      this.columns.add(ROW_KEY);
    }

    Configuration config = HBaseConfiguration.create(conf);
    try {
      HTable table = new HTable(config, e.getTableName());
      resultScanner = table.getScanner(scan);
    } catch (IOException e1) {
      throw new RuntimeException(e1);
    }
  }

  @Override
  public void setup(OutputMutator output) throws ExecutionSetupException {
    this.outputMutator = output;
    output.removeAllFields();
    Map<String, NullableVarBinaryVector> vvMap = new HashMap();

    // Add Vectors to output in the order specified when creating reader
    for (String column : columns) {
      try {
        if (column.equals(ROW_KEY)) {
          MaterializedField field = MaterializedField.create(new SchemaPath(column, ExpressionPosition.UNKNOWN), Types.required(TypeProtos.MinorType.VARBINARY));
          rowKeyVector = new VarBinaryVector(field, context.getAllocator());
          output.addField(rowKeyVector);
        } else {
          MaterializedField field = MaterializedField.create(new SchemaPath(column, ExpressionPosition.UNKNOWN), Types.optional(TypeProtos.MinorType.VARBINARY));
          NullableVarBinaryVector v = new NullableVarBinaryVector(field, context.getAllocator());
          output.addField(v);
          vvMap.put(column, v);
        }
      } catch (SchemaChangeException e) {
        throw new ExecutionSetupException(e);
      }
    }

    try {
      output.setNewSchema();
    } catch (SchemaChangeException e) {
      throw new ExecutionSetupException(e);
    }

    // Create a list of the vectors sorted according to HBase Key order
    sortedColumns = Lists.newArrayList();
    for (int i = 0; i < sortedFamilies.size(); i++) {
      StringBuilder columnBuilder = new StringBuilder();
      columnBuilder.append(new String(sortedFamilies.get(i)));
      columnBuilder.append(":");
      columnBuilder.append(new String(sortedQualifiers.get(i)));
      String column = columnBuilder.toString();
      sortedColumns.add(vvMap.get(column));
    }
  }

  @Override
  public int next() {
    Stopwatch watch = new Stopwatch();
    watch.start();
    if (rowKeyVector != null) {
      rowKeyVector.clear();
      VectorAllocator.getAllocator(rowKeyVector, 100).alloc(TARGET_RECORD_COUNT);
    }
    for (ValueVector v : sortedColumns) {
      v.clear();
      VectorAllocator.getAllocator(v, 100).alloc(TARGET_RECORD_COUNT);
    }
    for (int count = 0; count < TARGET_RECORD_COUNT; count++) {
      Result result = null;
      try {
        if (leftOver != null) {
          result = leftOver;
          leftOver = null;
        } else {
          result = resultScanner.next();
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      if (result == null) {
        setOutputValueCount(count);
        logger.debug("Took {} ms to get {} records", watch.elapsed(TimeUnit.MILLISECONDS), count);
        return count;
      }
      KeyValue[] kvs = result.raw();
      byte[] bytes = result.getBytes().get();
      if (rowKeyVector != null) {
        if (!rowKeyVector.getMutator().setSafe(count, bytes, kvs[0].getRowOffset(), kvs[0].getRowLength())) {
          setOutputValueCount(count);
          leftOver = result;
          logger.debug("Took {} ms to get {} records", watch.elapsed(TimeUnit.MILLISECONDS), count);
          return count;
        }
      }
      int sortedListCount = 0;
      for (KeyValue kv : kvs) {
        int familyOffset = kv.getFamilyOffset();
        int familyLength = kv.getFamilyLength();
        int qualifierOffset = kv.getQualifierOffset();
        int qualifierLength = kv.getQualifierLength();
        NullableVarBinaryVector v;

        if (sortedListCount == sortedColumns.size()) {
          addNewVector(Arrays.copyOfRange(bytes, familyOffset, familyOffset + familyLength),
                  Arrays.copyOfRange(bytes, qualifierOffset, qualifierOffset + qualifierLength), sortedListCount, count);
          v = sortedColumns.get(sortedListCount);
        } else {
          byte[] f = sortedFamilies.get(sortedListCount);
          byte[] q = sortedQualifiers.get(sortedListCount);
          v = sortedColumns.get(sortedListCount);
          int fCompare = 0, qCompare = 0;
          while ((fCompare = compareArrays(bytes, familyOffset, familyLength, f, 0, f.length)) != 0 ||
                  (qCompare = compareArrays(bytes, qualifierOffset, qualifierLength, q, 0, q.length)) != 0) {
            if (fCompare < 0 || (fCompare == 0 && qCompare < 0)) { // We have a new column
              addNewVector(Arrays.copyOfRange(bytes, familyOffset, familyOffset + familyLength), Arrays.copyOfRange(bytes, qualifierOffset, qualifierOffset + qualifierLength), sortedListCount, count);
              v = sortedColumns.get(sortedListCount);
              break;
            } else {
              sortedListCount++;
              v.getMutator().setNull(count);
              if (sortedListCount >= sortedColumns.size()) {
                addNewVector(Arrays.copyOfRange(bytes, familyOffset, familyOffset + familyLength),
                        Arrays.copyOfRange(bytes, qualifierOffset, qualifierOffset + qualifierLength), sortedListCount, count);
                v = sortedColumns.get(sortedListCount);
                break;
              }
              f = sortedFamilies.get(sortedListCount);
              q = sortedQualifiers.get(sortedListCount);
              v = sortedColumns.get(sortedListCount);
            }
          }
        }
        int valueOffset = kv.getValueOffset();
        int valueLength = kv.getValueLength();
        if (!v.getMutator().setSafe(count, bytes, valueOffset, valueLength)) {
          setOutputValueCount(count);
          leftOver = result;
          logger.debug("Took {} ms to get {} records", watch.elapsed(TimeUnit.MILLISECONDS), count);
          return count;
        }
        sortedListCount++;
      }
      while (sortedListCount < sortedColumns.size()) {
        sortedColumns.get(sortedListCount).getMutator().setNull(count);
        sortedListCount++;
      }
    }
    setOutputValueCount(TARGET_RECORD_COUNT);
    logger.debug("Took {} ms to get {} records", watch.elapsed(TimeUnit.MILLISECONDS), TARGET_RECORD_COUNT);
    return TARGET_RECORD_COUNT;
  }

  private void addNewVector(byte[] family, byte[] qualifier, int position, int count) {
    StringBuilder columnBuilder = new StringBuilder();
    columnBuilder.append(new String(family));
    columnBuilder.append(":");
    columnBuilder.append(new String(qualifier));
    String column = columnBuilder.toString();
    MaterializedField field = MaterializedField.create(new SchemaPath(column, ExpressionPosition.UNKNOWN), Types.optional(TypeProtos.MinorType.VARBINARY));
    NullableVarBinaryVector v = new NullableVarBinaryVector(field, context.getAllocator());
    VectorAllocator.getAllocator(v, 100).alloc(TARGET_RECORD_COUNT);
    NullableVarBinaryVector.Mutator m = v.getMutator();
    for (int i = 0; i < count; i++) {
      m.setNull(count);
    }
    sortedFamilies.add(position, family);
    sortedQualifiers.add(position, qualifier);
    sortedColumns.add(position, v);
    try {
      outputMutator.addField(v);
      outputMutator.setNewSchema();
    } catch (SchemaChangeException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void cleanup() {
    System.out.println("");
  }

  private void setOutputValueCount(int count) {
    for (ValueVector vv : sortedColumns) {
      vv.getMutator().setValueCount(count);
    }
    if (rowKeyVector != null) {
      rowKeyVector.getMutator().setValueCount(count);
    }
  }

  private int compareArrays(byte[] left, int lstart, int llength, byte[] right, int rstart, int rlength) {
    int length = Math.min(llength, rlength);
    for (int i = 0; i < length; i++) {
      if (left[lstart + i] != right[rstart + i]) {
        return left[lstart + i] - right[rstart + 1];
      }
    }
    return llength - rlength;
  }
}
