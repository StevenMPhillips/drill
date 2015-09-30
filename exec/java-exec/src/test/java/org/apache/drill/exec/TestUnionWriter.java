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
package org.apache.drill.exec;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.memory.RootAllocatorFactory;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.store.TestOutputMutator;
import org.apache.drill.exec.store.easy.json.JsonProcessor.ReadState;
import org.apache.drill.exec.util.VectorUtil;
import org.apache.drill.exec.vector.complex.fn.JsonReader;
import org.apache.drill.exec.vector.complex.impl.UnionReader;
import org.apache.drill.exec.vector.complex.impl.UnionVector;
import org.apache.drill.exec.vector.complex.impl.UnionWriter;
import org.apache.drill.exec.vector.complex.impl.VectorContainerWriter;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.IOException;

public class TestUnionWriter {

  /*
  @Test
  public void test() throws Exception {
    BufferAllocator allocator = new TopLevelAllocator(DrillConfig.create());
    UnionWriter writer = new UnionWriter(allocator);
    writer.allocate();

    MapWriter rootWriter = writer.asMap();
    rootWriter.start();
    {

      BigIntWriter a = rootWriter.bigInt("a");
      a.writeBigInt(1);

      MapWriter b = rootWriter.map("b");
      BigIntWriter c = b.bigInt("c");

      b.start();
      c.writeBigInt(2);
      b.end();

      rootWriter.end();
    }
    {

      rootWriter.setPosition(1);
      rootWriter.start();

      BigIntWriter a = rootWriter.bigInt("a");
      a.writeBigInt(3);

      MapWriter b = rootWriter.map("b");
      BigIntWriter c = b.bigInt("c");
      VarCharWriter d = b.varChar("d");

      b.start();
      c.writeBigInt(2);
      DrillBuf buf = allocator.buffer(20);
      buf.writeBytes("hello".getBytes());
      d.writeVarChar(0, 5, buf);
      b.end();


      rootWriter.end();
    }

    {

      rootWriter.setPosition(2);
      rootWriter.start();

      BigIntWriter a = rootWriter.bigInt("a");
      a.writeBigInt(3);

      BigIntWriter b = rootWriter.bigInt("b");
      b.writeBigInt(4);


      rootWriter.end();
    }

//    writer.setValueCount(3);

//    UnionVector vector = writer.getData();

    {
//      Object obj = vector.getAccessor().getObject(0);

//      System.out.println(obj);
    }
    {
//      Object obj = vector.getAccessor().getObject(1);

      System.out.println(obj);
    }
    {
//      Object obj = vector.getAccessor().getObject(2);

      System.out.println(obj);
    }

//    UnionReader reader = new UnionReader(vector);

    reader.setPosition(0);

    UnionHolder holder = new UnionHolder();
    reader.reader("a").read(holder);

    System.out.println(holder.reader.readLong());
    reader.reader("b").reader("c").read(holder);

    System.out.println(holder.reader.readLong());

    reader.setPosition(1);
    reader.reader("a").read(holder);

    System.out.println(holder.reader.readLong());
    reader.reader("b").reader("c").read(holder);

    System.out.println(holder.reader.readLong());

    reader.reader("b").reader("d").read(holder);

    System.out.println(holder.reader.readObject());

    reader.setPosition(2);

    reader.reader("b").read(holder);

    System.out.println(holder.reader.readObject());

    TransferPair tp = vector.getTransferPair();
    tp.transfer();
    System.out.println(tp.getTo().getAccessor().getObject(0));
    System.out.println(tp.getTo().getAccessor().getObject(1));
    System.out.println(tp.getTo().getAccessor().getObject(2));
  }

  */
  @Test
  public void q() throws IOException {
    FileInputStream input = new FileInputStream("/tmp/a.json");
//    FileInputStream input = new FileInputStream("/Users/stevenphillips/yelp_dataset_challenge_academic_dataset/yelp_academic_dataset_business.json");
    BufferAllocator allocator = RootAllocatorFactory.newRoot(DrillConfig.create());
    TestOutputMutator mutator = new TestOutputMutator(allocator);
    VectorContainerWriter writer = new VectorContainerWriter(mutator);
    JsonReader reader = new JsonReader(allocator.buffer(1000), false, false, false);
    reader.setSource(input);

    ReadState state;
    int count = -1;
    do {
      count++;
      writer.setPosition(count);
      state = reader.write(writer);
    } while (state == ReadState.WRITE_SUCCEED);

    final int cnt = count;
    VectorContainer container = mutator.getContainer();
    container.setRecordCount(cnt);
    writer.setValueCount(cnt);
//    String name = "graph";
//    final UnionVector v = (UnionVector) container.getValueAccessorById(UnionVector.class, container.getValueVectorId(SchemaPath.getSimplePath(name)).getFieldIds()).getValueVector();
//    final UnionReader r = new UnionReader(v);

    VectorUtil.showVectorAccessibleContent(container, "  |---|  ");

//    for (int i = 0; i < cnt; i++) {
//      System.out.println(v.getAccessor().getObject(i));
//    }

//    final NullableBigIntVector v = ((UnionVector) mutator.iterator().next().getValueVector()).getBigInt();

/*
    long t1 = System.nanoTime();
    int sum = 0;
    final NullableBigIntHolder holder = new NullableBigIntHolder();
    for (int i = 0; i < cnt; i ++) {
      r.setPosition(i);
      r.read(holder);
//        v.getAccessor().get(i, holder);
        if (holder.isSet == 1) {
          sum += holder.value;
        }
    }
    System.out.println(sum);

    long t2 = System.nanoTime();

    long t = (t2 - t1) / cnt;
    System.out.println(t);
    */
  }
}
