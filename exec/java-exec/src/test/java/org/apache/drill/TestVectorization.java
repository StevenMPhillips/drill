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
package org.apache.drill;

import io.netty.buffer.DrillBuf;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.holders.IntHolder;
import org.apache.drill.exec.memory.TopLevelAllocator;
import org.apache.drill.exec.vector.IntVector;
import org.junit.Test;

import java.util.concurrent.ThreadLocalRandom;

/**
 * Created by stevenphillips on 9/12/15.
 */
public class TestVectorization {

  IntVector vv0;
  IntVector vv4;
  IntVector vv9;

  public void doEval(int inIndex, int outIndex)
          throws SchemaChangeException
  {
    {
      IntHolder out3 = new IntHolder();
      {
        out3 .value = vv0 .getAccessor().get((inIndex));
      }
      IntHolder out7 = new IntHolder();
      {
        out7 .value = vv4 .getAccessor().get((inIndex));
      }
      //---- start of eval portion of add function. ----//
      IntHolder out8 = new IntHolder();
      {
        final IntHolder out = new IntHolder();
        IntHolder in1 = out3;
        IntHolder in2 = out7;

        AddFunctions$IntIntAdd_eval: {
          out.value = (int) (in1.value + in2.value);
        }

        out8 = out;
      }
      //---- end of eval portion of add function. ----//
      vv9 .getMutator().set((outIndex), out8 .value);
    }
  }

  @Test
  public void q() {
    TopLevelAllocator allocator = new TopLevelAllocator(DrillConfig.create());
    DrillBuf buf1 = allocator.buffer(1024*1024);
    DrillBuf buf2 = allocator.buffer(1024*1024);
    DrillBuf buf3 = allocator.buffer(1024*1024);

    populate(buf1);
    populate(buf2);

    long t1 = System.nanoTime();

    for (int i = 0; i < 10000; i ++) {
      add(buf1, buf2, buf3);
    }
    long t2 = System.nanoTime();

    long nanos = (t2 - t1) / (1000 * 1024*1024);
    System.out.println(nanos);
  }

  public static void main(String[] args) {

  }

  public static void q2 (String[] args) {
    final int length = 1024*1024;
//    int[] buf1 = new int[1024*1024];
//    int[] buf2 = new int[1024*1024];
//    int[] buf3 = new int[1024*1024];

    TopLevelAllocator allocator = new TopLevelAllocator(DrillConfig.create());
    DrillBuf buf1 = allocator.buffer(length);
    DrillBuf buf2 = allocator.buffer(length);
    DrillBuf buf3 = allocator.buffer(length);

    populate(buf1);
    populate(buf2);

    long t1 = System.nanoTime();

    for (int i = 0; i < 10000; i ++) {
//      mul2(buf1, buf2, buf3);
      add(buf1, buf2, buf3);
//      add2(buf1, buf2, buf3);
    }
    long t2 = System.nanoTime();

    long nanos = (t2 - t1) * 1000 / (10000 * (long) length);
    System.out.println(nanos);

    System.out.println(buf3.getInt(Math.abs(ThreadLocalRandom.current().nextInt()) % length));
//    System.out.println(buf3[Math.abs(ThreadLocalRandom.current().nextInt()) % length]);
  }

  private static void add(int[] buf1, int[] buf2, int[] out) {
    int length = buf1.length;

    for (int i = 0; i < length; i++) {
      out[i] = buf1[i] + buf2[i];
    }
  }

  private static void mul(int[] buf1, int[] buf2, int[] out) {
    int length = buf1.length;

    for (int i = 0; i < length; i++) {
      out[i] = buf1[i] * buf2[i];
    }
  }

  private static void add2(int[] buf1, int[] buf2, int[] out) {
    int length = buf1.length;

    int ub = (length / 8);

    for (int index = 0; index < ub; index++) {
      int i = index * 8;
      out[i] = buf1[i] + buf2[i];
      out[i+1] = buf1[i+1] + buf2[i+1];
      out[i+2] = buf1[i+2] + buf2[i+2];
      out[i+3] = buf1[i+3] + buf2[i+3];
      out[i+4] = buf1[i+4] + buf2[i+4];
      out[i+5] = buf1[i+5] + buf2[i+5];
      out[i+6] = buf1[i+6] + buf2[i+6];
      out[i+7] = buf1[i+7] + buf2[i+7];
    }

    int finish = ub * 8;
    for (int i = finish; i < length; i++) {
      out[i] = buf1[i] + buf2[i];
    }
  }

  private static void mul2(int[] buf1, int[] buf2, int[] out) {
    int length = buf1.length;

    int ub = (length / 8);

    for (int index = 0; index < ub; index++) {
      int i = index * 8;
      out[i] = buf1[i] * buf2[i];
      out[i+1] = buf1[i+1] * buf2[i+1];
      out[i+2] = buf1[i+2] * buf2[i+2];
      out[i+3] = buf1[i+3] * buf2[i+3];
      out[i+4] = buf1[i+4] * buf2[i+4];
      out[i+5] = buf1[i+5] * buf2[i+5];
      out[i+6] = buf1[i+6] * buf2[i+6];
      out[i+7] = buf1[i+7] * buf2[i+7];
    }

    int finish = ub * 8;
    for (int i = finish; i < length; i++) {
      out[i] = buf1[i] * buf2[i];
    }
  }

  private static void add(DrillBuf buf1, DrillBuf buf2, DrillBuf out) {
    int length = buf1.capacity() / 4;

    for (int i = 0; i < length; i++) {
      int index = i * 4;
      out.setInt(index, buf1.getInt(index) + buf2.getInt(index));
    }
  }

  private static void populate(int[] buf) {
    for (int i = 0; i < buf.length;  i++) {
      buf[i] = i;
    }
  }

  private static void populate(DrillBuf buf) {
    for (int i = 0; i < buf.capacity() / 4; i++) {
      buf.setInt(i, i);
    }
  }

}
