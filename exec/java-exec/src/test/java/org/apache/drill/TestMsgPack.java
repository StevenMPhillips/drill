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

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.expr.holders.VarBinaryHolder;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.memory.TopLevelAllocator;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.vector.VarBinaryVector;
import org.msgpack.core.MessageFormat;
import org.msgpack.core.MessagePacker;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.core.buffer.ByteBufferInput;
import org.msgpack.core.buffer.InputStreamBufferInput;
import org.msgpack.core.buffer.MessageBuffer;
import org.msgpack.core.buffer.MessageBufferInput;
import org.msgpack.core.buffer.MessageBufferOutput;
import org.msgpack.core.buffer.OutputStreamBufferOutput;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Created by stevenphillips on 8/25/15.
 */
public class TestMsgPack {
  public static void main(String[] args) throws IOException {

    BufferAllocator allocator = new TopLevelAllocator(DrillConfig.create());
    VarBinaryVector vector = new VarBinaryVector(MaterializedField.create("field", Types.required(TypeProtos.MinorType.VARBINARY)), allocator);
    vector.allocateNew();

    ByteArrayOutputStream os = new ByteArrayOutputStream();
    MessageBufferOutput buffer = new OutputStreamBufferOutput(os);
    MessagePacker packer = new MessagePacker(buffer);
//    packer.packArrayHeader(3);
    packer.packString("hello").packString("world");
    packer.close();

    vector.getMutator().setSafe(0, os.toByteArray());

    vector.getMutator().setValueCount(1);

    VarBinaryHolder holder = new VarBinaryHolder();
    vector.getAccessor().get(0, holder);
    ByteBuffer buf = holder.buffer.internalNioBuffer(holder.start, holder.end - holder.start);
    MessageBufferInput bufferInput = new ByteBufferInput(buf);

    MessageUnpacker unpacker = new MessageUnpacker(bufferInput);
    while (unpacker.hasNext()) {
      MessageFormat format = unpacker.getNextFormat();
      switch (format) {
      case FIXSTR:
      case STR8:
      case STR16:
      case STR32:
        int len = unpacker.unpackRawStringHeader();
        byte[] b = unpacker.readPayload(len);
        System.out.println(new String(b));
        break;
      case ARRAY16:
      case ARRAY32:
      case FIXARRAY:
        System.out.println(unpacker.unpackValue().asArrayValue().list());
//        unpacker.unpackArrayHeader();
      }
    }
  }
}
