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
package org.apache.drill.exec.record;

import io.netty.buffer.DrillBuf;

import org.apache.drill.exec.proto.BitData.FragmentRecordBatch;
import org.apache.drill.exec.rpc.data.AckSender;

public class RawFragmentBatch {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(RawFragmentBatch.class);

  private final FragmentRecordBatch header;
  private final DrillBuf body;
  private final AckSender sender;

  private boolean ackSent;

  public RawFragmentBatch(FragmentRecordBatch header, DrillBuf body, AckSender sender) {
    super();
    this.header = header;
    this.body = body;
    this.sender = sender;
    if (body != null) {
      body.retain();
    }
  }

  public FragmentRecordBatch getHeader() {
    return header;
  }

  public DrillBuf getBody() {
    return body;
  }

  @Override
  public String toString() {
    return "RawFragmentBatch [header=" + header + ", body=" + body + "]";
  }

  public void release() {
    if (body != null) {
      body.release();
    }
  }


  public AckSender getSender() {
    return sender;
  }

  public void sendOk() {
    if (sender != null && !ackSent) {
      sender.sendOk();
      ackSent = true;
    }
  }

  public long getByteCount() {
    return body == null ? 0 : body.readableBytes();
  }

  public boolean isAckSent() {
    return ackSent;
  }

}
