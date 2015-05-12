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
package org.apache.drill.exec.work.batch;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Queues;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.proto.BitData.FragmentRecordBatch;
import org.apache.drill.exec.record.RawFragmentBatch;

import java.io.IOException;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicBoolean;

public class UnlimitedRawBatchBuffer extends BaseRawBatchBuffer {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(UnlimitedRawBatchBuffer.class);

  private final AtomicBoolean overlimit = new AtomicBoolean(false);
  private final int softlimit;
  private final int startlimit;

  public UnlimitedRawBatchBuffer(FragmentContext context, int fragmentCount, int oppositeId) {
    super(context, fragmentCount);
    this.softlimit = bufferSizePerSocket * fragmentCount;
    this.startlimit = Math.max(softlimit/2, 1);
    logger.trace("softLimit: {}, startLimit: {}", softlimit, startlimit);
    this.bufferQueue = new UnlimitedBufferQueue();
  }

  private class UnlimitedBufferQueue implements BufferQueue<RawFragmentBatch> {
    private final LinkedBlockingDeque<RawFragmentBatch> buffer = Queues.newLinkedBlockingDeque();;

    @Override
    public void addOomBatch(RawFragmentBatch batch) {
      buffer.addFirst(batch);
    }

    @Override
    public RawFragmentBatch poll() throws IOException {
      RawFragmentBatch batch = buffer.poll();
      if (batch != null) {
        batch.sendOk();
      }
      return batch;
    }

    @Override
    public RawFragmentBatch take() throws IOException, InterruptedException {
      RawFragmentBatch batch = buffer.take();
      batch.sendOk();
      return batch;
    }

    @Override
    public boolean checkForOutOfMemory() {
      return buffer.peekFirst().getHeader().getIsOutOfMemory();
    }

    @Override
    public int size() {
      return buffer.size();
    }

    @Override
    public boolean isEmpty() {
      return buffer.size() == 0;
    }

    @Override
    public void add(RawFragmentBatch batch) {
      buffer.add(batch);
    }
  }

  protected void enqueueInner(final RawFragmentBatch batch) throws IOException {
    if (bufferQueue.size() < softlimit) {
      batch.sendOk();
    }
    bufferQueue.add(batch);
  }

  @Override
  protected void flush() {
  }


  protected void upkeep(RawFragmentBatch batch) {
  }

  boolean isBufferEmpty() {
    return bufferQueue.isEmpty();
  }

  protected int getBufferSize() {
    return bufferQueue.size();
  }
}
