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

  private final LinkedBlockingDeque<RawFragmentBatch> buffer;
  private final AtomicBoolean overlimit = new AtomicBoolean(false);
  private final int softlimit;
  private final int startlimit;
  private final ResponseSenderQueue readController = new ResponseSenderQueue();

  public UnlimitedRawBatchBuffer(FragmentContext context, int fragmentCount, int oppositeId) {
    super(context, fragmentCount);
    this.softlimit = bufferSizePerSocket * fragmentCount;
    this.startlimit = Math.max(softlimit/2, 1);
    logger.trace("softLimit: {}, startLimit: {}", softlimit, startlimit);
    this.buffer = Queues.newLinkedBlockingDeque();
  }

  protected void handleOutOfMemory(final RawFragmentBatch batch) {
    logger.trace("Setting autoread false");
    final RawFragmentBatch firstBatch = buffer.peekFirst();
    final FragmentRecordBatch header = firstBatch == null ? null :firstBatch.getHeader();
    if (!outOfMemory.get() && !(header == null) && header.getIsOutOfMemory()) {
      buffer.addFirst(batch);
    }
    outOfMemory.set(true);
  }

  protected void enqueueInner(final RawFragmentBatch batch) throws IOException {
    buffer.add(batch);
    if (buffer.size() >= softlimit) {
      logger.trace("buffer.size: {}", buffer.size());
      overlimit.set(true);
      readController.enqueueResponse(batch.getSender());
    } else {
      batch.sendOk();
    }
  }

  protected void clearBufferWithBody() {

    WE NEED TO FLUSH ACKS here.

    while (!buffer.isEmpty()) {
      final RawFragmentBatch batch = buffer.poll();
      if (batch.getBody() != null) {
        batch.getBody().release();
      }
    }
    readController.flushResponses();
  }

  protected void outOfMemoryCase() {
    if (buffer.size() < 10) {
      logger.trace("Setting autoread true");
      outOfMemory.set(false);
      readController.flushResponses();
    }
  }

  protected RawFragmentBatch getNextBatchInternal() throws InterruptedException {
    RawFragmentBatch b = buffer.poll();

    // if we didn't get a buffer, block on waiting for buffer.
    if (b == null && (!isFinished() || !buffer.isEmpty())) {
      b = buffer.take();
    }
    return b;
  }

  protected void upkeep() {
    // try to flush the difference between softlimit and queue size, so every flush we are reducing backlog
    // when queue size is lower then softlimit - the bigger the difference the more we can flush
    if (!isFinished() && overlimit.get()) {
      final int flushCount = softlimit - buffer.size();
      if ( flushCount > 0 ) {
        final int flushed = readController.flushResponses(flushCount);
        logger.trace("flush {} entries, flushed {} entries ", flushCount, flushed);
        if ( flushed == 0 ) {
          // queue is empty - nothing to do for now
          overlimit.set(false);
        }
      }
    }
  }

  boolean isBufferEmpty() {
    return buffer.isEmpty();
  }

  protected int getBufferSize() {
    return buffer.size();
  }

  @VisibleForTesting
  ResponseSenderQueue getReadController() {
    return readController;
  }
}
