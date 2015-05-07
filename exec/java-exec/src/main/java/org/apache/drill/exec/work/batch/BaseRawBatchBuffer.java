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

import java.io.IOException;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.proto.BitData.FragmentRecordBatch;
import org.apache.drill.exec.record.RawFragmentBatch;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Queues;

public abstract class BaseRawBatchBuffer implements RawBatchBuffer {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BaseRawBatchBuffer.class);

  private static enum BufferState {
    INIT,
    FINISHED,
    KILLED
  }

  private volatile BufferState state = BufferState.INIT;
  protected final int bufferSizePerSocket;
  protected final AtomicBoolean outOfMemory = new AtomicBoolean(false);
  private int streamCounter;
  private final int fragmentCount;
  protected final FragmentContext context;

  public BaseRawBatchBuffer(final FragmentContext context, final int fragmentCount) {
    bufferSizePerSocket = context.getConfig().getInt(ExecConstants.INCOMING_BUFFER_SIZE);

    this.fragmentCount = fragmentCount;
    this.streamCounter = fragmentCount;
    this.context = context;
  }

  @Override
  public void enqueue(final RawFragmentBatch batch) throws IOException {

    // if this fragment is already canceled or failed, we shouldn't need any or more stuff. We do the null check to
    // ensure that tests run.
    if (context != null && !context.shouldContinue()) {
      this.kill(context);
    }

    if (isFinished()) {
      if (state == BufferState.KILLED) {
        // do not even enqueue just release and send ack back
        batch.release();
        batch.sendOk();
        return;
      } else {
        throw new IOException("Attempted to enqueue batch after finished");
      }
    }
    if (batch.getHeader().getIsOutOfMemory()) {
      handleOutOfMemory(batch);
      return;
    }
    enqueueInner(batch);
  }

  protected abstract void handleOutOfMemory(final RawFragmentBatch batch);

  protected abstract void enqueueInner(final RawFragmentBatch batch) throws IOException;

  protected abstract int getBufferSize();

  @Override
  public void cleanup() {
    if (!isFinished() && context.shouldContinue()) {
      final String msg = String.format("Cleanup before finished. " + (fragmentCount - streamCounter) + " out of " + fragmentCount + " streams have finished.");
      final IllegalStateException e = new IllegalStateException(msg);
      throw e;
    }

    if (!isBufferEmpty()) {
      if (context.shouldContinue()) {
        context.fail(new IllegalStateException("Batches still in queue during cleanup"));
        logger.error("{} Batches in queue.", getBufferSize());
      }
      clearBufferWithBody();
    }
  }

  @Override
  public void kill(final FragmentContext context) {
    state = BufferState.KILLED;
    clearBufferWithBody();
  }

  /**
   * Helper method to clear buffer with request bodies release
   * also flushes ack queue - in case there are still responses pending
   */
  protected abstract void clearBufferWithBody();

  @Override
  public void finished() {
    if (state != BufferState.KILLED) {
      state = BufferState.FINISHED;
    }
    if (!isBufferEmpty()) {
      throw new IllegalStateException("buffer not empty when finished");
    }
  }

  @Override
  public RawFragmentBatch getNext() throws IOException {

    if (outOfMemory.get()) {
      outOfMemoryCase();
    }

    RawFragmentBatch b;
    try {
      b = getNextBufferInternal();
    } catch (InterruptedException e) {
      return null;
    }

    if (b != null && b.getHeader().getIsOutOfMemory()) {
      outOfMemory.set(true);
      return b;
    }

    upkeep();

    if (b != null && b.getHeader().getIsLastBatch()) {
      logger.debug("Got last batch from {}:{}", b.getHeader().getSendingMajorFragmentId(), b.getHeader().getSendingMinorFragmentId());
      streamCounter--;
      if (streamCounter == 0) {
        logger.debug("Stream finished");
        finished();
      }
    }

    if (b == null && !isBufferEmpty()) {
      throw new IllegalStateException("Returning null when there are batches left in queue");
    }
    if (b == null && !isFinished()) {
      throw new IllegalStateException("Returning null when not finished");
    }
    return b;

  }

  protected abstract void outOfMemoryCase();

  protected abstract RawFragmentBatch getNextBufferInternal() throws IOException, InterruptedException;

  protected abstract void upkeep();

  protected boolean isFinished() {
    return (state == BufferState.KILLED || state == BufferState.FINISHED);
  }

  abstract boolean isBufferEmpty();
}
