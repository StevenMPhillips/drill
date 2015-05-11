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
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.record.RawFragmentBatch;

public abstract class BaseRawBatchBuffer<T extends MyNewInterface> implements RawBatchBuffer {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BaseRawBatchBuffer.class);

  private static enum BufferState {
    INIT,
    FINISHED,
    KILLED
  }

  class MyNewInterface{

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

  /**
   * handle the out of memory case
   *
   * @param batch
   */
  protected abstract void handleOutOfMemory(final RawFragmentBatch batch);

  /**
   * implementation specific method to enqueue batch
   *
   * @param batch
   * @throws IOException
   */
  protected abstract void enqueueInner(final RawFragmentBatch batch) throws IOException;

  /**
   * implementation specific method to get the current number of enqueued batches
   *
   * @return
   */
  protected abstract int getBufferSize();

  ## Add assertion that all acks have been sent.
  @Override
  public void cleanup() {
    if (!isFinished() && context.shouldContinue()) {
      final String msg = String.format("Cleanup before finished. " + (fragmentCount - streamCounter) + " out of "
          + fragmentCount + " streams have finished.");
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
   * Helper method to clear buffer with request bodies release also flushes ack queue - in case there are still
   * responses pending
   */
  protected abstract void clearBufferWithBody();

  @Override
  // RENAME FINISHED to something like more descriptive, make it private.
  public void finished() {
    if (state != BufferState.KILLED) {
      state = BufferState.FINISHED;
    }

    // we need to flushAcksIn this case.

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
      b = getNextBatchInternal();
    } catch (final InterruptedException e) {
      // Preserve evidence that the interruption occurred so that code higher up on the call stack can learn of the
      // interruption and respond to it if it wants to.
      Thread.currentThread().interrupt();

      // TODO: Add finished() call here.

      return null;
    }

    if (b != null) {
      if (b != null && b.getHeader().getIsOutOfMemory()) {
        outOfMemory.set(true);
        return b;
      }

      upkeep();

      if (b != null && b.getHeader().getIsLastBatch()) {
        logger.debug("Got last batch from {}:{}", b.getHeader().getSendingMajorFragmentId(), b.getHeader()
            .getSendingMinorFragmentId());
        streamCounter--;
        if (streamCounter == 0) {
          logger.debug("Stream finished");
          finished();
        }
      }
    } else {
      if (b == null && !isBufferEmpty()) {
        throw new IllegalStateException("Returning null when there are batches left in queue");
      }
      if (b == null && !isFinished()) {
        throw new IllegalStateException("Returning null when not finished");
      }
    }

    return b;

  }

  /**
   * handle OutOfMemory case when getNext() is called
   */
  protected abstract void outOfMemoryCase();

  /**
   * Retrieve the next batch from the queue
   *
   * @return
   * @throws IOException
   * @throws InterruptedException
   */
  protected abstract RawFragmentBatch getNextBatchInternal() throws IOException, InterruptedException;

  /**
   * Handle miscellaneous tasks after batch retrieval
   */
  protected abstract void upkeep();

  protected boolean isFinished() {
    return (state == BufferState.KILLED || state == BufferState.FINISHED);
  }

  abstract boolean isBufferEmpty();
}
