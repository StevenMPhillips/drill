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

import com.google.common.collect.Queues;
import com.sun.corba.se.impl.encoding.BufferQueue;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.record.RawFragmentBatch;

public abstract class BaseRawBatchBuffer implements RawBatchBuffer {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BaseRawBatchBuffer.class);

  private static enum BufferState {
    INIT,
    STREAM_ENDED,
    KILLED
  }

  protected interface BufferQueue<T> {
    public void addOomBatch(RawFragmentBatch batch);
    public RawFragmentBatch poll() throws IOException;
    public RawFragmentBatch take() throws IOException, InterruptedException;
    public boolean checkForOutOfMemory();
    public int size();
    public boolean isEmpty();
    public void add(T obj);
  }

  protected BufferQueue bufferQueue;
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

    if (isTerminated()) {
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
  protected void handleOutOfMemory(final RawFragmentBatch batch) {
    if (!bufferQueue.checkForOutOfMemory()) {
      logger.debug("Adding OOM message to front of queue. Current queue size: {}", bufferQueue.size());
      bufferQueue.addOomBatch(batch);
    } else {
      logger.debug("ignoring duplicate OOM message");
    }
  }

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

//  ## Add assertion that all acks have been sent. TODO
  @Override
  public void cleanup() {
    if (!isTerminated() && context.shouldContinue()) {
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
  private void clearBufferWithBody() {
    flush();
    while (!bufferQueue.isEmpty()) {
      final RawFragmentBatch batch;
      try {
        batch = bufferQueue.poll();
      } catch (IOException e) {
        context.fail(e);
        continue;
      }
      if (batch.getBody() != null) {
        batch.getBody().release();
      }
    }
  }

  private void streamEnded() {
    if (state != BufferState.KILLED) {
      state = BufferState.STREAM_ENDED;
    }

    flush();

    if (!isBufferEmpty()) {
      throw new IllegalStateException("buffer not empty when finished");
    }
  }

  @Override
  public RawFragmentBatch getNext() throws IOException {

    if (outOfMemory.get()) {
      if (bufferQueue.size() < 10) {
        outOfMemory.set(false);
        flush();
      }
    }

    RawFragmentBatch b;
    try {
      b = bufferQueue.poll();

      // if we didn't get a batch, block on waiting for queue.
      if (b == null && (!isTerminated() || !bufferQueue.isEmpty())) {
        b = bufferQueue.take();
      }
    } catch (final InterruptedException e) {

      if (!context.shouldContinue()) {
        kill(context);
      } else {
        throw new DrillRuntimeException("Interrupted but context.shouldContinue() is true", e);
      }
      // Preserve evidence that the interruption occurred so that code higher up on the call stack can learn of the
      // interruption and respond to it if it wants to.
      Thread.currentThread().interrupt();

      return null;
    }

    if (b != null) {
      if (b.getHeader().getIsOutOfMemory()) {
        outOfMemory.set(true);
        return b;
      }

      upkeep(b);

      if (b.getHeader().getIsLastBatch()) {
        logger.debug("Got last batch from {}:{}", b.getHeader().getSendingMajorFragmentId(), b.getHeader()
            .getSendingMinorFragmentId());
        streamCounter--;
        if (streamCounter == 0) {
          logger.debug("Stream finished");
          streamEnded();
        }
      }
    } else {
      if (!isBufferEmpty()) {
        throw new IllegalStateException("Returning null when there are batches left in queue");
      }
      if (!isTerminated()) {
        throw new IllegalStateException("Returning null when not finished");
      }
    }

    return b;

  }

  protected abstract void flush();

  /**
   * Handle miscellaneous tasks after batch retrieval
   */
  protected abstract void upkeep(RawFragmentBatch batch);

  protected boolean isTerminated() {
    return (state == BufferState.KILLED || state == BufferState.STREAM_ENDED);
  }

  abstract boolean isBufferEmpty();
}
