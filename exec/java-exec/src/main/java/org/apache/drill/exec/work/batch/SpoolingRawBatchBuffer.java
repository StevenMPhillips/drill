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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.DrillBuf;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.memory.OutOfMemoryException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.proto.BitData;
import org.apache.drill.exec.proto.BitData.FragmentRecordBatch;
import org.apache.drill.exec.proto.ExecProtos;
import org.apache.drill.exec.proto.helper.QueryIdHelper;
import org.apache.drill.exec.record.RawFragmentBatch;
import org.apache.drill.exec.store.LocalSyncableFileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Queues;

/**
 * This implementation of RawBatchBuffer starts writing incoming batches to disk once the buffer size reaches a threshold.
 * The order of the incoming buffers is maintained.
 */
public class SpoolingRawBatchBuffer extends BaseRawBatchBuffer {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SpoolingRawBatchBuffer.class);

  private static String DRILL_LOCAL_IMPL_STRING = "fs.drill-local.impl";
  private static final float STOP_SPOOLING_FRACTION = (float) 0.5;
  public static final long ALLOCATOR_INITIAL_RESERVATION = 1*1024*1024;
  public static final long ALLOCATOR_MAX_RESERVATION = 20L*1000*1000*1000;

  private final LinkedBlockingDeque<RawFragmentBatchWrapper> buffer = Queues.newLinkedBlockingDeque();
  private volatile long queueSize = 0;
  private long threshold;
  private BufferAllocator allocator;
  private volatile AtomicBoolean spooling = new AtomicBoolean(false);
  private FileSystem fs;
  private Path path;
  private FSDataOutputStream outputStream;
  private FSDataInputStream inputStream;
  private boolean outOfMemory = false;
  private boolean closed = false;
  private int incomingBatchCounter = 0;
  private int outgoingBatchCounter = 0;
  private int oppositeId;

  public SpoolingRawBatchBuffer(FragmentContext context, int fragmentCount, int oppositeId) throws IOException, OutOfMemoryException {
    super(context, fragmentCount);
    this.allocator = context.getNewChildAllocator(ALLOCATOR_INITIAL_RESERVATION, ALLOCATOR_MAX_RESERVATION, true);
//    this.threshold = context.getConfig().getLong(ExecConstants.SPOOLING_BUFFER_MEMORY);
    this.threshold = 10000;
    Configuration conf = new Configuration();
    conf.set(FileSystem.FS_DEFAULT_NAME_KEY, context.getConfig().getString(ExecConstants.TEMP_FILESYSTEM));
    conf.set(DRILL_LOCAL_IMPL_STRING, LocalSyncableFileSystem.class.getName());
    this.fs = FileSystem.get(conf);
    this.oppositeId = oppositeId;
    this.path = new Path(getDir(), getFileName());
  }

  public static List<String> DIRS = DrillConfig.create().getStringList(ExecConstants.TEMP_DIRECTORIES);

  public static String getDir() {
    Random random = new Random();
    return DIRS.get(random.nextInt(DIRS.size()));
  }

  @Override
  protected void handleOutOfMemory(RawFragmentBatch batch) {
    if (!outOfMemory && !buffer.peekFirst().isOutOfMemory()) {
      logger.debug("Adding OOM message to front of queue. Current queue size: {}", buffer.size());
      buffer.addFirst(new RawFragmentBatchWrapper(batch, true));
    } else {
      logger.debug("ignoring duplicate OOM message");
    }
    batch.sendOk();
  }

  @Override
  protected void enqueueInner(RawFragmentBatch batch) throws IOException {
    assert batch.getHeader().getSendingMajorFragmentId() == oppositeId;
    logger.debug("Enqueue batch: {}. Current buffer size: {}. Last batch: {}. Sending fragment: {}", ++incomingBatchCounter, buffer.size(), batch.getHeader().getIsLastBatch(), batch.getHeader().getSendingMajorFragmentId());
    RawFragmentBatchWrapper wrapper;
    boolean spool = spooling.get();
    wrapper = new RawFragmentBatchWrapper(batch, !spool);
    queueSize += wrapper.getBodySize();
    if (spool) {
      if (outputStream == null) {
        outputStream = fs.create(path);
      }
      wrapper.writeToStream(outputStream);
    }
    buffer.add(wrapper);
    if (!spool && queueSize > threshold) {
      logger.debug("Buffer size {} greater than threshold {}. Start spooling to disk", queueSize, threshold);
      spooling.set(true);
    }
    batch.sendOk();
  }

  @Override
  protected int getBufferSize() {
    return buffer.size();
  }

  @Override
  public void kill(FragmentContext context) {
    allocator.close();
  }

  @Override
  protected void clearBufferWithBody() {
    try {
      while (!buffer.isEmpty()) {
        final RawFragmentBatch batch = buffer.poll().get();
        if (batch.getBody() != null) {
          batch.getBody().release();
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected void outOfMemoryCase() {
    if (buffer.size() < 10) {
      outOfMemory = false;
      logger.debug("Setting autoRead true");
    }
  }

  @Override
  protected RawFragmentBatch getNextBufferInternal() throws InterruptedException, IOException {
    logger.debug("Getting batch: {}. Current buffer size: {}", ++outgoingBatchCounter, buffer.size());
    boolean spool = spooling.get();
    RawFragmentBatchWrapper w = buffer.poll();
    RawFragmentBatch batch;
    if(w == null && (!isFinished() || !buffer.isEmpty())) {
      w = buffer.take();
      batch = w.get();
      if (batch.getHeader().getIsOutOfMemory()) {
        outOfMemory = true;
        return batch;
      }
      queueSize -= w.getBodySize();
      return batch;
    }
    if (w == null) {
      return null;
    }
    batch = w.get();
    if (batch.getHeader().getIsOutOfMemory()) {
      outOfMemory = true;
      return batch;
    }
    queueSize -= w.getBodySize();
    if (spool && queueSize < threshold * STOP_SPOOLING_FRACTION) {
      logger.debug("buffer size {} less than {}x threshold. Stop spooling.", queueSize, STOP_SPOOLING_FRACTION);
      spooling.set(false);
    }
    logger.debug("Got batch: {}. Current buffer size: {}", outgoingBatchCounter, buffer.size());
    return batch;
  }

  @Override
  protected void upkeep() {

  }

  @Override
  boolean isBufferEmpty() {
    return buffer.isEmpty();
  }

  public void cleanup() {
    if (closed) {
      logger.warn("Tried cleanup twice");
      return;
    }
    closed = true;
    allocator.close();
    try {
      if (outputStream != null) {
        outputStream.close();
      }
      if (inputStream != null) {
        inputStream.close();
      }
    } catch (IOException e) {
      logger.warn("Failed to cleanup I/O streams", e);
    }
    if (context.getConfig().getBoolean(ExecConstants.SPOOLING_BUFFER_DELETE)) {
      try {
        fs.delete(path,false);
      } catch (IOException e) {
        logger.warn("Failed to delete temporary files", e);
      }
      logger.debug("Deleted file {}", path.toString());
    }
    super.cleanup();
  }

  private class RawFragmentBatchWrapper {
    private RawFragmentBatch batch;
    private volatile boolean available;
    private CountDownLatch latch = new CountDownLatch(1);
    private int bodyLength;
    private boolean outOfMemory = false;

    public RawFragmentBatchWrapper(RawFragmentBatch batch, boolean available) {
      Preconditions.checkNotNull(batch);
      this.batch = batch;
      this.available = available;
    }

    public boolean isNull() {
      return batch == null;
    }

    public RawFragmentBatch get() throws IOException {
      if (available) {
        return batch;
      } else {
        if (inputStream == null) {
          inputStream = fs.open(path);
        }
        readFromStream(inputStream);
        available = true;
        return batch;
      }
    }

    public long getBodySize() {
      if (batch.getBody() == null) {
        return 0;
      }
      assert batch.getBody().readableBytes() >= 0;
      return batch.getBody().readableBytes();
    }

    public void writeToStream(FSDataOutputStream stream) throws IOException {
      assert batch.getHeader().getSendingMajorFragmentId() == oppositeId : String.format("oppositeId: %s headerOppositeId: %s", oppositeId, batch.getHeader().getSendingMajorFragmentId());
      Stopwatch watch = new Stopwatch();
      watch.start();
      available = false;
      batch.getHeader().writeDelimitedTo(stream);
      ByteBuf buf = batch.getBody();
      if (buf != null) {
        bodyLength = buf.readableBytes();
      } else {
        bodyLength = 0;
      }
      if (bodyLength > 0) {
        buf.getBytes(0, stream, bodyLength);
      }
      stream.sync();
      if (batch.getHeader().getIsLastBatch()) {
        stream.close();
      }
      long t = watch.elapsed(TimeUnit.MICROSECONDS);
      logger.debug("Took {} us to spool {} to disk. Rate {} mb/s", t, bodyLength, bodyLength / t);
      if (buf != null) {
        buf.release();
      }
    }

    public void readFromStream(FSDataInputStream stream) throws IOException {
      Stopwatch watch = new Stopwatch();
      watch.start();
      BitData.FragmentRecordBatch header = BitData.FragmentRecordBatch.parseDelimitedFrom(stream);
//      assert header.getSendingMajorFragmentId() == oppositeId : String.format("oppositeId: %s headerOppositeId: %s", oppositeId, header.getSendingMajorFragmentId());
      DrillBuf buf = allocator.buffer(bodyLength);
      buf.writeBytes(stream, bodyLength);
      batch = new RawFragmentBatch(header, buf, null);
      buf.release();
      available = true;
      latch.countDown();
      long t = watch.elapsed(TimeUnit.MICROSECONDS);
      logger.debug("Took {} us to read {} from disk. Rate {} mb/s", t, bodyLength, bodyLength / t);
    }

    private boolean isOutOfMemory() {
      return outOfMemory;
    }

    private void setOutOfMemory(boolean outOfMemory) {
      this.outOfMemory = outOfMemory;
    }
  }

  private String getFileName() {
    ExecProtos.FragmentHandle handle = context.getHandle();

    String qid = QueryIdHelper.getQueryId(handle.getQueryId());

    int majorFragmentId = handle.getMajorFragmentId();
    int minorFragmentId = handle.getMinorFragmentId();

    String fileName = String.format("%s_%s_%s_%s", qid, majorFragmentId, minorFragmentId, oppositeId);

    return fileName;
  }
}
