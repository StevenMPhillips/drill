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
import java.io.EOFException;
import java.io.IOException;
import java.util.Deque;
import java.util.List;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
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
import org.apache.hadoop.fs.FileStatus;
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
  private long streamTimeStamp;
  private volatile Spooler spooler;
  private Thread spoolingThread;
  private String spoolingThreadName;
  private int bufferIndex;

  public SpoolingRawBatchBuffer(FragmentContext context, int fragmentCount, int oppositeId, int bufferIndex) throws IOException, OutOfMemoryException {
    super(context, fragmentCount);
    this.allocator = context.getNewChildAllocator(ALLOCATOR_INITIAL_RESERVATION, ALLOCATOR_MAX_RESERVATION, true);
    this.threshold = context.getConfig().getLong(ExecConstants.SPOOLING_BUFFER_MEMORY);
//    this.threshold = 10000;
    this.oppositeId = oppositeId;
    this.bufferIndex = bufferIndex;
  }

  public static List<String> DIRS = DrillConfig.create().getStringList(ExecConstants.TEMP_DIRECTORIES);

  public static String getDir() {
    Random random = new Random();
    return DIRS.get(random.nextInt(DIRS.size()));
  }

  private synchronized void initSpooler() throws IOException {
    if (spooler != null) {
      return;
    }

    Configuration conf = new Configuration();
    conf.set(FileSystem.FS_DEFAULT_NAME_KEY, context.getConfig().getString(ExecConstants.TEMP_FILESYSTEM));
    conf.set(DRILL_LOCAL_IMPL_STRING, LocalSyncableFileSystem.class.getName());
    fs = FileSystem.get(conf);
    path = new Path(getDir(), getFileName());
    outputStream = fs.create(path);
    spoolingThreadName = QueryIdHelper.getExecutorThreadName(context.getHandle()).concat(":Spooler-" + oppositeId + "-" + bufferIndex);
    Spooler s = new Spooler();
    spoolingThread = new Thread(s);
    spoolingThread.start();
    spooler = s;
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
      if (spooler == null) {
        initSpooler();
      }
      spooler.addBatchForSpooling(wrapper);
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
    if (spooler != null) {
      spooler.stop();
    }
  }

  @Override
  protected void clearBufferWithBody() {
    try {
      while (!buffer.isEmpty()) {
        try {
          final RawFragmentBatch batch = buffer.poll().get();
          if (batch.getBody() != null) {
            batch.getBody().release();
          }
        } catch (InterruptedException e) {
          // keep trying until buffer is cleared
          continue;
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
      FragmentRecordBatch header = batch.getHeader();
      if (header.getIsOutOfMemory()) {
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
    FragmentRecordBatch header = batch.getHeader();
    if (header.getIsOutOfMemory()) {
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
    if (spooler != null) {
      spooler.stop();
    }
    closed = true;
    allocator.close();
    try {
      if (outputStream != null) {
        outputStream.close();
      }
      if (inputStream != null) {
//        inputStream.close();
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

  private class Spooler implements Runnable {

    private volatile LinkedBlockingDeque<RawFragmentBatchWrapper> spoolingQueue;
    private volatile boolean shouldContinue = true;
    private Thread spoolingThread;

    public Spooler() {
      spoolingQueue = Queues.newLinkedBlockingDeque();
    }

    public void run() {
      try {
        spoolingThread = Thread.currentThread();
        spoolingThread.setName(spoolingThreadName);
        while (shouldContinue) {
          RawFragmentBatchWrapper batch;
          try {
            batch = spoolingQueue.take();
          } catch (InterruptedException e) {
            if (shouldContinue) {
              continue;
            } else {
              break;
            }
          }
          try {
            batch.writeToStream(outputStream);
          } catch (IOException e) {
            context.fail(e);
          }
        }
      } catch (Throwable e) {
        logger.error("Caught.", e);
        context.fail(e);
      } finally {
        logger.info("Spooler thread exiting");
      }
    }

    public void addBatchForSpooling(RawFragmentBatchWrapper batchWrapper) {
      if (spoolingQueue != null) {
        spoolingQueue.add(batchWrapper);
      } else {
        // will not spill this batch
        batchWrapper.available = true;
        batchWrapper.latch.countDown();
      }
    }

    public void stop() {
      spoolingQueue = null;
      shouldContinue = false;
      if (spoolingThread.isAlive()) {
        spoolingThread.interrupt();
      }
    }
  }

  private class RawFragmentBatchWrapper {
    private RawFragmentBatch batch;
    private volatile boolean available;
    private CountDownLatch latch;
    private volatile int bodyLength;
    private volatile boolean outOfMemory = false;
    private volatile long timestamp;
    private long start = -1;
    private long check;

    public RawFragmentBatchWrapper(RawFragmentBatch batch, boolean available) {
      Preconditions.checkNotNull(batch);
      this.batch = batch;
      this.available = available;
      this.latch = new CountDownLatch(available ? 0 : 1);
    }

    public boolean isNull() {
      return batch == null;
    }

    public RawFragmentBatch get() throws InterruptedException, IOException {
      if (available) {
        assert batch.getHeader() != null : "batch header null";
        return batch;
      } else {
        latch.await();
        if (inputStream == null) {
          streamTimeStamp = System.nanoTime();
          inputStream = fs.open(path);
        }
        readFromStream();
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
      Stopwatch watch = new Stopwatch();
      watch.start();
      available = false;
      check = ThreadLocalRandom.current().nextLong();
      start = stream.getPos();
      logger.info("Writing check value {} at position {}", check, start);
      stream.writeLong(check);
      batch.getHeader().writeDelimitedTo(stream);
      ByteBuf buf = batch.getBody();
//      assert buf.capacity() == buf.readByte() : String.format("capacity: %d, readableBytes: %d", buf.capacity(), buf.readableBytes());
      if (buf != null) {
        bodyLength = buf.capacity();
      } else {
        bodyLength = 0;
      }
      if (bodyLength > 0) {
        buf.getBytes(0, stream, bodyLength);
      }
      stream.hsync();
      FileStatus status = fs.getFileStatus(path);
      long len = status.getLen();
      long pos = stream.getPos();
      logger.info("After spooling batch, stream at position {}. File length {}", stream.getPos(), len);
//      int duration = 1;
//      while (pos != len) {
//        try {
//          if (duration > 30000) {
//            throw new  IOException(String.format("Stream position and file length don't match. Pos: %d len: %d", stream.getPos(), len));
//          }
//          stream.hsync();
//          Thread.sleep(duration);
//          status = fs.getFileStatus(path);
//          len = status.getLen();
//          pos = stream.getPos();
//          duration *= 2;
//        } catch (InterruptedException e) {
//          throw new IOException(e);
//        }
//      }
//      assert stream.getPos() == len : String.format("Stream position and file length don't match. Pos: %d len: %d", stream.getPos(), len);
      timestamp = System.nanoTime();
      latch.countDown();
      long t = watch.elapsed(TimeUnit.MICROSECONDS);
      logger.debug("Took {} us to spool {} to disk. Rate {} mb/s", t, bodyLength, bodyLength / t);
      if (buf != null) {
        buf.release();
      }
    }

    public void readFromStream() throws IOException, InterruptedException {
      long pos = start;
      boolean tryAgain = true;
      DrillBuf buf = null;
      int duration = 0;
      while (tryAgain) {
        Thread.sleep(duration);
        try {
          FSDataInputStream stream = fs.open(path);
          stream.seek(start);
          long currentPos = stream.getPos();
          long check = stream.readLong();
          pos = stream.getPos();
          assert check == this.check : String.format("Check values don't match: %d %d, Position %d", this.check, check, currentPos);
//      checkTimeStampAndReOpenFile(timestamp, stream.getPos());
          Stopwatch watch = new Stopwatch();
          watch.start();
          BitData.FragmentRecordBatch header = BitData.FragmentRecordBatch.parseDelimitedFrom(stream);
          pos = stream.getPos();
          assert header != null : "header null after parsing from stream";
//      assert header.getSendingMajorFragmentId() == oppositeId : String.format("oppositeId: %s headerOppositeId: %s", oppositeId, header.getSendingMajorFragmentId());
          buf = allocator.buffer(bodyLength);
          buf.writeBytes(stream, bodyLength);
          pos = stream.getPos();
          batch = new RawFragmentBatch(header, buf, null);
          buf.release();
          available = true;
          latch.countDown();
          long t = watch.elapsed(TimeUnit.MICROSECONDS);
          stream.close();
          logger.debug("Took {} us to read {} from disk. Rate {} mb/s", t, bodyLength, bodyLength / t);
          tryAgain = false;
        } catch (EOFException e) {
          FileStatus status = fs.getFileStatus(path);
          logger.warn("EOF reading from file {} at pos {}. Current file size: {}", path, pos, status.getLen());
          if (buf != null) {
            buf.release();
          }
          duration = Math.max(1, duration * 2);
          if (duration < 60000) {
            continue;
          } else {
            throw e;
          }
        }
      }
    }

    private boolean isOutOfMemory() {
      return outOfMemory;
    }

    private void setOutOfMemory(boolean outOfMemory) {
      this.outOfMemory = outOfMemory;
    }
  }

  private void checkTimeStampAndReOpenFile(long timestamp, long currentPosition) throws IOException {
    if (timestamp < streamTimeStamp) {
      return;
    }
    logger.info("Resetting inputStream");
//    inputStream.close();
    inputStream = fs.open(path);
    streamTimeStamp = System.nanoTime();
    inputStream.seek(currentPosition);
  }

  private String getFileName() {
    ExecProtos.FragmentHandle handle = context.getHandle();

    String qid = QueryIdHelper.getQueryId(handle.getQueryId());

    int majorFragmentId = handle.getMajorFragmentId();
    int minorFragmentId = handle.getMinorFragmentId();

    String fileName = String.format("%s_%s_%s_%s_%s", qid, majorFragmentId, minorFragmentId, oppositeId, bufferIndex);

    return fileName;
  }
}
