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
package org.apache.drill.exec.store.parquet;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.DrillBuf;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.IdentityHashMap;

import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.store.parquet.DirectCodecFactory.DirectBytesDecompressor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.DirectDecompressor;
import org.xerial.snappy.Snappy;

import parquet.bytes.ByteBufferAllocator;
import parquet.bytes.BytesInput;
import parquet.hadoop.CodecFactory;
import parquet.hadoop.CodecFactory.BytesCompressor;
import parquet.hadoop.HeapCodecFactory.HeapBytesCompressor;
import parquet.hadoop.metadata.CompressionCodecName;

import com.google.common.base.Preconditions;

public class DirectCodecFactory extends CodecFactory<BytesCompressor, DirectBytesDecompressor> implements AutoCloseable {

  private final ByteBufferAllocator allocator;
  private final IdentityHashMap<ByteBuffer, Integer> allocatedBuffers = new IdentityHashMap<ByteBuffer, Integer>();

  public DirectCodecFactory(Configuration config, ByteBufferAllocator allocator) {
    super(config);
    Preconditions.checkNotNull(allocator);
    this.allocator = allocator;
  }

  public DirectCodecFactory(Configuration config, BufferAllocator allocator) {
    this(config, new ParquetDirectByteBufferAllocator(allocator));
  }

  private ByteBuffer ensure(ByteBuffer buffer, int size) {
    if (buffer == null) {
      buffer = allocator.allocate(size);
      allocatedBuffers.put(buffer, 0);
    } else if (buffer.capacity() >= size) {
      buffer.clear();
    } else {
      allocator.release(buffer);
      release(buffer);
      buffer = allocator.allocate(size);
      allocatedBuffers.put(buffer, 0);
    }
    return buffer;
  }

  ByteBuffer release(ByteBuffer buffer) {
    if (buffer != null) {
      allocator.release(buffer);
      allocatedBuffers.remove(buffer);
    }
    return null;
  }

  @Override
  protected BytesCompressor createCompressor(final CompressionCodecName codecName, final CompressionCodec codec,
      int pageSize) {
    if (codec == null) {
      return new NoopCompressor();
    } else if (codecName == CompressionCodecName.SNAPPY) {
      // avoid using the Parquet Snappy codec since it allocates direct buffers at awkward spots.
      return new SnappyCompressor();
    }else{

      // todo: move zlib above since it also generates allocateDirect calls.
      return new HeapBytesCompressor(codecName, codec, pageSize);
    }
  }

  @Override
  protected DirectBytesDecompressor createDecompressor(final CompressionCodec codec) {
    if (codec == null) {
      return new NoopDecompressor();
    } else if (DirectCodecPool.INSTANCE.codec(codec).supportsDirectDecompression()) {
      return new FullDirectDecompressor(codec);
    } else {
      return new IndirectDecompressor(codec);
    }
  }

  public void close() {
    release();
  }

  public class IndirectDecompressor extends DirectBytesDecompressor {
    private final Decompressor decompressor;

    public IndirectDecompressor(CompressionCodec codec) {
      this.decompressor = DirectCodecPool.INSTANCE.codec(codec).borrowDecompressor();
    }

    @Override
    public BytesInput decompress(BytesInput bytes, int uncompressedSize) throws IOException {
      decompressor.reset();
      byte[] inputBytes = bytes.toByteArray();
      decompressor.setInput(inputBytes, 0, inputBytes.length);
      byte[] output = new byte[uncompressedSize];
      decompressor.decompress(output, 0, uncompressedSize);
      return BytesInput.from(output);
    }

    @Override
    public void decompress(DrillBuf input, int compressedSize, DrillBuf output, int uncompressedSize)
        throws IOException {
      decompressor.reset();
      byte[] inputBytes = new byte[compressedSize];
      input.getBytes(0, inputBytes);
      decompressor.setInput(inputBytes, 0, inputBytes.length);
      byte[] outputBytes = new byte[uncompressedSize];
      decompressor.decompress(outputBytes, 0, uncompressedSize);
      output.writeBytes(outputBytes);
    }

    @Override
    protected void release() {
      DirectCodecPool.INSTANCE.returnDecompressor(decompressor);
    }
  }

  public class FullDirectDecompressor extends DirectBytesDecompressor {
    private final DirectDecompressor decompressor;
    private ByteBuffer incomingBuffer;
    private ByteBuffer outgoingBuffer;
    public FullDirectDecompressor(CompressionCodec codec){
      this.decompressor = DirectCodecPool.INSTANCE.codec(codec).borrowDirectDecompressor();
    }

    @Override
    public BytesInput decompress(BytesInput bytes, int uncompressedSize) throws IOException {
      outgoingBuffer = ensure(outgoingBuffer, uncompressedSize);
      ByteBuffer bufferIn = bytes.toByteBuffer();
      if (bufferIn.isDirect()) {
        decompressor.decompress(bufferIn, outgoingBuffer);
      } else {
        this.incomingBuffer = ensure(this.incomingBuffer, (int) bytes.size());
        this.incomingBuffer.put(bufferIn);
        this.incomingBuffer.flip();
        decompressor.decompress(bufferIn, outgoingBuffer);
      }

      return BytesInput.from(outgoingBuffer, 0, uncompressedSize);
    }

    @Override
    public void decompress(DrillBuf input, int compressedSize, DrillBuf output, int uncompressedSize)
        throws IOException {
      decompressor.decompress(input.nioBuffer(0, compressedSize), output.nioBuffer(0, uncompressedSize));
    }

    @Override
    protected void release() {
      outgoingBuffer = DirectCodecFactory.this.release(outgoingBuffer);
      DirectCodecPool.INSTANCE.returnDecompressor(decompressor);
    }

  }

  public class NoopDecompressor extends DirectBytesDecompressor {

    @Override
    public void decompress(DrillBuf input, int compressedSize, DrillBuf output, int uncompressedSize)
        throws IOException {
      Preconditions.checkArgument(compressedSize == uncompressedSize,
          "Non-compressed data did not have matching compressed and uncompressed sizes.");
      output.writeBytes(input, compressedSize);
    }

    @Override
    public BytesInput decompress(BytesInput bytes, int uncompressedSize) throws IOException {
      return bytes;
    }

    @Override
    protected void release() {
    }

  }

  public class SnappyCompressor extends BytesCompressor {

    private ByteBuffer incoming;
    private ByteBuffer outgoing;

    public SnappyCompressor() {
      super();
    }

    @Override
    public BytesInput compress(BytesInput bytes) throws IOException {
      int maxOutputSize = Snappy.maxCompressedLength((int) bytes.size());
      ByteBuffer bufferIn = bytes.toByteBuffer();
      outgoing = ensure(outgoing, maxOutputSize);
      final int size;
      if (bufferIn.isDirect()) {
        size = Snappy.compress(bufferIn, outgoing);
      } else {
        this.incoming = ensure(this.incoming, (int) bytes.size());
        this.incoming.put(bufferIn);
        this.incoming.flip();
        size = Snappy.compress(this.incoming, outgoing);
      }

      return BytesInput.from(outgoing, 0, (int) size);
    }

    @Override
    public CompressionCodecName getCodecName() {
      return CompressionCodecName.SNAPPY;
    }

    @Override
    protected void release() {
      outgoing = DirectCodecFactory.this.release(outgoing);
      incoming = DirectCodecFactory.this.release(incoming);
    }

  }

  public static class NoopCompressor extends BytesCompressor {

    @Override
    public BytesInput compress(BytesInput bytes) throws IOException {
      return bytes;
    }

    @Override
    public CompressionCodecName getCodecName() {
      return CompressionCodecName.UNCOMPRESSED;
    }

    @Override
    protected void release() {
    }

  }

  public static class ByteBufBytesInput extends BytesInput {
    private final ByteBuf buf;
    private final ByteBuffer byteBuffer;
    private final int length;

    public ByteBufBytesInput(ByteBuf buf) {
      this(buf, 0, buf.capacity());
    }

    public ByteBufBytesInput(ByteBuf buf, int offset, int length) {
      super();
      if(buf.capacity() == length && offset == 0){
        this.buf = buf;
      }else{
        this.buf = buf.slice(offset, length);
      }

      this.byteBuffer = this.buf.nioBuffer();
      this.length = length;
    }

    @Override
    public void writeAllTo(OutputStream out) throws IOException {
      final WritableByteChannel outputChannel = Channels.newChannel(out);
      outputChannel.write(byteBuffer);
    }

    @Override
    public ByteBuffer toByteBuffer() throws IOException {
      return byteBuffer;
    }

    @Override
    public long size() {
      return length;
    }
  }


  public abstract class DirectBytesDecompressor extends CodecFactory.BytesDecompressor {
    public abstract void decompress(DrillBuf input, int compressedSize, DrillBuf output, int uncompressedSize)
        throws IOException;
  }



}

