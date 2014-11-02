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
package org.apache.drill.exec.store.internal;

import com.google.common.base.Joiner;
import org.apache.drill.exec.cache.VectorAccessibleSerializable;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.WritableBatch;
import org.apache.drill.exec.store.AbstractRecordWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.Map;


public class InternalWriter extends AbstractRecordWriter {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(InternalWriter.class);

  private String location;
  private String prefix;

  private FileSystem fs = null;
  private FSDataOutputStream outputStream;
  private BufferAllocator allocator;
  private String extension;
  private int index = 0;

  public InternalWriter(BufferAllocator allocator) {
    this.allocator = allocator;
  }

  public int writeBatch(RecordBatch batch) throws IOException {
    WritableBatch writableBatch = batch.getWritableBatch();
    VectorAccessibleSerializable va = new VectorAccessibleSerializable(writableBatch, allocator);
    va.writeToStream(outputStream);
    return batch.getRecordCount();
  }

  @Override
  public void init(Map<String, String> writerOptions) throws IOException {
    this.location = writerOptions.get("location");
    this.prefix = writerOptions.get("prefix");
    this.extension = writerOptions.get("extension");

    Configuration conf = new Configuration();
    conf.set(FileSystem.FS_DEFAULT_NAME_KEY, writerOptions.get(FileSystem.FS_DEFAULT_NAME_KEY));
    this.fs = FileSystem.get(conf);
    newStream();
  }

  private void newStream() throws IOException {
    cleanup();
    Path fileName = new Path(location, prefix + "_" + index++ + "." + extension);
    outputStream = fs.create(fileName);
  }

  @Override
  public void updateSchema(BatchSchema schema) throws IOException {
  }

  @Override
  public void startRecord() throws IOException {

  }

  @Override
  public void endRecord() throws IOException {

  }

  @Override
  public void abort() throws IOException {

  }

  @Override
  public void cleanup() throws IOException {
    if (outputStream != null) {
      outputStream.close();
    }
  }
}
