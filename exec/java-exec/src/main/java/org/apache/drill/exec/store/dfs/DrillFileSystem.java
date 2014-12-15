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
package org.apache.drill.exec.store.dfs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.EnumSet;
import java.util.List;

import com.google.common.collect.Lists;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.ops.OperatorStats;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Options.ChecksumOpt;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

public class DrillFileSystem extends FileSystem {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillFileSystem.class);

  private static final String ERROR_MSG = "'%s' is not supported in DrillFileSystem implementation";

  private final FileSystem underlyingFs;
  private final OperatorStats operatorStats;

  public DrillFileSystem(Configuration fsConf) throws IOException {
    this.underlyingFs = FileSystem.get(fsConf);
    this.operatorStats = null;
  }

  public DrillFileSystem(FileSystem fs, OperatorStats operatorStats) {
    this.underlyingFs = fs;
    this.operatorStats = operatorStats;
  }

  @Override
  public Configuration getConf() {
    return underlyingFs.getConf();
  }

  @Override
  public FSDataInputStream open(Path f, int bufferSize) throws IOException {
    if (operatorStats != null) {
      return new DrillFSDataInputStream(underlyingFs.open(f, bufferSize), operatorStats);
    }

    return underlyingFs.open(f, bufferSize);
  }

  @Override
  public FileStatus getFileStatus(Path f) throws IOException {
    return underlyingFs.getFileStatus(f);
  }

  @Override
  public Path getWorkingDirectory() {
    return underlyingFs.getWorkingDirectory();
  }

  @Override
  public FSDataOutputStream append(Path f, int bufferSize, Progressable progress) throws IOException {
    throw new UnsupportedOperationException(String.format(ERROR_MSG, "append"));
  }

  @Override
  public boolean mkdirs(Path f, FsPermission permission) throws IOException {
    return underlyingFs.mkdirs(f, permission);
  }

  @Override
  public boolean mkdirs(Path folderPath) throws IOException {
    if (!underlyingFs.exists(folderPath)) {
      return underlyingFs.mkdirs(folderPath);
    } else if (!underlyingFs.getFileStatus(folderPath).isDir()) {
      throw new IOException("The specified folder path exists and is not a folder.");
    }
    return false;
  }

  @Override
  public FSDataOutputStream create(Path f, FsPermission permission, EnumSet<CreateFlag> flags, int bufferSize, short replication, long blockSize, Progressable progress, ChecksumOpt checksumOpt) throws IOException {
    return underlyingFs.create(f, permission, flags, bufferSize, replication, blockSize, progress, checksumOpt);
  }

  @Override
  public FSDataOutputStream create(Path f, FsPermission permission, boolean overwrite, int bufferSize, short replication, long blockSize, Progressable progress) throws IOException {
    return underlyingFs.create(f, permission, overwrite, bufferSize, replication, blockSize, progress);
  }

  @Override
  public FileStatus[] listStatus(Path f) throws FileNotFoundException, IOException {
    return underlyingFs.listStatus(f);
  }

  @Override
  public void setWorkingDirectory(Path new_dir) {
    underlyingFs.setWorkingDirectory(new_dir);
  }

  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    return underlyingFs.rename(src, dst);
  }

  @Override
  public boolean delete(Path f, boolean recursive) throws IOException {
    return underlyingFs.delete(f, recursive);
  }

  @Override
  public URI getUri() {
    return underlyingFs.getUri();
  }

  public List<FileStatus> list(boolean recursive, Path... paths) throws IOException {
    if (recursive) {
      List<FileStatus> statuses = Lists.newArrayList();
      for (Path p : paths) {
        addRecursiveStatus(underlyingFs.getFileStatus(p), statuses);
      }
      return statuses;

    } else {
      return Lists.newArrayList(underlyingFs.listStatus(paths));
    }
  }


  private void addRecursiveStatus(FileStatus parent, List<FileStatus> listToFill) throws IOException {
    if (parent.isDir()) {
      Path pattern = new Path(parent.getPath(), "*");
      FileStatus[] sub = underlyingFs.globStatus(pattern, new DrillPathFilter());
      for(FileStatus s : sub){
        if (s.isDir()) {
          addRecursiveStatus(s, listToFill);
        } else {
          listToFill.add(s);
        }
      }
    } else {
      listToFill.add(parent);
    }
  }
}
