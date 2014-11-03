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
package org.apache.drill.exec.store.sys.zk;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.shared.SharedCount;
import org.apache.curator.framework.recipes.shared.SharedCountListener;
import org.apache.curator.framework.recipes.shared.SharedCountReader;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.drill.exec.coord.ClusterCoordinator;
import org.apache.drill.exec.coord.zk.ZKClusterCoordinator;
import org.apache.drill.exec.exception.DrillbitStartupException;
import org.apache.drill.exec.store.dfs.shim.DrillFileSystem;
import org.apache.drill.exec.store.dfs.shim.FileSystemCreator;
import org.apache.drill.exec.store.sys.PStore;
import org.apache.drill.exec.store.sys.PStoreConfig;
import org.apache.drill.exec.store.sys.PStoreProvider;
import org.apache.drill.exec.store.sys.PStoreRegistry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.annotations.VisibleForTesting;

public class ZkPStoreProvider implements PStoreProvider{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ZkPStoreProvider.class);

  private static final String DRILL_EXEC_SYS_STORE_PROVIDER_ZK_BLOBROOT = "drill.exec.sys.store.provider.zk.blobroot";

  private final CuratorFramework curator;

  private final DrillFileSystem fs;

  private final Path blobRoot;

  public ZkPStoreProvider(PStoreRegistry registry) throws DrillbitStartupException {
    ClusterCoordinator coord = registry.getClusterCoordinator();
    if (!(coord instanceof ZKClusterCoordinator)) {
      throw new DrillbitStartupException("A ZkPStoreProvider was created without a ZKClusterCoordinator.");
    }
    this.curator = ((ZKClusterCoordinator)registry.getClusterCoordinator()).getCurator();

    if (registry.getConfig().hasPath(DRILL_EXEC_SYS_STORE_PROVIDER_ZK_BLOBROOT)) {
      blobRoot = new Path(registry.getConfig().getString(DRILL_EXEC_SYS_STORE_PROVIDER_ZK_BLOBROOT));
    } else {
      String drillLogDir = System.getenv("DRILL_LOG_DIR");
      if (drillLogDir == null) {
        drillLogDir = "/var/log/drill";
      }
      blobRoot = new Path(new File(drillLogDir).getAbsoluteFile().toURI());
    }
    Configuration fsConf = new Configuration();
    fsConf.set(FileSystem.FS_DEFAULT_NAME_KEY, blobRoot.toUri().toString());
    try {
      fs = FileSystemCreator.getFileSystem(registry.getConfig(), fsConf);
      fs.mkdirs(blobRoot);
    } catch (IOException e) {
      throw new DrillbitStartupException("Unable to initialize blob storage.", e);
    }

  }

  @VisibleForTesting
  public ZkPStoreProvider(CuratorFramework curator) {
    this.curator = curator;
    this.fs = null;
    String drillLogDir = System.getenv("DRILL_LOG_DIR");
    if (drillLogDir == null) {
      drillLogDir = "/var/log/drill";
    }
    blobRoot = new Path(new File(drillLogDir).getAbsoluteFile().toURI());
  }

  @Override
  public void close() {
  }

  @Override
  public <V> PStore<V> getPStore(PStoreConfig<V> store) throws IOException {
    return new ZkPStore<V>(curator, fs, blobRoot, store);
  }

  @Override
  public void start() {
  }

  public DistributedLatch getDistributedLatch(String name, int seed) {
    return new ZKLatch(this.curator, name, seed);
  }

  public class ZKLatch implements DistributedLatch {
    SharedCount count;
    CountDownLatch latch = new CountDownLatch(1);

    public ZKLatch(CuratorFramework framework, String name, int seed) {
      count = new SharedCount(framework, name, seed);
      SharedCountListener countListener = new SharedCountListener() {
        @Override
        public void countHasChanged(SharedCountReader sharedCountReader, int i) throws Exception {
          if (sharedCountReader.getCount() == 0) {
            latch.countDown();
          }
        }

        @Override
        public void stateChanged(CuratorFramework client, ConnectionState newState) {

        }
      };
      count.addListener(countListener);
      try {
        count.start();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public int getCount() {
      return count.getCount();
    }

    @Override
    public void countDown() {
      boolean success = false;
      while (!success) {
        synchronized(curator) {
          int currentCount = count.getCount();
          try {
            success = count.trySetCount(currentCount - 1);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        }
      }
    }

    @Override
    public void await() throws InterruptedException {
      if (count.getCount() == 0) {
        return;
      }
      latch.await();
    }
  }

}
