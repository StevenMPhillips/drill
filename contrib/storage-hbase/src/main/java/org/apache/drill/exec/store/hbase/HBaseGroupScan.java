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
package org.apache.drill.exec.store.hbase;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.NavigableMap;
import java.util.concurrent.TimeUnit;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.exec.physical.EndpointAffinity;
import org.apache.drill.exec.physical.OperatorCost;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.Size;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;

import parquet.org.codehaus.jackson.annotate.JsonCreator;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ArrayListMultimap;


@JsonTypeName("hbase-scan")
public class HBaseGroupScan extends AbstractGroupScan {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HBaseGroupScan.class);

  private ArrayListMultimap<Integer, HBaseSubScan.HBaseSubScanReadEntry> mappings;
  private Stopwatch watch = new Stopwatch();

  public String getTableName() {
    return tableName;
  }

  @JsonProperty("storage")
  public HBaseStoragePluginConfig getStorageConfig() {
    return this.storagePluginConfig;
  }

  private String tableName;
  private Collection<DrillbitEndpoint> availableEndpoints;
  private HBaseStoragePlugin storageEngine;
  private HBaseStoragePluginConfig storagePluginConfig;
  private FileSystem fs;
  private final FieldReference ref;
  private List<EndpointAffinity> endpointAffinities;

  private NavigableMap<HRegionInfo,ServerName> regionsMap;

  @JsonCreator
  public HBaseGroupScan(@JsonProperty("entries") List<HTableReadEntry> entries,
                          @JsonProperty("storage") HBaseStoragePluginConfig storageEngineConfig,
                          @JacksonInject StoragePluginRegistry engineRegistry,
                          @JsonProperty("ref") FieldReference ref
                           )throws IOException, ExecutionSetupException {
    Preconditions.checkArgument(entries.size() == 1);
    engineRegistry.init(DrillConfig.create());
    this.storageEngine = (HBaseStoragePlugin) engineRegistry.getEngine(storageEngineConfig);
    this.availableEndpoints = storageEngine.getContext().getBits();
    this.storagePluginConfig = storageEngineConfig;
    this.tableName = entries.get(0).getTableName();
    this.ref = ref;
    getRegionInfos();
  }

  public HBaseGroupScan(String tableName, HBaseStoragePlugin storageEngine, FieldReference ref) throws IOException {
    this.storageEngine = storageEngine;
    this.storagePluginConfig = storageEngine.getEngineConfig();
    this.availableEndpoints = storageEngine.getContext().getBits();
    this.tableName = tableName;
    this.ref = ref;
    getRegionInfos();
  }

  protected void getRegionInfos() throws IOException {
    logger.debug("Getting region locations");
    HTable table = new HTable(storagePluginConfig.conf, tableName);
    regionsMap = table.getRegionLocations();
    table.close();
  }

  /**
   * Calculates the affinity each endpoint has for this scan, by adding up the affinity each endpoint has for each
   * rowGroup
   * @return a list of EndpointAffinity objects
   */
  @Override
  public List<EndpointAffinity> getOperatorAffinity() {
    watch.reset();
    watch.start();
    if (this.endpointAffinities == null) {
      HashMap<DrillbitEndpoint, Float> affinities = new HashMap<>();
//      for (RowGroupInfo entry : rowGroupInfos) {
//        for (DrillbitEndpoint d : entry.getEndpointBytes().keySet()) {
//          long bytes = entry.getEndpointBytes().get(d);
//          float affinity = (float)bytes / (float)totalBytes;
//          logger.debug("RowGroup: {} Endpoint: {} Bytes: {}", entry.getRowGroupIndex(), d.getAddress(), bytes);
//          if (affinities.keySet().contains(d)) {
//            affinities.put(d, affinities.get(d) + affinity);
//          } else {
//            affinities.put(d, affinity);
//          }
//        }
//      }
      List<EndpointAffinity> affinityList = new LinkedList<>();
      for (DrillbitEndpoint d : affinities.keySet()) {
        logger.debug("Endpoint {} has affinity {}", d.getAddress(), affinities.get(d).floatValue());
        affinityList.add(new EndpointAffinity(d,affinities.get(d).floatValue()));
      }
      this.endpointAffinities = affinityList;
    }
    watch.stop();
    logger.debug("Took {} ms to get operator affinity", watch.elapsed(TimeUnit.MILLISECONDS));
    return this.endpointAffinities;
  }


  static final double[] ASSIGNMENT_CUTOFFS = {0.99, 0.50, 0.25, 0.01};

  /**
   *
   * @param incomingEndpoints
   */
  @Override
  public void applyAssignments(List<DrillbitEndpoint> incomingEndpoints) {
    watch.reset();
    watch.start();
    Preconditions.checkArgument(incomingEndpoints.size() <= regionsMap.size(),
        String.format("Incoming endpoints %d is greater than number of row groups %d", incomingEndpoints.size(), regionsMap.size()));
    mappings = ArrayListMultimap.create();
    int i = -1;
    for (HRegionInfo reiongInfo : regionsMap.keySet()) {
      logger.debug("creating read entry. start key: {} end key: {}", Bytes.toStringBinary(reiongInfo.getStartKey()), Bytes.toStringBinary(reiongInfo.getEndKey()));
      HBaseSubScan.HBaseSubScanReadEntry p = new HBaseSubScan.HBaseSubScanReadEntry(
          tableName, Bytes.toStringBinary(reiongInfo.getStartKey()), Bytes.toStringBinary(reiongInfo.getEndKey()));
      mappings.put((++i % incomingEndpoints.size()), p);
    }
  }

  @Override
  public HBaseSubScan getSpecificScan(int minorFragmentId) {
    return new HBaseSubScan(storageEngine, storagePluginConfig, mappings.get(minorFragmentId), ref);
  }

  public FieldReference getRef() {
    return ref;
  }

  @Override
  public int getMaxParallelizationWidth() {
    return regionsMap.size();
  }

  @Override
  public OperatorCost getCost() {
    //TODO Figure out how to properly calculate cost
    return new OperatorCost(1,1,1,1);
  }

  @Override
  public Size getSize() {
    // TODO - this is wrong, need to populate correctly
    return new Size(10,10);
  }

  @Override
  @JsonIgnore
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) {
    Preconditions.checkArgument(children.isEmpty());
    //TODO return copy of self
    return this;
  }

}
