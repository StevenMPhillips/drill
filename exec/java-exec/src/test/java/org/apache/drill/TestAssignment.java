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
package org.apache.drill;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Iterators;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.drill.exec.physical.EndpointAffinity;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.store.TimedRunnable;
import org.apache.drill.exec.store.parquet.ParquetGroupScan.RowGroupInfo;
import org.apache.drill.exec.store.schedule.AffinityCreator;
import org.apache.drill.exec.store.schedule.AssignmentCreator;
import org.apache.drill.exec.store.schedule.EndpointByteMap;
import org.apache.drill.exec.store.schedule.EndpointByteMapImpl;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Created by sphillips on 3/3/15.
 */
public class TestAssignment extends BaseTestQuery {
  private static final int NUMBER_DRILLBITS = 30;
  private static final long FILE_SIZE = 1000000;
  private static final String PATH_FILE = "/Users/sphillips/filenames";
  private static final String BLOCK_FILE = "/Users/sphillips/blocklocations";
  private static final int NUMBER_TASKS = 600;
  private static final float AFFINITY_FACTOR = 1.2f;
  private static final int THREADS = 1;

  @Test
  public void t() throws Exception {
    List<RowGroupInfo> rowGroups = getRowGroups(PATH_FILE, BLOCK_FILE);
    List<EndpointAffinity> affinities =  AffinityCreator.getAffinityMap(rowGroups);
    Set<DrillbitEndpoint> affinedEndpoints = Sets.newLinkedHashSet();
    for (EndpointAffinity affinity : affinities) {
      affinedEndpoints.add(affinity.getEndpoint());
    }
    Iterator<DrillbitEndpoint> cycleIterator = Iterators.cycle(affinedEndpoints);
    List<DrillbitEndpoint> endpointList = Lists.newArrayList();
    int affinedSlots = Math.min((Math.max(1, (int) (AFFINITY_FACTOR*NUMBER_TASKS/NUMBER_DRILLBITS)) * affinedEndpoints.size()),
        NUMBER_TASKS);
    for (int i = 0; i < affinedSlots; i++) {
      endpointList.add(cycleIterator.next());
    }
    Iterator<DrillbitEndpoint> unaffinedIterator = getUnaffinedEnpointIterator(NUMBER_DRILLBITS - affinedEndpoints.size());

    while (endpointList.size() < NUMBER_TASKS) {
      endpointList.add(unaffinedIterator.next());
    }

    Stopwatch watch = new Stopwatch();
    watch.start();
    ListMultimap<Integer, RowGroupInfo> assignments = AssignmentCreator.getMappings(endpointList, rowGroups, THREADS);
    System.out.printf("Took %d ms to get assignments\n", watch.elapsed(TimeUnit.MILLISECONDS));


//    for (Integer i : assignments.keySet()) {
//      System.out.println(i + " " + assignments.get(i).size());
//    }

  }

  private static Iterator<DrillbitEndpoint> getUnaffinedEnpointIterator(int n) {
    List<DrillbitEndpoint> endpointlist = Lists.newArrayList();
    for (int i = 0; i < n; i++) {
      endpointlist.add(DrillbitEndpoint.newBuilder().setAddress("host"+n).build());
    }
    return Iterators.cycle(endpointlist);
  }

  private List<RowGroupInfo> getRowGroups(String paths, String blocks) throws Exception {
    BufferedReader pathFile = new BufferedReader(new FileReader(paths));
    BufferedReader blockFile = new BufferedReader(new FileReader(blocks));

    List<String> files = Lists.newArrayList();
    List<List<String>> blockLocations = Lists.newArrayList();

    String line;
    while ((line = pathFile.readLine()) != null) {
      files.add(line);
    }
    while ((line = blockFile.readLine()) != null) {
      String[] hosts = line.split(" ");
      blockLocations.add(Lists.newArrayList(hosts));
    }

    assert files.size() == blockLocations.size() : "Lists not the same size";

    List<RowGroupInfo> rowGroups = Lists.newArrayList();

    for (int i = 0; i < files.size(); i ++) {
      RowGroupInfo rgi = new RowGroupInfo(files.get(i), 4, FILE_SIZE, 0);
      EndpointByteMap byteMap = new EndpointByteMapImpl();
      for (String location : blockLocations.get(i)) {
        String address = location.split(":")[0];
        DrillbitEndpoint ep = DrillbitEndpoint.newBuilder().setAddress(address).build();
        byteMap.add(ep, FILE_SIZE);
      }
      rgi.setEndpointByteMap(byteMap);
      rowGroups.add(rgi);
    }
    return rowGroups;
  }
}
