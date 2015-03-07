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
package org.apache.drill.exec.store.schedule;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import org.apache.drill.exec.store.TimedRunnable;

/**
 * The AssignmentCreator is responsible for assigning a set of work units to the available slices.
 */
public class AssignmentCreator<T extends CompleteWork> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AssignmentCreator.class);

  private final ArrayListMultimap<Integer, T> mappings;
  private final List<DrillbitEndpoint> endpoints;
  private int offset;

  public static <T extends CompleteWork> ListMultimap<Integer, T> getMappings(List<DrillbitEndpoint> incomingEndpoints,
                                                                              List<T> units, int parallelization) throws IOException {
    List<TimedRunnable<ListMultimap<Integer,T>>> assigners = Lists.newArrayList();

    int offsetInc = incomingEndpoints.size() / parallelization;
    int inc = units.size() / parallelization;
    for (int i = 0; i < parallelization; i++) {
      int start = i * inc;
      int end = start + inc;
      assigners.add(new Assigner(incomingEndpoints, units.subList(start, end), offsetInc * i));
    }

    List<ListMultimap<Integer,T>> assignmentLists = TimedRunnable.run("Applying assignments", logger,
        assigners, parallelization);
    return combineMaps(assignmentLists);
  }

  private static <Integer,V> ListMultimap<Integer,V> combineMaps(List<ListMultimap<Integer,V>> maps) {
    ListMultimap<Integer,V> multiMap = ArrayListMultimap.create();
    for (ListMultimap map : maps) {
      multiMap.putAll(map);
    }
    return multiMap;
  }

  /**
   * Given a set of endpoints to assign work to, attempt to evenly assign work based on affinity of work units to
   * Drillbits.
   *
   * @param incomingEndpoints
   *          The set of nodes to assign work to. Note that nodes can be listed multiple times if we want to have
   *          multiple slices on a node working on the task simultaneously.
   * @param units
   *          The work units to assign.
   * @return ListMultimap of Integer > List<CompleteWork> (based on their incoming order) to with
   */
  public static <T extends CompleteWork> ListMultimap<Integer, T> getMappingsInternal(List<DrillbitEndpoint> incomingEndpoints,
                                                                                      List<T> units, int offset) {
    AssignmentCreator<T> creator = new AssignmentCreator<T>(incomingEndpoints, units, offset);
    return creator.mappings;
  }

  private AssignmentCreator(List<DrillbitEndpoint> incomingEndpoints, List<T> units, int offset) {
    this.offset = offset;
    logger.debug("Assigning {} units to {} endpoints", units.size(), incomingEndpoints.size());
    Stopwatch watch = new Stopwatch();
    watch.start();

    Preconditions.checkArgument(incomingEndpoints.size() <= units.size(), String.format("Incoming endpoints %d "
        + "is greater than number of row groups %d", incomingEndpoints.size(), units.size()));
    this.mappings = ArrayListMultimap.create();
    this.endpoints = Lists.newLinkedList(incomingEndpoints);

    ArrayList<T> rowGroupList = new ArrayList<>(units);
    scanAndAssign(rowGroupList, false, false);
    scanAndAssign(rowGroupList, true, false);
    scanAndAssign(rowGroupList, true, true);

    logger.debug("Took {} ms to apply assignments", watch.elapsed(TimeUnit.MILLISECONDS));
    Preconditions.checkState(rowGroupList.isEmpty(), "All readEntries should be assigned by now, but some are still unassigned");
    Preconditions.checkState(!units.isEmpty());

  }

  /**
   * Attempts to assign workunits to endpoints
   * @param workunits the workunits to assign
   * @param assignAllToEmpty assign remaining work units to any empty endpoints, regardless of affinity
   * @param assignAll assign all remaining work units to an endpoint, regardless of affinity or whether that endpoint is empty
   */
  private void scanAndAssign(List<T> workunits, boolean assignAllToEmpty, boolean assignAll) {
    final int numEndpoints = endpoints.size();
    Collections.sort(workunits);
    int fragmentPointer = offset;
    int maxAssignments = (int) (workunits.size() / endpoints.size());

    if (maxAssignments < 1) {
      maxAssignments = 1;
    }

    for (Iterator<T> iter = workunits.iterator(); iter.hasNext();) {
      T unit = iter.next();
      int minorFragmentId = fragmentPointer;
      for (int i = 0; i < endpoints.size(); i++, minorFragmentId++) {
        if (minorFragmentId == numEndpoints) {
          minorFragmentId = 0;
        }
        boolean haveAffinity = false;
        boolean slotEmpty = false;

        if (!assignAll) {
          DrillbitEndpoint currentEndpoint = endpoints.get(minorFragmentId);
          EndpointByteMap endpointByteMap = unit.getByteMap();
          Long affinity = endpointByteMap.get(currentEndpoint);
          haveAffinity = affinity != null && affinity > 0.5;
          slotEmpty = !mappings.containsKey(minorFragmentId);
        }

        if (assignAll
            || (assignAllToEmpty && slotEmpty)
            || (slotEmpty || mappings.get(minorFragmentId).size() < maxAssignments) &&
            haveAffinity) {

          mappings.put(minorFragmentId, unit);
          iter.remove();
          fragmentPointer = minorFragmentId + 1;
          if (fragmentPointer == numEndpoints) {
            fragmentPointer = 0;
          }
          break;
        }
      }

    }
  }

  private static class Assigner<X extends CompleteWork> extends TimedRunnable<ListMultimap<Integer, X>> {

    private List<DrillbitEndpoint> endpoints;
    private List<X> workUnits;
    private int offset;

    public Assigner(List<DrillbitEndpoint> endpoints, List<X> workUnits, int offset) {
      this.endpoints = endpoints;
      this.workUnits = workUnits;
      this.offset = offset;
    }

    @Override
    protected ListMultimap<Integer, X> runInner() throws Exception {
      return AssignmentCreator.getMappingsInternal(endpoints, workUnits, offset);
    }

    @Override
    protected IOException convertToIOException(Exception e) {
      return null;
    }
  }

}
