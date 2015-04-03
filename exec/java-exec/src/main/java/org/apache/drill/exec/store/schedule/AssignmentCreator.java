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

import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import com.carrotsearch.hppc.cursors.ObjectLongCursor;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;

/**
 * The AssignmentCreator is responsible for assigning a set of work units to the available slices.
 */
public class AssignmentCreator<T extends CompleteWork> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AssignmentCreator.class);


  private static Comparator<Entry<DrillbitEndpoint,Long>> comparator = new Comparator<Entry<DrillbitEndpoint,Long>>() {
    @Override
    public int compare(Entry<DrillbitEndpoint, Long> o1, Entry<DrillbitEndpoint,Long> o2) {
      int v = (int) (o1.getValue() - o2.getValue());
      if (v != 0) {
        return v;
      }
      return o1.getKey().getAddress().compareTo(o2.getKey().getAddress());
    }
  };

  public static <T extends CompleteWork> ListMultimap<Integer, T> getMappings(List<DrillbitEndpoint> incomingEndpoints, List<T> units) {
    Stopwatch watch = new Stopwatch();
    watch.start();
    int maxWork = (int) Math.ceil(units.size() / ((float) incomingEndpoints.size()));
    LinkedList<WorkEndpointListPair<T>> workList = getWorkList(units);
    LinkedList<WorkEndpointListPair<T>> unassignedWorkList = Lists.newLinkedList();
    Map<DrillbitEndpoint,FragIteratorWrapper> endpointIterators = getEndpointIterators(incomingEndpoints, maxWork);
    ArrayListMultimap<Integer, T> mappings = ArrayListMultimap.create();

    outer: for (WorkEndpointListPair<T> workPair : workList) {
      List<DrillbitEndpoint> endpoints = workPair.sortedEndpoints;
      for (DrillbitEndpoint endpoint : endpoints) {
        FragIteratorWrapper iteratorWrapper = endpointIterators.get(endpoint);
        if (iteratorWrapper.count < iteratorWrapper.maxCount) {
          Integer assignment = iteratorWrapper.iter.next();
          iteratorWrapper.count++;
          mappings.put(assignment, workPair.work);
          continue outer;
        }
      }
      unassignedWorkList.add(workPair);
    }

    outer: for (FragIteratorWrapper iteratorWrapper : endpointIterators.values()) {
      while (iteratorWrapper.count < iteratorWrapper.maxCount) {
        WorkEndpointListPair<T> workPair = unassignedWorkList.poll();
        if (workPair == null) {
          break outer;
        }
        Integer assignment = iteratorWrapper.iter.next();
        iteratorWrapper.count++;
        mappings.put(assignment, workPair.work);
      }
    }

    logger.debug("Took {} ms to assign {} work units to {} fragments", watch.elapsed(TimeUnit.MILLISECONDS), units.size(), incomingEndpoints.size());
    return mappings;
  }

  private static <T extends CompleteWork> LinkedList<WorkEndpointListPair<T>> getWorkList(List<T> units) {
    Stopwatch watch = new Stopwatch();
    watch.start();
    LinkedList<WorkEndpointListPair<T>> workList = Lists.newLinkedList();
    for (T work : units) {
      List<Map.Entry<DrillbitEndpoint,Long>> entries = Lists.newArrayList();
      for (ObjectLongCursor<DrillbitEndpoint> cursor : work.getByteMap()) {
        final DrillbitEndpoint ep = cursor.key;
        final Long val = cursor.value;
        Map.Entry<DrillbitEndpoint,Long> entry = new Entry() {

          @Override
          public Object getKey() {
            return ep;
          }

          @Override
          public Object getValue() {
            return val;
          }

          @Override
          public Object setValue(Object value) {
            throw new UnsupportedOperationException();
          }
        };
        entries.add(entry);
      }
      Collections.sort(entries, comparator);
      List<DrillbitEndpoint> sortedEndpoints = Lists.newArrayList();
      for (Entry<DrillbitEndpoint,Long> entry : entries) {
        sortedEndpoints.add(entry.getKey());
      }
      workList.add(new WorkEndpointListPair<T>(work, sortedEndpoints));
    }
    return workList;
  }

  private static class WorkEndpointListPair<T> {
    T work;
    List<DrillbitEndpoint> sortedEndpoints;

    WorkEndpointListPair(T work, List<DrillbitEndpoint> sortedEndpoints) {
      this.work = work;
      this.sortedEndpoints = sortedEndpoints;
    }
  }

  private static Map<DrillbitEndpoint,FragIteratorWrapper> getEndpointIterators(List<DrillbitEndpoint> endpoints, int maxWork) {
    Stopwatch watch = new Stopwatch();
    watch.start();
    Map<DrillbitEndpoint,FragIteratorWrapper> map = Maps.newLinkedHashMap();
    Map<DrillbitEndpoint,List<Integer>> mmap = Maps.newLinkedHashMap();
    for (int i = 0; i < endpoints.size(); i++) {
      DrillbitEndpoint endpoint = endpoints.get(i);
      List<Integer> intList = mmap.get(endpoints.get(i));
      if (intList == null) {
        intList = Lists.newArrayList();
      }
      intList.add(Integer.valueOf(i));
      mmap.put(endpoint, intList);
    }

    for (DrillbitEndpoint endpoint : mmap.keySet()) {
      FragIteratorWrapper wrapper = new FragIteratorWrapper();
      wrapper.iter = Iterators.cycle(mmap.get(endpoint));
      wrapper.maxCount = maxWork * mmap.get(endpoint).size();
      map.put(endpoint, wrapper);
    }
    return map;
  }

  private static class FragIteratorWrapper {
    int count = 0;
    int maxCount;
    Iterator<Integer> iter;
  }

}
