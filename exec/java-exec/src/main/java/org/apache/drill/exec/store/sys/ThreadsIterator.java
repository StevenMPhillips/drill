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
package org.apache.drill.exec.store.sys;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;

import com.google.common.collect.Lists;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.ThreadStatCollector;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;

public class ThreadsIterator implements Iterator<Object> {

  private final FragmentContext context;
  private final Iterator<Long> threadIdIterator;
  private final ThreadMXBean threadMXBean;
  private final ThreadStatCollector threadStatCollector;

  public ThreadsIterator(final FragmentContext context) {
    this.context = context;
    threadMXBean = ManagementFactory.getThreadMXBean();
    final long[] ids = threadMXBean.getAllThreadIds();
    threadIdIterator = new ArrayList<Long>() {{
      for (long id : ids) {
        add(id);
      }
    }}.iterator();
    this.threadStatCollector = context.getDrillbitContext().getThreadStatCollector();
  }

  public boolean hasNext() {
    return threadIdIterator.hasNext();
  }

  @Override
  public Object next() {
    long id = threadIdIterator.next();
    ThreadInfo currentThread = threadMXBean.getThreadInfo(id);
    final ThreadSummary threadSummary = new ThreadSummary();

    final DrillbitEndpoint endpoint = context.getIdentity();
    threadSummary.hostname = endpoint.getAddress();
    threadSummary.user_port = endpoint.getUserPort();
    threadSummary.threadName = currentThread.getThreadName();
    threadSummary.threadState = currentThread.getThreadState().name();
    threadSummary.threadId = currentThread.getThreadId();
    threadSummary.inNative = currentThread.isInNative();
    threadSummary.suspended = currentThread.isSuspended();
    threadSummary.cpuTime = threadStatCollector.getCpuTrailingAverage(id, 1);
    threadSummary.userTime = threadStatCollector.getUserTrailingAverage(id, 1);

    return threadSummary;
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }

  public static class ThreadSummary {
    //name, priority, state, id, thread-level cpu stats

    // should this be in all distributed system tables?
    public String hostname;
    public long user_port;

    // pulled out of java.lang.management.ThreadInfo
    public String threadName;
    public long threadId;
    public boolean inNative;
    public boolean suspended;
    public String threadState;
    public Integer cpuTime;
    public Integer userTime;
  }
}
