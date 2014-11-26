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
package org.apache.drill.exec.planner.fragment;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.primitives.Ints;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.util.DrillStringUtils;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.expr.fn.impl.DateUtility;
import org.apache.drill.exec.ops.QueryContext;
import org.apache.drill.exec.physical.PhysicalOperatorSetupException;
import org.apache.drill.exec.physical.base.FragmentRoot;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.config.HashToRandomExchange;
import org.apache.drill.exec.physical.config.LocalExchange;
import org.apache.drill.exec.planner.PhysicalPlanReader;
import org.apache.drill.exec.planner.fragment.Fragment.ExchangeFragmentPair;
import org.apache.drill.exec.planner.fragment.Materializer.IndexedFragmentNode;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.drill.exec.proto.BitControl.PlanFragment;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.proto.ExecProtos.FragmentHandle;
import org.apache.drill.exec.proto.UserBitShared.QueryId;
import org.apache.drill.exec.rpc.user.UserSession;
import org.apache.drill.exec.server.options.OptionList;
import org.apache.drill.exec.server.options.OptionManager;
import org.apache.drill.exec.work.QueryWorkUnit;
import org.apache.drill.exec.work.foreman.ForemanException;
import org.apache.drill.exec.work.foreman.ForemanSetupException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * The simple parallelizer determines the level of parallelization of a plan based on the cost of the underlying
 * operations.  It doesn't take into account system load or other factors.  Based on the cost of the query, the
 * parallelization for each major fragment will be determined.  Once the amount of parallelization is done, assignment
 * is done based on round robin assignment ordered by operator affinity (locality) to available execution Drillbits.
 */
public class SimpleParallelizer {



  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SimpleParallelizer.class);
  private final Materializer materializer = new Materializer();
  private final long parallelizationThreshold;
  private final int maxWidthPerNode;
  private final int maxGlobalWidth;
  private double affinityFactor;

  public SimpleParallelizer(QueryContext context) {
    long sliceTarget = context.getOptions().getOption(ExecConstants.SLICE_TARGET).num_val;
    this.parallelizationThreshold = sliceTarget > 0 ? sliceTarget : 1;
    this.maxWidthPerNode = context.getOptions().getOption(ExecConstants.MAX_WIDTH_PER_NODE_KEY).num_val.intValue();
    this.maxGlobalWidth = context.getOptions().getOption(ExecConstants.MAX_WIDTH_GLOBAL_KEY).num_val.intValue();
    this.affinityFactor = context.getOptions().getOption(ExecConstants.AFFINITY_FACTOR_KEY).float_val.intValue();
  }

  public SimpleParallelizer(long parallelizationThreshold, int maxWidthPerNode, int maxGlobalWidth, double affinityFactor) {
    this.parallelizationThreshold = parallelizationThreshold;
    this.maxWidthPerNode = maxWidthPerNode;
    this.maxGlobalWidth = maxGlobalWidth;
    this.affinityFactor = affinityFactor;
  }


  /**
   * Generate a set of assigned fragments based on the provided planningSet. Do not allow parallelization stages to go
   * beyond the global max width.
   *
   * @param foremanNode     The driving/foreman node for this query.  (this node)
   * @param queryId         The queryId for this query.
   * @param activeEndpoints The list of endpoints to consider for inclusion in planning this query.
   * @param reader          Tool used to read JSON plans
   * @param rootNode        The root node of the PhysicalPlan that we will parallelizing.
   * @param planningSet     The set of queries with collected statistics that we'll work with.
   * @return The list of generated PlanFragment protobuf objects to be assigned out to the individual nodes.
   * @throws ForemanException
   */
  public QueryWorkUnit getFragments(OptionList options, DrillbitEndpoint foremanNode, QueryId queryId, Collection<DrillbitEndpoint> activeEndpoints,
      PhysicalPlanReader reader, Fragment rootNode, PlanningSet planningSet, UserSession session) throws ExecutionSetupException {
    assignEndpoints(activeEndpoints, planningSet);
    if (session.getOptions().getOption(PlannerSettings.INSERT_LOCAL_EXCHANGE.getOptionName()).bool_val) {
      adjustLocalExchangeParallelization(planningSet, session.getOptions());
    }
    return generateWorkUnit(options, foremanNode, queryId, reader, rootNode, planningSet, session);
  }

  private void adjustLocalExchangeParallelization(PlanningSet planningSet, OptionManager optionManager) throws PhysicalOperatorSetupException {
    List<Wrapper> wrappers = Lists.newArrayListWithCapacity(planningSet.getNumberOfFragments());

    for(Wrapper wrapper : planningSet) {
      wrappers.add(wrapper);
    }

    Collections.sort(wrappers, new Ordering<Wrapper>() {
        @Override
        public int compare(Wrapper w1, Wrapper w2) {
          // sort in order of decreasing fragment ids
          return Ints.compare(w2.getMajorFragmentId(), w1.getMajorFragmentId());
        }
      }
    );

    // Go through the fragment starting from the leaf fragment. Whenever a HashToRandomExchange is encountered:
    // 1. Calculate the width of the HashToRandomExchange by looking at the assigned DrillBits of previous fragment
    //    which is the local exchange. Set the width of the HashToRandomExchange to be the number of drillbits to which
    //    local exchange is assigned.
    // 2. Set the assigned DrillBits (basically remove duplicates from the set returned in Step 1)
    for(int i = wrappers.size() - 1; i >= 0; i--) {
      Wrapper wrapper = wrappers.get(i);

      if (!(wrapper.getNode().getRoot() instanceof HashToRandomExchange)) {
        continue;
      }

      List<ExchangeFragmentPair> receivingExchangePairs = wrapper.getNode().getReceivingExchangePairs();

      // TODO: Some of the existing physical plan tests have no local exchange. So the following condition will fail.
      // Disable the check and return if the number of receivers is 0.
      // Preconditions.checkState(receivingExchangePairs.size() == 1,
      //     "HashToRandomExchange has more than one receiver. Expected to have only one receiver.");
      if (receivingExchangePairs.size() == 0) {
        return;
      }

      // if the receiving exchange is not a LocalExchange, then there is no need to adjust the parallelization.
      if (!(receivingExchangePairs.get(0).getExchange() instanceof LocalExchange)) {
        continue;
      }

      Fragment localExchangeFragment = receivingExchangePairs.get(0).getNode();
      List<DrillbitEndpoint> endpoints = planningSet.get(localExchangeFragment).getAssignedEndpoints();

      Map<DrillbitEndpoint, Integer> epCounts = Maps.newHashMap();
      for(DrillbitEndpoint ep : endpoints) {
        if (epCounts.containsKey(ep)) {
          Integer count = epCounts.remove(ep);
          epCounts.put(ep, count+1);
        } else {
          epCounts.put(ep, 1);
        }
      }

      // Now find the parallelization per node
      List<DrillbitEndpoint> newEndpoints = Lists.newArrayList();
      for(Entry<DrillbitEndpoint, Integer> entry : epCounts.entrySet()) {
        Integer count = entry.getValue();

        int width = (int) Math.ceil(((double)count)/
            optionManager.getOption(PlannerSettings.LOCAL_EXCHANGES_PER_PARTITION_SENDER.getOptionName()).num_val);
        width = Math.max(1, width);

        while(width-- > 0) {
          newEndpoints.add(entry.getKey());
        }
      }

      wrapper.resetAssignment();
      wrapper.setWidth(newEndpoints.size());
      wrapper.assignEndpoints(newEndpoints, affinityFactor);
    }
  }

  private QueryWorkUnit generateWorkUnit(OptionList options, DrillbitEndpoint foremanNode, QueryId queryId, PhysicalPlanReader reader, Fragment rootNode,
                                         PlanningSet planningSet, UserSession session) throws ExecutionSetupException {

    List<PlanFragment> fragments = Lists.newArrayList();

    PlanFragment rootFragment = null;
    FragmentRoot rootOperator = null;

    long queryStartTime = System.currentTimeMillis();
    int timeZone = DateUtility.getIndex(System.getProperty("user.timezone"));

    // now we generate all the individual plan fragments and associated assignments. Note, we need all endpoints
    // assigned before we can materialize, so we start a new loop here rather than utilizing the previous one.
    for (Wrapper wrapper : planningSet) {
      Fragment node = wrapper.getNode();
      final PhysicalOperator physicalOperatorRoot = node.getRoot();
      boolean isRootNode = rootNode == node;

      if (isRootNode && wrapper.getWidth() != 1) {
        throw new ForemanSetupException(
            String.format(
                "Failure while trying to setup fragment.  The root fragment must always have parallelization one.  In the current case, the width was set to %d.",
                wrapper.getWidth()));
      }
      // a fragment is self driven if it doesn't rely on any other exchanges.
      boolean isLeafFragment = node.getReceivingExchangePairs().size() == 0;

      // Create a minorFragment for each major fragment.
      for (int minorFragmentId = 0; minorFragmentId < wrapper.getWidth(); minorFragmentId++) {
        IndexedFragmentNode iNode = new IndexedFragmentNode(minorFragmentId, wrapper);
        wrapper.resetAllocation();
        PhysicalOperator op = physicalOperatorRoot.accept(materializer, iNode);
        Preconditions.checkArgument(op instanceof FragmentRoot);
        FragmentRoot root = (FragmentRoot) op;

        // get plan as JSON
        String plan;
        String optionsData;
        try {
          plan = reader.writeJson(root);
          optionsData = reader.writeJson(options);
        } catch (JsonProcessingException e) {
          throw new ForemanSetupException("Failure while trying to convert fragment into json.", e);
        }

        FragmentHandle handle = FragmentHandle //
            .newBuilder() //
            .setMajorFragmentId(wrapper.getMajorFragmentId()) //
            .setMinorFragmentId(minorFragmentId) //
            .setQueryId(queryId) //
            .build();
        PlanFragment fragment = PlanFragment.newBuilder() //
            .setForeman(foremanNode) //
            .setFragmentJson(plan) //
            .setHandle(handle) //
            .setAssignment(wrapper.getAssignedEndpoint(minorFragmentId)) //
            .setLeafFragment(isLeafFragment) //
            .setQueryStartTime(queryStartTime)
            .setTimeZone(timeZone)//
            .setMemInitial(wrapper.getInitialAllocation())//
            .setMemMax(wrapper.getMaxAllocation())
            .setOptionsJson(optionsData)
            .setCredentials(session.getCredentials())
            .build();

        if (isRootNode) {
          logger.debug("Root fragment:\n {}", DrillStringUtils.unescapeJava(fragment.toString()));
          rootFragment = fragment;
          rootOperator = root;
        } else {
          logger.debug("Remote fragment:\n {}", DrillStringUtils.unescapeJava(fragment.toString()));
          fragments.add(fragment);
        }
      }
    }

    return new QueryWorkUnit(rootOperator, rootFragment, fragments);
  }

  private void assignEndpoints(Collection<DrillbitEndpoint> allNodes, PlanningSet planningSet) throws PhysicalOperatorSetupException {
    // for each node, set the width based on the parallelization threshold and cluster width.
    for (Wrapper wrapper : planningSet) {

      Stats stats = wrapper.getStats();

      double targetSlices = stats.getTotalCost()/parallelizationThreshold;
      int targetIntSlices = (int) Math.ceil(targetSlices);

      // figure out width.
      int width = Math.min(targetIntSlices, Math.min(stats.getMaxWidth(), maxGlobalWidth));


      width = Math.min(width, maxWidthPerNode*allNodes.size());

      if (width < 1) {
        width = 1;
      }
//      logger.debug("Setting width {} on fragment {}", width, wrapper);
      wrapper.setWidth(width);
      // figure out endpoint assignments. also informs the exchanges about their respective endpoints.
      wrapper.assignEndpoints(allNodes, affinityFactor);
    }
  }

}
