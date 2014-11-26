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

package org.apache.drill.exec.physical.config;

import java.util.List;
import java.util.Map;

import com.google.common.collect.Maps;
import org.apache.drill.common.logical.data.Order.Ordering;
import org.apache.drill.exec.physical.PhysicalOperatorSetupException;
import org.apache.drill.exec.physical.base.AbstractExchange;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.Receiver;
import org.apache.drill.exec.physical.base.Sender;
import org.apache.drill.exec.proto.CoordinationProtos;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;

@JsonTypeName("single-merge-exchange")
public class SingleMergeExchange extends AbstractExchange {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SingleMergeExchange.class);

  private final List<Ordering> orderExpr;

  // ephemeral for setup tasks
  private DrillbitEndpoint receiverLocation;

  private Map<Integer, DrillbitEndpoint> senderFragmentsAndLocations;

  @JsonCreator
  public SingleMergeExchange(@JsonProperty("child") PhysicalOperator child,
                             @JsonProperty("orderings") List<Ordering> orderExpr) {
    super(child);
    this.orderExpr = orderExpr;
  }

  @Override
  public int getMaxSendWidth() {
    return Integer.MAX_VALUE;
  }

  @Override
  protected void setupSenders(List<CoordinationProtos.DrillbitEndpoint> senderLocations) {
    this.senderFragmentsAndLocations = Maps.newHashMap();
    for(int i=0; i<senderLocations.size(); i++) {
      senderFragmentsAndLocations.put(i, senderLocations.get(i));
    }
  }

  @Override
  protected void setupReceivers(List<CoordinationProtos.DrillbitEndpoint> receiverLocations)
      throws PhysicalOperatorSetupException {

    if (receiverLocations.size() != 1) {
      throw new PhysicalOperatorSetupException("SingleMergeExchange only supports a single receiver endpoint");
    }
    receiverLocation = receiverLocations.iterator().next();

  }

  @Override
  public Sender getSender(int minorFragmentId, PhysicalOperator child) {
    return new SingleSender(receiverMajorFragmentId, child, receiverLocation);
  }

  @Override
  public Receiver getReceiver(int minorFragmentId) {
    return new MergingReceiverPOP(senderMajorFragmentId, senderFragmentsAndLocations, orderExpr);
  }

  @Override
  protected PhysicalOperator getNewWithChild(PhysicalOperator child) {
    return new SingleMergeExchange(child, orderExpr);
  }

  @JsonProperty("orderings")
  public List<Ordering> getOrderings() {
    return this.orderExpr;
  }

}
