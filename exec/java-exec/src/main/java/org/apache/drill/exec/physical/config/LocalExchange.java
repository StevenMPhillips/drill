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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.drill.exec.physical.PhysicalOperatorSetupException;
import org.apache.drill.exec.physical.base.AbstractExchange;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.Receiver;
import org.apache.drill.exec.physical.base.Sender;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName("local-exchange")
public class LocalExchange extends AbstractExchange{

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(UnionExchange.class);

  private final int multiplexingFactor;

  private List<DrillbitEndpoint> senderLocations;
  private List<DrillbitEndpoint> receiverLocations;

  private boolean mappingCreated = false;
  private List<Integer> senderToReceiverMapping = Lists.newArrayList();
  private Map<Integer, List<Integer>> mapRecv2Senders = Maps.newHashMap();


  public LocalExchange(@JsonProperty("child") PhysicalOperator child,
                       @JsonProperty("multiplexingFactor") int multiplexingFactor) {
    super(child);
    this.multiplexingFactor = multiplexingFactor;
  }

  @JsonProperty("multiplexingFactor")
  public int getMultiplexingFactor() {
    return multiplexingFactor;
  }

  @Override
  public void setupSenders(List<DrillbitEndpoint> senderLocations) {
    this.senderLocations = senderLocations;
  }

  @Override
  protected void setupReceivers(List<DrillbitEndpoint> receiverLocations) throws PhysicalOperatorSetupException {
    this.receiverLocations = receiverLocations;
  }

  @Override
  public Sender getSender(int minorFragmentId, PhysicalOperator child) {
    if (!mappingCreated) {
      createSenderReceiverMappings();
    }

    return new LocalSingleSender(receiverMajorFragmentId, senderToReceiverMapping.get(minorFragmentId), child,
        receiverLocations.get(senderToReceiverMapping.get(minorFragmentId)));
  }

  @Override
  public Receiver getReceiver(int minorFragmentId) {
    if (!mappingCreated) {
      createSenderReceiverMappings();
    }

    List<Integer> sendingMinorFragmentIds = mapRecv2Senders.get(minorFragmentId);
    Map<Integer, DrillbitEndpoint> senders = Maps.newHashMap();
    for(Integer fragmentId : sendingMinorFragmentIds) {
      senders.put(fragmentId, senderLocations.get(fragmentId));
    }

    return new UnorderedReceiver(this.senderMajorFragmentId, senders);
  }

  @Override
  public int getMaxSendWidth() {
    return Integer.MAX_VALUE;
  }

  @Override
  protected PhysicalOperator getNewWithChild(PhysicalOperator child) {
    return new UnionExchange(child);
  }


  private void createSenderReceiverMappings() {
    // Create a mapping of Drillbit to the list of receiving fragment minorFragmentIds
    Map<DrillbitEndpoint, List<Integer>> drillbit2RecvMinorFragIdList = Maps.newHashMap();
    for(int i=0; i<receiverLocations.size(); i++) {
      DrillbitEndpoint ep = receiverLocations.get(i);

      if (drillbit2RecvMinorFragIdList.containsKey(ep)) {
        drillbit2RecvMinorFragIdList.get(ep).add(i);
      } else {
        List<Integer> minorFragIdList = Lists.newArrayList();
        minorFragIdList.add(i);
        drillbit2RecvMinorFragIdList.put(ep, minorFragIdList);
      }
    }

    // Maintain the occurrences of sender nodes seen so far
    Map<DrillbitEndpoint, Integer> sendersSeenPerNode = Maps.newHashMap();
    for(int senderMinorFragIdCounter=0; senderMinorFragIdCounter<senderLocations.size(); senderMinorFragIdCounter++) {
      DrillbitEndpoint sendingEp = senderLocations.get(senderMinorFragIdCounter);
      Integer count = 1;
      if (sendersSeenPerNode.containsKey(sendingEp)) {
        count = sendersSeenPerNode.remove(sendingEp) + 1;
      }
      sendersSeenPerNode.put(sendingEp, count);

      // pick destination from drillbit2RecvMinorFragIdList
      Integer recvMinorFragId = drillbit2RecvMinorFragIdList.get(sendingEp).get((count-1)/multiplexingFactor);

      senderToReceiverMapping.add(recvMinorFragId);

      if (mapRecv2Senders.containsKey(recvMinorFragId)) {
        mapRecv2Senders.get(recvMinorFragId).add(senderMinorFragIdCounter);
      } else {
        List<Integer> minorFragIdList = Lists.newArrayList();
        minorFragIdList.add(senderMinorFragIdCounter);
        mapRecv2Senders.put(recvMinorFragId, minorFragIdList);
      }
    }

    mappingCreated = true;
  }
}
