/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.physical.impl;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.LocalSingleSender;
import org.apache.drill.exec.physical.config.SingleSender;
import org.apache.drill.exec.physical.impl.SingleSenderCreator.SingleSenderRootExec;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.record.RecordBatch;

import java.util.List;

@SuppressWarnings("unused")
public class LocalSingleSenderCreator implements RootCreator<LocalSingleSender> {

  @Override
  public RootExec getRoot(FragmentContext context, LocalSingleSender config, List<RecordBatch> children)
      throws ExecutionSetupException {
    assert children != null && children.size() == 1;
    DrillbitEndpoint thisDrillbit = context.getDrillbitContext().getEndpoint();

    if (config.getDestinations().contains(thisDrillbit)) {
      throw new ExecutionSetupException("Current Drillbit has no local partitioner.");
    }

    SingleSender singleSenderConfig = new SingleSender(config.getOppositeMajorFragmentId(),
        config.getChild(), thisDrillbit);

    return new SingleSenderRootExec(context, children.iterator().next(), singleSenderConfig,
        config.getOppositeMinorFragmentId());
  }
}
