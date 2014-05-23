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

package org.apache.drill.exec.store.parquet2;

import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.planner.logical.DrillOptiq;
import org.apache.drill.exec.planner.logical.DrillParseContext;
import org.apache.drill.exec.planner.logical.RelOptHelper;
import org.apache.drill.exec.planner.physical.FilterPrel;
import org.apache.drill.exec.planner.physical.ScanPrel;
import org.apache.drill.exec.store.StoragePluginOptimizerRule;
import org.apache.drill.exec.store.parquet.ParquetGroupScan;
import org.eigenbase.relopt.RelOptRuleCall;
import org.eigenbase.rex.RexNode;

import java.util.Collections;
import java.util.List;

public class ParquetPushFilterIntoScan extends StoragePluginOptimizerRule {
  public static final StoragePluginOptimizerRule INSTANCE = new ParquetPushFilterIntoScan();

  private ParquetPushFilterIntoScan() {
    super(RelOptHelper.some(FilterPrel.class, RelOptHelper.any(ScanPrel.class)), "ParquetPushFilterIntoScan");
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final ScanPrel scan = call.rel(1);
    final FilterPrel filter = call.rel(0);
    final RexNode condition = filter.getCondition();

    ParquetGroupScan groupScan = (ParquetGroupScan)scan.getGroupScan();

    LogicalExpression conditionExp = DrillOptiq.toDrill(new DrillParseContext(), scan, condition);

    final ParquetGroupScan newGroupScan = groupScan.clone(conditionExp);
    groupScan.setFilter(conditionExp);

    final ScanPrel newScanPrel = ScanPrel.create(scan, filter.getTraitSet(), newGroupScan, scan.getRowType());
    call.transformTo(newScanPrel);
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    final ScanPrel scan = (ScanPrel) call.rel(1);
    if (scan.getGroupScan() instanceof ParquetGroupScan) {
      if (((ParquetGroupScan) scan.getGroupScan()).isSupportFilterPushdown()) {
        return super.matches(call);
      }
    }
    return false;
  }

}
