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
package org.apache.drill.exec.planner.sql.handlers;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.google.common.collect.Lists;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;

import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.exec.physical.PhysicalPlan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.planner.logical.CreateTableEntry;
import org.apache.drill.exec.planner.logical.DrillRel;
import org.apache.drill.exec.planner.logical.DrillScreenRel;
import org.apache.drill.exec.planner.logical.DrillStoreRel;
import org.apache.drill.exec.planner.logical.DrillWriterRel;
import org.apache.drill.exec.planner.physical.Prel;
import org.apache.drill.exec.planner.physical.ProjectPrel;
import org.apache.drill.exec.planner.physical.WriterPrel;
import org.apache.drill.exec.planner.physical.visitor.BasePrelVisitor;
import org.apache.drill.exec.planner.sql.DrillSqlOperator;
import org.apache.drill.exec.planner.sql.DrillSqlWorker;
import org.apache.drill.exec.planner.sql.SchemaUtilites;
import org.apache.drill.exec.planner.sql.parser.SqlCreateTable;
import org.apache.drill.exec.store.AbstractSchema;
import org.apache.drill.exec.util.Pointer;
import org.apache.drill.exec.work.foreman.ForemanSetupException;
import org.apache.drill.exec.work.foreman.SqlUnsupportedException;

public class CreateTableHandler extends DefaultSqlHandler {
  public CreateTableHandler(SqlHandlerConfig config, Pointer<String> textPlan) {
    super(config, textPlan);
  }

  @Override
  public PhysicalPlan getPlan(SqlNode sqlNode) throws ValidationException, RelConversionException, IOException, ForemanSetupException {
    SqlCreateTable sqlCreateTable = unwrap(sqlNode, SqlCreateTable.class);

    final String newTblName = sqlCreateTable.getName();
    final RelNode newTblRelNode =
        SqlHandlerUtil.resolveNewTableRel(false, planner, sqlCreateTable.getFieldNames(), sqlCreateTable.getQuery());


    final AbstractSchema drillSchema =
        SchemaUtilites.resolveToMutableDrillSchema(context.getNewDefaultSchema(), sqlCreateTable.getSchemaPath());
    final String schemaPath = drillSchema.getFullSchemaName();

    if (SqlHandlerUtil.getTableFromSchema(drillSchema, newTblName) != null) {
      throw UserException.validationError()
          .message("A table or view with given name [%s] already exists in schema [%s]", newTblName, schemaPath)
          .build();
    }

    final RelNode newTblRelNodeWithPCol = SqlHandlerUtil.qualifyPartitionCol(newTblRelNode, sqlCreateTable.getPartitionColumns());

    log("Optiq Logical", newTblRelNodeWithPCol);

    // Convert the query to Drill Logical plan and insert a writer operator on top.
    DrillRel drel = convertToDrel(newTblRelNodeWithPCol, drillSchema, newTblName, sqlCreateTable.getPartitionColumns());
    log("Drill Logical", drel);
    Prel prel = convertToPrel(drel, newTblRelNode.getRowType());
    log("Drill Physical", prel);
    PhysicalOperator pop = convertToPop(prel);
    PhysicalPlan plan = convertToPlan(pop);
    log("Drill Plan", plan);

    return plan;
  }

  private DrillRel convertToDrel(RelNode relNode, AbstractSchema schema, String tableName, List<String> partitionColumns)
      throws RelConversionException {
    RelNode convertedRelNode = planner.transform(DrillSqlWorker.LOGICAL_RULES,
        relNode.getTraitSet().plus(DrillRel.DRILL_LOGICAL), relNode);

    if (convertedRelNode instanceof DrillStoreRel) {
      throw new UnsupportedOperationException();
    }

    DrillWriterRel writerRel = new DrillWriterRel(convertedRelNode.getCluster(), convertedRelNode.getTraitSet(),
        convertedRelNode, schema.createNewTable(tableName, partitionColumns));
    return new DrillScreenRel(writerRel.getCluster(), writerRel.getTraitSet(), writerRel);
  }

  private Prel convertToPrel(RelNode drel, RelDataType inputRowType) throws RelConversionException, SqlUnsupportedException {
    Prel prel = convertToPrel(drel);

    prel = prel.accept(new ProjectForWriterVisitor(inputRowType), null);

    return prel;
  }

  /**
   * A PrelVisitor which will insert a project under Writer.
   *
   * For CTAS : create table t1 partition by (con_A) select * from T1;
   *   A Project with Item expr will be inserted, in addition to *.  We need insert another Project to remove
   *   this additional expression.
   *
   * In addition, to make execution's implementation easier, we'll add a special field into project under
   * writer, so that execution will use that field to check uniqueness.
   */
  private class ProjectForWriterVisitor extends BasePrelVisitor<Prel, Void, RuntimeException> {

    private final RelDataType queryRowType;

    ProjectForWriterVisitor(RelDataType queryRowType) {
      this.queryRowType = queryRowType;
    }

    @Override
    public Prel visitPrel(Prel prel, Void value) throws RuntimeException {
      List<RelNode> children = Lists.newArrayList();
      for(Prel child : prel){
        child = child.accept(this, null);
        children.add(child);
      }

      return (Prel) prel.copy(prel.getTraitSet(), children);

    }

    @Override
    public Prel visitWriter(WriterPrel prel, Void value) throws RuntimeException {

      final Prel child = ((Prel)prel.getInput()).accept(this, null);

      final RelDataType childRowType = child.getRowType();

      final RelOptCluster cluster = prel.getCluster();

      final List<String> partitionColumns = prel.getCreateTableEntry().getPartitionColumns();

      final List<RexNode> exprs =
          new AbstractList<RexNode>() {
            public int size() {
              return queryRowType.getFieldCount() + 1 ;
            }

            public RexNode get(int index) {
              if (index < queryRowType.getFieldCount()) {
                return RexInputRef.of(index, queryRowType);
              } else {
                return buildNewPartitionExpression(cluster.getRexBuilder(), partitionColumns);
              }
            }
          };


      final List<String> fieldnames = new ArrayList<String>(queryRowType.getFieldNames());
      fieldnames.add(WriterPrel.PARTITION_COLUMN_IDENTIFIER);

      final RelDataType rowTypeWithPCI = RexUtil.createStructType(cluster.getTypeFactory(), exprs, fieldnames);

      ProjectPrel projectUnderWriter = new ProjectPrel(cluster,
          cluster.getPlanner().emptyTraitSet().plus(Prel.DRILL_PHYSICAL), child, exprs, rowTypeWithPCI);

      return (Prel) prel.copy(projectUnderWriter.getTraitSet(),
          Collections.singletonList( (RelNode) projectUnderWriter));
    }

    private RexNode buildNewPartitionExpression(RexBuilder rexBuilder, List<String> partitionColumns) {
      List<RexInputRef> inputRefs = Lists.newLinkedList();
      int index = 0;
      for (String field : queryRowType.getFieldNames()) {
        if (partitionColumns.contains(field)) {
          inputRefs.add(RexInputRef.of(index, queryRowType));
        }
        index++;
      }
      final DrillSqlOperator newValueFunc
          = new DrillSqlOperator("newValue", 1, MajorType.getDefaultInstance(), true);
      RexNode node = rexBuilder.makeCall(newValueFunc, inputRefs.get(0));
      inputRefs.remove(0);

      final DrillSqlOperator booleanOrFunc
          = new DrillSqlOperator("booleanOr", 2, MajorType.getDefaultInstance(), true);
      while (!inputRefs.isEmpty()) {
        node = rexBuilder.makeCall(booleanOrFunc, node, rexBuilder.makeCall(newValueFunc, inputRefs.get(0)));
        inputRefs.remove(0);
      }
      return node;
    }

  }

}