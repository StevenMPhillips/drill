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
package org.apache.drill.exec.planner.physical.visitor;

import com.google.common.collect.Lists;
import org.apache.drill.exec.planner.physical.ExchangePrel;
import org.apache.drill.exec.planner.physical.HashToRandomExchangePrel;
import org.apache.drill.exec.planner.physical.LocalExchangePrel;
import org.apache.drill.exec.planner.physical.Prel;
import org.apache.drill.exec.planner.physical.SelectionVectorRemoverPrel;
import org.eigenbase.rel.RelNode;

import java.util.Collections;
import java.util.List;

public class InsertLocalExchangeVisitor extends BasePrelVisitor<Prel, Void, RuntimeException> {
  private final int multiplexingFactor;

  public InsertLocalExchangeVisitor(int multiplexingFactor) {
    this.multiplexingFactor = multiplexingFactor;
  }

  public static Prel insertLocalExchange(Prel prel, int multiplexingFactor) {
    return prel.accept(new InsertLocalExchangeVisitor(multiplexingFactor), null);
  }

  @Override
  public Prel visitExchange(ExchangePrel prel, Void value) throws RuntimeException {
    Prel child = ((Prel)prel.getChild()).accept(this, null);
    // Whenever we encounter a HashToRandomExchangePrel, insert a LocalExchangePrel in before HashToRandomExchangePrel
    if (prel instanceof HashToRandomExchangePrel) {
      Prel svRemovePrel = new SelectionVectorRemoverPrel(child);
      Prel localExPrel = new LocalExchangePrel(prel.getCluster(), prel.getTraitSet(), svRemovePrel, multiplexingFactor);
      HashToRandomExchangePrel hashExPrel = (HashToRandomExchangePrel)
          prel.copy(prel.getTraitSet(), Collections.singletonList((RelNode)localExPrel));
      return hashExPrel;
    }
    return (Prel)prel.copy(prel.getTraitSet(), Collections.singletonList(((RelNode)child)));
  }

  @Override
  public Prel visitPrel(Prel prel, Void value) throws RuntimeException {
    List<RelNode> children = Lists.newArrayList();
    for(Prel child : prel){
      children.add(child.accept(this, null));
    }
    return (Prel) prel.copy(prel.getTraitSet(), children);
  }
}
