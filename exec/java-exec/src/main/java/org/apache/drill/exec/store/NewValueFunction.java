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
package org.apache.drill.exec.store;

import io.netty.buffer.DrillBuf;
import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.NullHandling;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.annotations.Workspace;
import org.apache.drill.exec.expr.holders.BitHolder;
import org.apache.drill.exec.expr.holders.IntHolder;
import org.apache.drill.exec.expr.holders.VarCharHolder;

import javax.inject.Inject;

public class NewValueFunction {

  @FunctionTemplate(name = "newValue",
      scope = FunctionTemplate.FunctionScope.SIMPLE,
      nulls = NullHandling.INTERNAL)
  public static class NewValueVarChar implements DrillSimpleFunc {

    @Param VarCharHolder in;
    @Workspace String previous;
    @Workspace Boolean initialized;
    @Output BitHolder out;

    public void setup() {
      initialized = false;
    }

    public void eval() {
      String value = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.getStringFromVarCharHolder(in);
      if (initialized) {
        if (value.equals(previous)) {
          out.value = 0;
        } else {
          previous = value;
          out.value = 1;
        }
      } else {
        previous = value;
        out.value = 1;
        initialized = true;
      }
    }
  }
}
