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
import org.apache.drill.exec.expr.holders.VarBinaryHolder;
import org.apache.drill.exec.expr.holders.VarCharHolder;

import javax.inject.Inject;

public class NewValueFunction {

  @FunctionTemplate(name = "newPartitionValue",
      scope = FunctionTemplate.FunctionScope.SIMPLE,
      nulls = NullHandling.INTERNAL)
  public static class NewValueVarChar implements DrillSimpleFunc {

    @Param VarCharHolder in;
    @Workspace DrillBuf previous;
    @Workspace Integer previousLength;
    @Workspace Boolean initialized;
    @Output BitHolder out;
    @Inject DrillBuf buf;

    public void setup() {
      initialized = false;
      previous = buf;
    }

    public void eval() {
      int length = in.end - in.start;

      if (initialized) {
        if (org.apache.drill.exec.expr.fn.impl.ByteFunctionHelpers.compare(previous, 0, previousLength, in.buffer, in.start, in.end) == 0) {
          out.value = 0;
        } else {
          previous = buf.reallocIfNeeded(length);
          previous.setBytes(0, in.buffer, in.start, in.end - in.start);
          previousLength = in.end - in.start;
          out.value = 1;
        }
      } else {
        previous = buf.reallocIfNeeded(length);
        previous.setBytes(0, in.buffer, in.start, in.end - in.start);
        previousLength = in.end - in.start;
        out.value = 1;
        initialized = true;
      }
    }
  }

  @FunctionTemplate(name = "newPartitionValue",
      scope = FunctionTemplate.FunctionScope.SIMPLE,
      nulls = NullHandling.INTERNAL)
  public static class NewValueVarBinary implements DrillSimpleFunc {

    @Param VarBinaryHolder in;
    @Workspace DrillBuf previous;
    @Workspace Integer previousLength;
    @Workspace Boolean initialized;
    @Output BitHolder out;
    @Inject DrillBuf buf;

    public void setup() {
      initialized = false;
      previous = buf;
    }

    public void eval() {
      int length = in.end - in.start;

      if (initialized) {
        if (org.apache.drill.exec.expr.fn.impl.ByteFunctionHelpers.compare(previous, 0, previousLength, in.buffer, in.start, in.end) == 0) {
          out.value = 0;
        } else {
          previous = buf.reallocIfNeeded(length);
          previous.setBytes(0, in.buffer, in.start, in.end - in.start);
          previousLength = in.end - in.start;
          out.value = 1;
        }
      } else {
        previous = buf.reallocIfNeeded(length);
        previous.setBytes(0, in.buffer, in.start, in.end - in.start);
        previousLength = in.end - in.start;
        out.value = 1;
        initialized = true;
      }
    }
  }
}
