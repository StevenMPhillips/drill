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

<@pp.dropOutputFile />
<@pp.changeOutputFile name="/org/apache/drill/exec/vector/complex/impl/EmbeddedFunctions.java" />


<#include "/@includes/license.ftl" />

package org.apache.drill.exec.vector.complex.impl;

<#include "/@includes/vv_imports.ftl" />
import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.NullHandling;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.holders.*;
import javax.inject.Inject;
import io.netty.buffer.DrillBuf;
import org.apache.drill.exec.record.RecordBatch;

/*
 * This class is generated using freemarker and the ${.template_name} template.
 */

@SuppressWarnings("unused")
public class EmbeddedFunctions {

  <#list vv.types as type><#list type.minor as minor><#assign name = minor.class?cap_first />
  <#assign fields = minor.fields!type.fields />
  <#assign uncappedName = name?uncap_first/>

  <#if !minor.class?starts_with("Decimal")>

  @SuppressWarnings("unused")
  @FunctionTemplate(name = "as${name}", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls=NullHandling.NULL_IF_NULL)
  public static class CastEmbedded${name} implements DrillSimpleFunc {

    @Param EmbeddedHolder in;
    @Output ${name}Holder out;

    public void setup() {}

    public void eval() {
      in.reader.read(out);
    }
  }

  @SuppressWarnings("unused")
  @FunctionTemplate(names = {"castEMBEDDED", "castToEmbedded"}, scope = FunctionTemplate.FunctionScope.SIMPLE, nulls=NullHandling.NULL_IF_NULL)
  public static class Cast${name}ToEmbedded implements DrillSimpleFunc {

    @Param Nullable${name}Holder in;
    @Output EmbeddedHolder out;

    public void setup() {}

    public void eval() {
      out.reader = new org.apache.drill.exec.vector.complex.impl.Nullable${name}HolderReaderImpl(in);
      out.isSet = 1;
    }
  }

  </#if>

  </#list></#list>

}
