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
package org.apache.drill.exec.expr.fn;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Maps;
import com.sun.codemodel.JType;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.exec.expr.ClassGenerator;
import org.apache.drill.exec.expr.ClassGenerator.BlockType;
import org.apache.drill.exec.expr.ClassGenerator.HoldingContainer;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.FunctionCostCategory;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.FunctionScope;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.NullHandling;

import com.google.common.base.Preconditions;
import com.sun.codemodel.JBlock;
import com.sun.codemodel.JConditional;
import com.sun.codemodel.JExpr;
import com.sun.codemodel.JExpression;
import com.sun.codemodel.JMod;
import com.sun.codemodel.JVar;
import org.apache.drill.exec.expr.fn.interpreter.DrillSimpleFuncInterpreter;
import org.apache.drill.exec.expr.fn.interpreter.InterpreterGenerator;

public class DrillSimpleFuncHolder extends DrillFuncHolder{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillSimpleFuncHolder.class);

  private final String setupBody;
  private final String evalBody;
  private final String resetBody;
  private final String cleanupBody;
  private final String interpreterClassName;

  public DrillSimpleFuncHolder(FunctionScope scope, NullHandling nullHandling, boolean isBinaryCommutative, boolean isRandom,
      String[] registeredNames, ValueReference[] parameters, ValueReference returnValue, WorkspaceReference[] workspaceVars,
      Map<String, String> methods, List<String> imports) {
    this(scope, nullHandling, isBinaryCommutative, isRandom, registeredNames, parameters, returnValue, workspaceVars, methods, imports, FunctionCostCategory.getDefault(), null);
  }

  public DrillSimpleFuncHolder(FunctionScope scope, NullHandling nullHandling, boolean isBinaryCommutative, boolean isRandom,
      String[] registeredNames, ValueReference[] parameters, ValueReference returnValue, WorkspaceReference[] workspaceVars,
      Map<String, String> methods, List<String> imports, FunctionCostCategory costCategory, String interpreterClassName) {
    super(scope, nullHandling, isBinaryCommutative, isRandom, registeredNames, parameters, returnValue, workspaceVars, methods, imports, costCategory);
    setupBody = methods.get("setup");
    evalBody = methods.get("eval");
    resetBody = methods.get("reset");
    cleanupBody = methods.get("cleanup");
    Preconditions.checkNotNull(evalBody);

    this.interpreterClassName = interpreterClassName;
  }

  @Override
  public boolean isNested() {
    return false;
  }

  public DrillSimpleFuncInterpreter createInterpreter() throws Exception {
    Preconditions.checkArgument(this.interpreterClassName != null, "interpreterClassName should not be null!");

    String className = InterpreterGenerator.PACKAGE_NAME + "." + this.interpreterClassName;
    return (DrillSimpleFuncInterpreter) Class.forName(className).newInstance();
  }

  public HoldingContainer renderEnd(ClassGenerator<?> g, HoldingContainer[] inputVariables, JVar[]  workspaceJVars){
    //If the function's annotation specifies a parameter has to be constant expression, but the HoldingContainer
    //for the argument is not, then raise exception.
    for (int i =0; i < inputVariables.length; i++) {
      if (parameters[i].isConstant && !inputVariables[i].isConstant()) {
        throw new DrillRuntimeException(String.format("The argument '%s' of Function '%s' has to be constant!", parameters[i].name, this.getRegisteredNames()[0]));
      }
    }
    generateBody(g, BlockType.SETUP, setupBody, inputVariables, workspaceJVars, true);
    HoldingContainer c = generateEvalBody(g, inputVariables, evalBody, workspaceJVars);
    generateBody(g, BlockType.RESET, resetBody, null, workspaceJVars, false);
    generateBody(g, BlockType.CLEANUP, cleanupBody, null, workspaceJVars, false);
    return c;
  }

  protected HoldingContainer generateEvalBody(ClassGenerator<?> g, HoldingContainer[] inputVariables, String body, JVar[] workspaceJVars) {

    g.getEvalBlock().directStatement(String.format("//---- start of eval portion of %s function. ----//", registeredNames[0]));

    JBlock sub = new JBlock(true, true);
    JBlock topSub = sub;
    HoldingContainer out = null;
    MajorType returnValueType = returnValue.type;

    // add outside null handling if it is defined.
    if (nullHandling == NullHandling.NULL_IF_NULL) {
      JExpression e = null;
      for (HoldingContainer v : inputVariables) {
        if (v.isOptional()) {
          if (e == null) {
            e = v.getIsSet();
          } else {
            e = e.mul(v.getIsSet());
          }
        }
      }

      if (e != null) {
        // if at least one expression must be checked, set up the conditional.
        returnValueType = returnValue.type.toBuilder().setMode(DataMode.OPTIONAL).build();
        out = g.declare(returnValueType);
        e = e.eq(JExpr.lit(0));
        JConditional jc = sub._if(e);
        jc._then().assign(out.getIsSet(), JExpr.lit(0));
        sub = jc._else();
      }
    }

    if (out == null) {
      out = g.declare(returnValueType);
    }

    // add the subblock after the out declaration.
    g.getEvalBlock().add(topSub);


//    JVar internalOutput = sub.decl(JMod.FINAL, g.getHolderType(returnValueType), returnValue.name, JExpr._new(g.getHolderType(returnValueType)));
    Field[] fields;
    try {
      fields = TypeHelper.getHolderReaderImpl(returnValueType.getMinorType(), returnValueType.getMode()).getDeclaredField("holder").getType().getFields();
    } catch (NoSuchFieldException e) {
      throw new RuntimeException(e);
    }
    Map<String,JVar> outMap = Maps.newHashMap();
    for (Field field : fields) {
      JType type = JType.parse(g.getModel(), field.getType().getName());
      JVar var = sub.decl(type, returnValue.name + "_" + field.getName());
      outMap.put(field.getName(), var);
    }
    addProtectedBlock(g, sub, getNewBody(body), inputVariables, workspaceJVars, false);
    if (sub != topSub) {
      sub.assign(out.getHolder().get("isSet"),JExpr.lit(1));// Assign null if NULL_IF_NULL mode
    }
//    sub.assign(out.getHolder(), internalOutput);
    for (String fieldName : out.getHolder().keySet()) {
      JVar varOut = out.getHolder().get(fieldName);
//      JVar varInternalOut = internalOutput.getHolder().get(fieldName);
      sub.assign(varOut, outMap.get(fieldName));
    }
    if (sub != topSub) {
      sub.assign(out.getHolder().get("isSet"),JExpr.lit(1));// Assign null if NULL_IF_NULL mode
    }

    g.getEvalBlock().directStatement(String.format("//---- end of eval portion of %s function. ----//", registeredNames[0]));

    return out;
  }

  private String getNewBody(String body) {
    String newBody = body;
    for (ValueReference parameter : parameters) {
      String pName = parameter.getName();
      Class holderClass = null;
      try {
        holderClass = TypeHelper.getHolderReaderImpl(parameter.getType().getMinorType(), parameter.getType().getMode()).getDeclaredField("holder").getType();
      } catch (NoSuchFieldException e) {
        throw new RuntimeException(e);
      }
      for (Field field : holderClass.getFields()) {
        String fieldName = field.getName();
        newBody = newBody.replace(String.format("%s.%s", pName, fieldName), String.format("%s_%s", pName, fieldName));
      }
    }
    try {
      String pName = returnValue.getName();
      Class holderClass = TypeHelper.getHolderReaderImpl(returnValue.getType().getMinorType(), returnValue.getType().getMode()).getDeclaredField("holder").getType();
      for (Field field : holderClass.getFields()) {
        String fieldName = field.getName();
        newBody = newBody.replace(String.format("%s.%s", pName, fieldName), String.format("%s_%s", pName, fieldName));
      }
    } catch (NoSuchFieldException e) {
      throw new RuntimeException(e);
    }
    return newBody;
  }

  public String getSetupBody() {
    return setupBody;
  }

  public String getEvalBody() {
    return evalBody;
  }
}
