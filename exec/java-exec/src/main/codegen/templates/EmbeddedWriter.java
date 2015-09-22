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
<@pp.changeOutputFile name="/org/apache/drill/exec/vector/complex/impl/EmbeddedWriter.java" />


<#include "/@includes/license.ftl" />

package org.apache.drill.exec.vector.complex.impl;

<#include "/@includes/vv_imports.ftl" />

/*
 * This class is generated using freemarker and the ${.template_name} template.
 */
@SuppressWarnings("unused")
public class EmbeddedWriter extends AbstractFieldWriter implements FieldWriter {

  private EmbeddedVector data;
  private MapWriter mapWriter;
  private EmbeddedListWriter listWriter;
  private List<BaseWriter> writers = Lists.newArrayList();

  public EmbeddedWriter(BufferAllocator allocator) {
    super(null);
    data = new EmbeddedVector(MaterializedField.create("root", Types.required(MinorType.EMBEDDED)), allocator);
  }

  public EmbeddedWriter(EmbeddedVector vector) {
    super(null);
    data = vector;
  }

  @Override
  public void setPosition(int index) {
    super.setPosition(index);
    for (BaseWriter writer : writers) {
      writer.setPosition(index);
    }
  }


  @Override
  public void start() {
    data.getMutator().setType(idx(), MinorType.MAP);
    getMapWriter(true).start();
  }

  @Override
  public void end() {
    getMapWriter(false).end();
  }

  @Override
  public void startList() {
    getListWriter(true).startList();
    data.getMutator().setType(idx(), MinorType.LIST);
  }

  @Override
  public void endList() {
    getListWriter(true).endList();
  }

  public void writeString(String s) {
    fail("string");
  }

  private MapWriter getMapWriter(boolean create) {
    if (create && mapWriter == null) {
      mapWriter = new SingleMapWriter(data.getMap(), null);
      mapWriter.setPosition(idx());
      writers.add(mapWriter);
    }
    return mapWriter;
  }

  public MapWriter asMap() {
    data.getMutator().setType(idx(), MinorType.MAP);
    return getMapWriter(true);
  }

  private ListWriter getListWriter(boolean create) {
    if (create && listWriter == null) {
      listWriter = new EmbeddedListWriter(data.getList());
      listWriter.setPosition(idx());
      writers.add(listWriter);
    }
    return listWriter;
  }

  public ListWriter asList() {
    data.getMutator().setType(idx(), MinorType.LIST);
    return getListWriter(true);
  }

  <#list vv.types as type><#list type.minor as minor><#assign name = minor.class?cap_first />
  <#assign fields = minor.fields!type.fields />
  <#assign uncappedName = name?uncap_first/>

          <#if !minor.class?starts_with("Decimal")>

  private ${name}Writer ${name?uncap_first}Writer;

  private ${name}Writer get${name}Writer(boolean create) {
    if (create && ${uncappedName}Writer == null) {
      ${uncappedName}Writer = new Nullable${name}WriterImpl(data.get${name}Vector(), null);
      writers.add(${uncappedName}Writer);
    }
    return ${uncappedName}Writer;
  }

  public ${name}Writer as${name}() {
    data.getMutator().setType(idx(), MinorType.${name?upper_case});
    return get${name}Writer(true);
  }

  @Override
  public void write(${name}Holder holder) {
    data.getMutator().setType(idx(), MinorType.${name?upper_case});
    get${name}Writer(true).setPosition(idx());
    get${name}Writer(true).write${name}(<#list fields as field>holder.${field.name}<#if field_has_next>, </#if></#list>);
  }

  public void write${minor.class}(<#list fields as field>${field.type} ${field.name}<#if field_has_next>, </#if></#list>) {
    data.getMutator().setType(idx(), MinorType.${name?upper_case});
    get${name}Writer(true).setPosition(idx());
    get${name}Writer(true).write${name}(<#list fields as field>${field.name}<#if field_has_next>, </#if></#list>);
  }
  </#if>

  </#list></#list>

  public void writeNull() {
  }

  @Override
  public MapWriter map() {
    data.getMutator().setType(idx(), MinorType.LIST);
    getListWriter(true).setPosition(idx());
    return getListWriter(true).map();
  }

  @Override
  public ListWriter list() {
    data.getMutator().setType(idx(), MinorType.LIST);
    getListWriter(true).setPosition(idx());
    return getListWriter(true).list();
  }

  @Override
  public ListWriter list(String name) {
    data.getMutator().setType(idx(), MinorType.MAP);
    getMapWriter(true).setPosition(idx());
    return getMapWriter(true).list(name);
  }

  @Override
  public MapWriter map(String name) {
    data.getMutator().setType(idx(), MinorType.MAP);
    getMapWriter(true).setPosition(idx());
    return getMapWriter(true).map(name);
  }

  <#list vv.types as type><#list type.minor as minor>
  <#assign lowerName = minor.class?uncap_first />
  <#if lowerName == "int" ><#assign lowerName = "integer" /></#if>
  <#assign upperName = minor.class?upper_case />
  <#assign capName = minor.class?cap_first />
  <#if minor.class?starts_with("Decimal") >
  public ${capName}Writer ${lowerName}(String name, int scale, int precision) {
    fail("${capName}");
    return null;
  }
  </#if>

  @Override
  public ${capName}Writer ${lowerName}(String name) {
    AbstractBaseWriter.check(name);
    data.getMutator().setType(idx(), MinorType.MAP);
    getMapWriter(true).setPosition(idx());
    return getMapWriter(true).${lowerName}(name);
  }

  @Override
  public ${capName}Writer ${lowerName}() {
    data.getMutator().setType(idx(), MinorType.LIST);
    getListWriter(true).setPosition(idx());
    return getListWriter(true).${lowerName}();
  }

  </#list></#list>

  public void copyReader(FieldReader reader) {
    fail("Copy FieldReader");
  }

  public void copyReaderToField(String name, FieldReader reader) {
    fail("Copy FieldReader to STring");
  }

  private void fail(String name) {
    throw new IllegalArgumentException(String.format("You tried to write a %s type when you are using a ValueWriter of type %s.", name, this.getClass().getSimpleName()));
  }

  @Override
  public void allocate() {
    data.allocateNew();
  }

  @Override
  public void clear() {
    data.clear();
  }

  @Override
  public void close() throws Exception {
    data.close();
  }

  @Override
  public MaterializedField getField() {
    return data.getField();
  }

  @Override
  public int getValueCapacity() {
    return data.getValueCapacity();
  }
}
