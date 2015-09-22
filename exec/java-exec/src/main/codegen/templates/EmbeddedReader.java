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
<@pp.changeOutputFile name="/org/apache/drill/exec/vector/complex/impl/EmbeddedReader.java" />


<#include "/@includes/license.ftl" />

package org.apache.drill.exec.vector.complex.impl;

<#include "/@includes/vv_imports.ftl" />

@SuppressWarnings("unused")
public class EmbeddedReader extends AbstractFieldReader {

  private List<BaseReader> readers = Lists.newArrayList();
  public EmbeddedVector data;
  
  public EmbeddedReader(EmbeddedVector data) {
    this.data = data;
  }

  public boolean isSet(){
    return !data.getAccessor().isNull(idx());
  }

  public void read(EmbeddedHolder holder) {
    holder.reader = this;
  }

  private SingleMapReaderImpl mapReader;

  private MapReader getMap() {
    if (mapReader == null) {
      this.mapReader = (SingleMapReaderImpl) data.getMap().getReader();
      readers.add(mapReader);
    }
    return mapReader;
  }

  <#list vv.types as type><#list type.minor as minor><#assign name = minor.class?cap_first />
          <#assign uncappedName = name?uncap_first/>
  <#assign boxedType = (minor.boxedType!type.boxedType) />
  <#assign javaType = (minor.javaType!type.javaType) />
  <#assign friendlyType = (minor.friendlyType!minor.boxedType!type.boxedType) />
  <#assign safeType=friendlyType />
  <#if safeType=="byte[]"><#assign safeType="ByteArray" /></#if>
  <#if !minor.class?starts_with("Decimal")>

  private Nullable${name}ReaderImpl ${uncappedName}Reader;

  private Nullable${name}ReaderImpl get${name}() {
    if (${uncappedName}Reader == null) {
      ${uncappedName}Reader = new Nullable${name}ReaderImpl(data.get${name}Vector());
      readers.add(${uncappedName}Reader);
    }
    return ${uncappedName}Reader;
  }

  public void read(Nullable${name}Holder holder){
    get${name}().read(holder);
  }

  /*
  public void read(int arrayIndex, ${name}Holder holder){
    fail("Repeated${name}");
  }
  
  public void read(int arrayIndex, Nullable${name}Holder holder){
    fail("Repeated${name}");
  }
  */
  
  public void copyAsValue(${name}Writer writer){
    get${name}().copyAsValue(writer);
  }
  /*
  public void copyAsField(String name, ${name}Writer writer){
    fail("${name}")
  }

  public ${friendlyType} read${safeType}(int arrayIndex){
    fail("read${safeType}(int arrayIndex)");
    return null;
  }

  public ${friendlyType} read${safeType}(){
    get${name}().setPosition(idx());
    return get${name}().read${safeType}();
  }
  */
  </#if>
  </#list></#list>

  @Override
  public void setPosition(int index) {
    super.setPosition(index);
    for (BaseReader reader : readers) {
      reader.setPosition(index);
    }
  }
  
  public FieldReader reader(String name){
    mapReader.setPosition(idx());
    return mapReader.reader(name);
  }
}



