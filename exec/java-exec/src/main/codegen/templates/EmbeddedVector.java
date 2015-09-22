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
<@pp.changeOutputFile name="/org/apache/drill/exec/vector/complex/impl/EmbeddedVector.java" />


<#include "/@includes/license.ftl" />

package org.apache.drill.exec.vector.complex.impl;

<#include "/@includes/vv_imports.ftl" />
import java.util.Iterator;

/*
 * This class is generated using freemarker and the ${.template_name} template.
 */
@SuppressWarnings("unused")


public class EmbeddedVector implements ValueVector {

  private MaterializedField field;
  private BufferAllocator allocator;
  private Accessor accessor = new Accessor();
  private Mutator mutator = new Mutator();
  private int valueCount;

  private MapVector internalMap;
  private SingleMapWriter internalMapWriter;
  private UInt1Vector typeVector;

  private MapVector mapVector;
  private ListVector listVector;
  private NullableBigIntVector bigInt;
  private NullableVarCharVector varChar;

  private FieldReader reader;
  private NullableBitVector bit;

  public EmbeddedVector(MaterializedField field, BufferAllocator allocator) {
    this.field = field;
    this.allocator = allocator;
    internalMap = new MapVector("internal", allocator, null);
    internalMapWriter = new SingleMapWriter(internalMap, null);
    internalMapWriter.embeddedVector = true;
    this.typeVector = internalMap.addOrGet("types", Types.required(MinorType.UINT1), UInt1Vector.class);
    this.field.addChild(internalMap.getField());
  }

  public MapVector getMap() {
    if (mapVector == null) {
      int vectorCount = internalMap.size();
      mapVector = internalMap.addOrGet("map", Types.optional(MinorType.MAP), MapVector.class);
      if (internalMap.size() > vectorCount) {
        mapVector.allocateNew();
      }
    }
    return mapVector;
  }

  <#list vv.types as type><#list type.minor as minor><#assign name = minor.class?cap_first />
  <#assign fields = minor.fields!type.fields />
  <#assign uncappedName = name?uncap_first/>
  <#if !minor.class?starts_with("Decimal")>

  private Nullable${name}Vector ${uncappedName}Vector;

  public Nullable${name}Vector get${name}Vector() {
    if (${uncappedName}Vector == null) {
      int vectorCount = internalMap.size();
      ${uncappedName}Vector = internalMap.addOrGet("${uncappedName}", Types.optional(MinorType.${name?upper_case}), Nullable${name}Vector.class);
      if (internalMap.size() > vectorCount) {
        ${uncappedName}Vector.allocateNew();
      }
    }
    return ${uncappedName}Vector;
  }

  </#if>

  </#list></#list>

  public ListVector getList() {
    if (listVector == null) {
      int vectorCount = internalMap.size();
      listVector = internalMap.addOrGet("list", Types.optional(MinorType.LIST), ListVector.class);
      if (internalMap.size() > vectorCount) {
        listVector.allocateNew();
      }
    }
    return listVector;
  }

  public int getTypeValue(int index) {
    return typeVector.getAccessor().get(index);
  }

  public UInt1Vector getTypeVector() {
    return typeVector;
  }

  @Override
  public void allocateNew() throws OutOfMemoryRuntimeException {
    internalMap.allocateNew();
  }

  @Override
  public boolean allocateNewSafe() {
    return internalMap.allocateNewSafe();
  }

  @Override
  public void setInitialCapacity(int numRecords) {
  }

  @Override
  public int getValueCapacity() {
    return Math.min(typeVector.getValueCapacity(), internalMap.getValueCapacity());
  }

  @Override
  public void close() {
  }

  @Override
  public void clear() {
    internalMap.clear();
  }

  @Override
  public MaterializedField getField() {
    return field;
  }

  @Override
  public TransferPair getTransferPair() {
    return new TransferImpl(field);
  }

  @Override
  public TransferPair getTransferPair(FieldReference ref) {
    return new TransferImpl(field.withPath(ref));
  }

  @Override
  public TransferPair makeTransferPair(ValueVector target) {
    return new TransferImpl((EmbeddedVector) target);
  }

  public void transferTo(EmbeddedVector target) {
    internalMap.makeTransferPair(target.internalMap).transfer();
    target.getMutator().setValueCount(this.valueCount);
  }

  public void copyFrom(int inIndex, int outIndex, EmbeddedVector from) {
    internalMap.copyFromSafe(inIndex, outIndex, from.internalMap);
  }

  private class TransferImpl implements TransferPair {

    EmbeddedVector to;

    public TransferImpl(MaterializedField field) {
      to = new EmbeddedVector(field, allocator);
    }

    public TransferImpl(EmbeddedVector to) {
      this.to = to;
    }

    @Override
    public void transfer() {
      transferTo(to);
    }

    @Override
    public void splitAndTransfer(int startIndex, int length) {

    }

    @Override
    public ValueVector getTo() {
      return to;
    }

    @Override
    public void copyValueSafe(int from, int to) {
      this.to.copyFrom(from, to, EmbeddedVector.this);
    }
  }

  @Override
  public Accessor getAccessor() {
    return accessor;
  }

  @Override
  public Mutator getMutator() {
    return mutator;
  }

  @Override
  public FieldReader getReader() {
    if (reader == null) {
      reader = new EmbeddedReader(this);
    }
    return reader;
  }

  @Override
  public UserBitShared.SerializedField getMetadata() {
    SerializedField.Builder b = getField() //
            .getAsBuilder() //
            .setBufferLength(getBufferSize()) //
            .setValueCount(valueCount);

    b.addChild(internalMap.getMetadata());
    return b.build();
  }

  @Override
  public int getBufferSize() {
    return internalMap.getBufferSize();
  }

  @Override
  public DrillBuf[] getBuffers(boolean clear) {
    return internalMap.getBuffers(clear);
  }

  @Override
  public void load(UserBitShared.SerializedField metadata, DrillBuf buffer) {
    valueCount = metadata.getValueCount();

    internalMap.load(metadata.getChild(0), buffer);
  }

  @Override
  public Iterator<ValueVector> iterator() {
    return null;
  }

  public class Accessor extends BaseValueVector.BaseAccessor {


    @Override
    public Object getObject(int index) {
      int type = typeVector.getAccessor().get(index);
      switch (type) {
      case 0:
        return null;
      <#list vv.types as type><#list type.minor as minor><#assign name = minor.class?cap_first />
      <#assign fields = minor.fields!type.fields />
      <#assign uncappedName = name?uncap_first/>
      <#if !minor.class?starts_with("Decimal")>
      case MinorType.${name?upper_case}_VALUE:
        return get${name}Vector().getAccessor().getObject(index);
      </#if>

      </#list></#list>
      case MinorType.MAP_VALUE:
        return getMap().getAccessor().getObject(index);
      case MinorType.LIST_VALUE:
        return getList().getAccessor().getObject(index);
      default:
        throw new UnsupportedOperationException();
      }
    }

    public byte[] get(int index) {
      return null;
    }

    public void get(int index, ComplexHolder holder) {
    }

    public void get(int index, EmbeddedHolder holder) {
      if (reader == null) {
        reader = new EmbeddedReader(EmbeddedVector.this);
      }
      reader.setPosition(index);
      holder.reader = reader;
    }

    @Override
    public int getValueCount() {
      return valueCount;
    }

  }

  public class Mutator extends BaseValueVector.BaseMutator {

    EmbeddedWriter writer;

    @Override
    public void setValueCount(int valueCount) {
      EmbeddedVector.this.valueCount = valueCount;
      internalMap.getMutator().setValueCount(valueCount);
    }

    public void set(int index, byte[] bytes) {
    }

    public void setSafe(int index, EmbeddedHolder holder) {
      FieldReader reader = holder.reader;
      if (writer == null) {
        writer = new EmbeddedWriter(EmbeddedVector.this);
      }
      writer.setPosition(index);
      MinorType type = reader.getType().getMinorType();
      switch (type) {
      case BIGINT:
        reader.copyAsValue(writer.asBigInt());
        break;
      case BIT:
        reader.copyAsValue(writer.asBit());
      case VARCHAR:
        reader.copyAsValue(writer.asVarChar());
        break;
      case MAP:
        reader.copyAsValue(writer.asMap());
        break;
      default:
        throw new UnsupportedOperationException();
      }
    }

    public void setType(int index, MinorType type) {
      typeVector.getMutator().setSafe(index, type.getNumber());
    }

    @Override
    public void reset() { }

    @Override
    public void generateTestData(int values) { }
  }
}
