/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.record;

import org.apache.drill.common.expression.PathSegment;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.complex.AbstractContainerVector;
import org.apache.drill.exec.vector.complex.ListVector;
import org.apache.drill.exec.vector.complex.UnionVector;

import java.util.List;

public abstract class BaseVectorWrapper implements VectorWrapper {

  protected TypedFieldId getFieldIdIfMatches(ValueVector vector, int id, SchemaPath expectedPath) {
    if (!expectedPath.getRootSegment().segmentEquals(vector.getField().getPath().getRootSegment())) {
      return null;
    }
    PathSegment seg = expectedPath.getRootSegment();

    if (vector instanceof UnionVector) {
      TypedFieldId.Builder builder = TypedFieldId.newBuilder();
      builder.addId(id).remainder(expectedPath.getRootSegment().getChild());
      List<MinorType> minorTypes = ((UnionVector) vector).getSubTypes();
      MajorType.Builder majorTypeBuilder = MajorType.newBuilder().setMinorType(MinorType.UNION);
      for (MinorType type : minorTypes) {
        majorTypeBuilder.addSubType(type);
      }
      MajorType majorType = majorTypeBuilder.build();
      builder.intermediateType(majorType);
      if (seg.isLastPath()) {
        builder.finalType(majorType);
        return builder.build();
      } else {
        return ((UnionVector) vector).getFieldIdIfMatches(builder, false, seg.getChild());
      }
    } else if (vector instanceof ListVector) {
      ListVector list = (ListVector) vector;
      TypedFieldId.Builder builder = TypedFieldId.newBuilder();
      builder.intermediateType(vector.getField().getType());
      builder.addId(id);
      return list.getFieldIdIfMatches(builder, true, expectedPath.getRootSegment().getChild());
    } else
    if (vector instanceof AbstractContainerVector) {
      // we're looking for a multi path.
      AbstractContainerVector c = (AbstractContainerVector) vector;
      TypedFieldId.Builder builder = TypedFieldId.newBuilder();
      builder.intermediateType(vector.getField().getType());
      builder.addId(id);
      return c.getFieldIdIfMatches(builder, true, expectedPath.getRootSegment().getChild());

    } else {
      TypedFieldId.Builder builder = TypedFieldId.newBuilder();
      builder.intermediateType(vector.getField().getType());
      builder.addId(id);
      builder.finalType(vector.getField().getType());
      if (seg.isLastPath()) {
        return builder.build();
      } else {
        PathSegment child = seg.getChild();
        if (child.isArray() && child.isLastPath()) {
          builder.remainder(child);
          builder.withIndex();
          builder.finalType(vector.getField().getType().toBuilder().setMode(DataMode.OPTIONAL).build());
          return builder.build();
        } else {
          return null;
        }

      }
    }
  }
}
