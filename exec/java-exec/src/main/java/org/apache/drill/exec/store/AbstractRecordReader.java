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

import org.apache.drill.common.expression.PathSegment;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.exception.OutOfMemoryException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.vector.ValueVector;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import java.util.Collection;
import java.util.List;
import java.util.Map;

public abstract class AbstractRecordReader implements RecordReader {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AbstractRecordReader.class);

  private static final String COL_NULL_ERROR = "Columns cannot be null. Use star column to select all fields.";
  public static final SchemaPath STAR_COLUMN = SchemaPath.getSimplePath("*");

  // For text reader, the default columns to read is "columns[0]".
  protected static final List<SchemaPath> DEFAULT_TEXT_COLS_TO_READ = ImmutableList.of(new SchemaPath(new PathSegment.NameSegment("columns", new PathSegment.ArraySegment(0))));

  private Collection<SchemaPath> columns = null;
  private boolean isStarQuery = false;
  private boolean isSkipQuery = false;
  protected long numRowsPerBatch;
  protected long numBytesPerBatch;

  public AbstractRecordReader(final FragmentContext fragmentContext, final List<SchemaPath> columns) {
    if (fragmentContext == null
            || fragmentContext.getOptions() == null
            || fragmentContext.getOptions().getOption(ExecConstants.OPERATOR_TARGET_BATCH_SIZE) == null) {
      this.numRowsPerBatch = ExecConstants.OPERATOR_TARGET_BATCH_SIZE_VALIDATOR.getDefault().num_val;
    } else {
      this.numRowsPerBatch = fragmentContext.getOptions().getOption(ExecConstants.OPERATOR_TARGET_BATCH_SIZE).num_val;
    }

    if (fragmentContext == null
            || fragmentContext.getOptions() == null
            || fragmentContext.getOptions().getOption(ExecConstants.OPERATOR_TARGET_BATCH_BYTES) == null) {
      this.numBytesPerBatch = ExecConstants.OPERATOR_TARGET_BATCH_BYTES_VALIDATOR.getDefault().num_val;
    } else {
      this.numBytesPerBatch = fragmentContext.getOptions().getOption(ExecConstants.OPERATOR_TARGET_BATCH_BYTES).num_val;
    }

    if (columns != null) {
      setColumns(columns);
    }
  }

  public long getNumRowsPerBatch() {
    return numRowsPerBatch;
  }

  public long getNumBytesPerBatch() {
    return numBytesPerBatch;
  }

  @Override
  public String toString() {
    return super.toString()
        + "[columns = " + columns
        + ", isStarQuery = " + isStarQuery
        + ", isSkipQuery = " + isSkipQuery + "]";
  }

  /**
   *
   * @param projected : The column list to be returned from this RecordReader.
   *                  1) empty column list: this is for skipAll query. It's up to each storage-plugin to
   *                  choose different policy of handling skipAll query. By default, it will use * column.
   *                  2) NULL : is NOT allowed. It requires the planner's rule, or GroupScan or ScanBatchCreator to handle NULL.
   */
  private final void setColumns(Collection<SchemaPath> projected) {
    Preconditions.checkNotNull(projected, COL_NULL_ERROR);
    isSkipQuery = projected.isEmpty();
    Collection<SchemaPath> columnsToRead = projected;

    // If no column is required (SkipQuery), by default it will use DEFAULT_COLS_TO_READ .
    // Handling SkipQuery is storage-plugin specif : JSON, text reader, parquet will override, in order to
    // improve query performance.
    if (projected.isEmpty()) {
      columnsToRead = getDefaultColumnsToRead();
    }

    isStarQuery = isStarQuery(columnsToRead);
    columns = transformColumns(columnsToRead);

    logger.debug("columns to read : {}", columns);
  }

  protected Collection<SchemaPath> getColumns() {
    return columns;
  }

  protected Collection<SchemaPath> transformColumns(Collection<SchemaPath> projected) {
    return projected;
  }

  protected boolean isStarQuery() {
    return isStarQuery;
  }

  /**
   * Returns true if reader should skip all of the columns, reporting number of records only. Handling of a skip query
   * is storage plugin-specific.
   */
  protected boolean isSkipQuery() {
    return isSkipQuery;
  }

  public static boolean isStarQuery(Collection<SchemaPath> projected) {
    return Iterables.tryFind(Preconditions.checkNotNull(projected, COL_NULL_ERROR), new Predicate<SchemaPath>() {
      @Override
      public boolean apply(SchemaPath path) {
        return Preconditions.checkNotNull(path).equals(STAR_COLUMN);
      }
    }).isPresent();
  }

  @Override
  public void allocate(Map<String, ValueVector> vectorMap) throws OutOfMemoryException {
    for (final ValueVector v : vectorMap.values()) {
      v.allocateNew();
    }
  }

  protected List<SchemaPath> getDefaultColumnsToRead() {
    return GroupScan.ALL_COLUMNS;
  }

}
