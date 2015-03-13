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
package org.apache.drill.exec.store.parquet;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator.Feature;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.expression.SchemaPath.De;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.store.TimedRunnable;
import org.apache.drill.exec.store.dfs.DrillPathFilter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import parquet.column.statistics.Statistics;
import parquet.hadoop.ParquetFileReader;
import parquet.hadoop.metadata.BlockMetaData;
import parquet.hadoop.metadata.ColumnChunkMetaData;
import parquet.hadoop.metadata.ParquetMetadata;
import parquet.schema.GroupType;
import parquet.schema.OriginalType;
import parquet.schema.PrimitiveType.PrimitiveTypeName;
import parquet.schema.Type;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class Metadata {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Metadata.class);

  public static void createMeta(FileSystem fs, String path) throws IOException {
    FileStatus fileStatus = fs.getFileStatus(new Path(path));
    Preconditions.checkArgument(fileStatus.isDirectory(), "Can only create metadata file for directories");
    createMetaFilesRecursively(fs, path);
  }

  private static ParquetTableMetadata createMetaFilesRecursively(final FileSystem fs, final String path) throws IOException {
    List<ParquetFileMetadata> metaDataList = Lists.newArrayList();
    List<String> directoryList = Lists.newArrayList();
    Path p = new Path(path);
    FileStatus fileStatus = fs.getFileStatus(p);
    assert fileStatus.isDirectory() : "Expected directory";

    final List<FileStatus> childFiles = Lists.newArrayList();

    for (final FileStatus file : fs.listStatus(p, new DrillPathFilter())) {
      if (file.isDirectory()) {
        ParquetTableMetadata subTableMetadata = createMetaFilesRecursively(fs, file.getPath().toString());
        metaDataList.addAll(subTableMetadata.files);
        directoryList.addAll(subTableMetadata.directories);
        directoryList.add(file.getPath().toString());
      } else {
        childFiles.add(file);
      }
    }
    if (childFiles.size() > 0) {
      metaDataList.addAll(getParquetFileMetadata(fs, childFiles));
    }
    ParquetTableMetadata parquetTableMetadata = new ParquetTableMetadata(metaDataList, directoryList);
    writeFile(parquetTableMetadata, fs, new Path(p, ".drill.parquet_metadata"));
    return parquetTableMetadata;
  }


  public static ParquetTableMetadata getParquetTableMetadata(FileSystem fs, String path) throws IOException {
    Path p = new Path(path);
    FileStatus fileStatus = fs.getFileStatus(p);
    Stopwatch watch = new Stopwatch();
    watch.start();
    List<FileStatus> fileStatuses = getFileStatuses(fs, fileStatus);
    logger.info("Took {} ms to get file statuses", watch.elapsed(TimeUnit.MILLISECONDS));
    return getParquetTableMetadata(fs, fileStatuses);
  }

  public static ParquetTableMetadata getParquetTableMetadata(FileSystem fs, List<FileStatus> fileStatuses) throws IOException {
    List<ParquetFileMetadata> fileMetadataList = getParquetFileMetadata(fs, fileStatuses);
    return new ParquetTableMetadata(fileMetadataList, new ArrayList<String>());
  }

  private static List<ParquetFileMetadata> getParquetFileMetadata(FileSystem fs, List<FileStatus> fileStatuses) throws IOException {
    List<TimedRunnable<ParquetFileMetadata>> gatherers = Lists.newArrayList();
    for (FileStatus file : fileStatuses) {
      gatherers.add(new MetadataGatherer(fs, file));
    }

    List<ParquetFileMetadata> metaDataList = Lists.newArrayList();
    metaDataList.addAll(TimedRunnable.run("Fetch parquet metadata", logger, gatherers, 16));
    return metaDataList;
  }

  /*
  private static ParquetTableMetadata getParquetTableMetadata(List<FileMetaDirectoryListPair> metaPairs) {
    Map<SchemaPath,Long> columnValueCounts = Maps.newHashMap();
    List<ParquetFileMetadata> fileMetadataList = Lists.newArrayList();
    long rowCount = 0;
    for (FileMetaDirectoryListPair f : metaPairs) {
      for (ColumnWithValueCount column : f.columnValueCounts) {
        SchemaPath schemaPath = column.column;
        Long count = columnValueCounts.get(schemaPath);
        if (count == null) {
          columnValueCounts.put(schemaPath, column.valueCount);
        } else {
          long newCount = (count < 0 || column.valueCount < 0) ? GroupScan.NO_COLUMN_STATS : count + column.valueCount;
          columnValueCounts.put(schemaPath, newCount);
        }
      }
      fileMetadataList.add(f.parquetFileMetadata);
      rowCount += f.parquetFileMetadata.rowCount;
    }
    List<ColumnWithValueCount> columnsWithValueCounts = Lists.newArrayList();
    for (SchemaPath schemaPath : columnValueCounts.keySet()) {
      ColumnWithValueCount cvc = new ColumnWithValueCount(schemaPath, columnValueCounts.get(schemaPath));
      columnsWithValueCounts.add(cvc);
    }

    ParquetTableMetadata parquetTableMetadata = new ParquetTableMetadata(fileMetadataList);
    return parquetTableMetadata;
  }
  */

  private static List<FileStatus> getFileStatuses(FileSystem fs, FileStatus fileStatus) throws IOException {
    List<FileStatus> statuses = Lists.newArrayList();
    if (fileStatus.isDirectory()) {
      for (FileStatus child : fs.listStatus(fileStatus.getPath(), new DrillPathFilter())) {
        statuses.addAll(getFileStatuses(fs, child));
      }
    } else {
      statuses.add(fileStatus);
    }
    return statuses;
  }

  private static class MetadataGatherer extends TimedRunnable<ParquetFileMetadata> {

    private FileSystem fs;
    private FileStatus fileStatus;

    public MetadataGatherer(FileSystem fs, FileStatus fileStatus) {
      this.fs = fs;
      this.fileStatus = fileStatus;
    }

    @Override
    protected ParquetFileMetadata runInner() throws Exception {
      return getParquetFileMetadata(fs, fileStatus);
    }

    @Override
    protected IOException convertToIOException(Exception e) {
      return null;
    }
  }

  public static void main(String[] args) throws Exception {
    FileSystem fs = FileSystem.get(new Configuration());
    String file = "/tmp/nation";
    createMeta(fs, file);
  }

  private static ParquetFileMetadata getParquetFileMetadata(FileSystem fs, FileStatus file) throws IOException {
    ParquetMetadata metadata = ParquetFileReader.readFooter(fs.getConf(), file);
    GroupType schema = metadata.getFileMetaData().getSchema();

    //Todo handle complex types
    Map<String,OriginalType> originalTypeMap = Maps.newHashMap();
    for (Type type : schema.getFields()) {
      originalTypeMap.put(type.getName(), type.getOriginalType());
    }

    List<RowGroupMetadata> rowGroupMetadataList = Lists.newArrayList();

    for (BlockMetaData rowGroup : metadata.getBlocks()) {
      List<ColumnMetadata> columnMetadataList = Lists.newArrayList();
      long length = 0;
      for (ColumnChunkMetaData col : rowGroup.getColumns()) {
        ColumnMetadata columnMetadata;

        boolean statsAvailable = (col.getStatistics() != null && !col.getStatistics().isEmpty());

        Statistics stats = col.getStatistics();
        String columnName = col.getPath().toString().replace("[", "").replace("]", "").toLowerCase();
        if (statsAvailable) {
          columnMetadata = new ColumnMetadata(columnName, col.getType(), originalTypeMap.get(columnName),
              stats.genericGetMax(), stats.genericGetMin(), stats.getNumNulls());
        } else {
          columnMetadata = new ColumnMetadata(columnName, col.getType(), originalTypeMap.get(columnName),
              null, null, null);
        }
        columnMetadataList.add(columnMetadata);
        length += col.getTotalSize();
      }

      RowGroupMetadata rowGroupMeta = new RowGroupMetadata(rowGroup.getStartingPos(), length, rowGroup.getRowCount(), getHostAffinity(fs, file, rowGroup.getStartingPos(), length), columnMetadataList);

      rowGroupMetadataList.add(rowGroupMeta);
    }
    String path = Path.getPathWithoutSchemeAndAuthority(file.getPath()).toString();

    return new ParquetFileMetadata(path, file.getLen(), rowGroupMetadataList);
  }

  private static Map<String,Float> getHostAffinity(FileSystem fs, FileStatus fileStatus, long start, long length) throws IOException {
    BlockLocation[] blockLocations = fs.getFileBlockLocations(fileStatus, start, length);
    Map<String,Float> hostAffinityMap = Maps.newHashMap();
    for (BlockLocation blockLocation : blockLocations) {
      for (String host : blockLocation.getHosts()) {
        Float currentAffinity = hostAffinityMap.get(host);
        float blockStart = blockLocation.getOffset();
        float blockEnd = blockStart + blockLocation.getLength();
        float rowGroupEnd = start + length;
        Float newAffinity = (blockLocation.getLength() - (blockStart < start ? start - blockStart : 0) - (blockEnd > rowGroupEnd ? blockEnd - rowGroupEnd : 0)) / length;
        if (currentAffinity != null) {
          hostAffinityMap.put(host, currentAffinity + newAffinity);
        } else {
          hostAffinityMap.put(host, newAffinity);
        }
      }
    }
    return hostAffinityMap;
  }

  private static void writeFile(ParquetTableMetadata parquetTableMetadata, FileSystem fs, Path p) throws IOException {
    JsonFactory jsonFactory = new JsonFactory();
    jsonFactory.configure(Feature.AUTO_CLOSE_TARGET, false);
    jsonFactory.configure(JsonParser.Feature.AUTO_CLOSE_SOURCE, false);
    ObjectMapper mapper = new ObjectMapper(jsonFactory);
    FSDataOutputStream os = fs.create(p);
    mapper.writerWithDefaultPrettyPrinter().writeValue(os, parquetTableMetadata);
    os.flush();
    os.close();
  }

  public static ParquetTableMetadata readBlockMeta(DrillConfig config, FileSystem fs, String path) throws IOException {
    Path p = new Path(path);
    ObjectMapper mapper = new ObjectMapper();
    SimpleModule module = new SimpleModule();
    module.addDeserializer(SchemaPath.class, new De(config));
    mapper.registerModule(module);
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    FSDataInputStream is = fs.open(p);
    ParquetTableMetadata parquetTableMetadata = mapper.readValue(is, ParquetTableMetadata.class);
    if (tableModified(fs, parquetTableMetadata, p)) {
      parquetTableMetadata = createMetaFilesRecursively(fs, Path.getPathWithoutSchemeAndAuthority(p.getParent()).toString());
    }
    return parquetTableMetadata;
  }

  private static boolean tableModified(FileSystem fs, ParquetTableMetadata tableMetadata, Path metaFilePath) throws IOException {
    long metaFileModifyTime = fs.getFileStatus(metaFilePath).getModificationTime();
    FileStatus directoryStatus = fs.getFileStatus(metaFilePath.getParent());
    if (directoryStatus.getModificationTime() > metaFileModifyTime) {
      return true;
    }
    for (String directory : tableMetadata.directories) {
      directoryStatus = fs.getFileStatus(new Path(directory));
      if (directoryStatus.getModificationTime() > metaFileModifyTime) {
        return true;
      }
    }
    return false;
  }

  public static class ParquetTableMetadata {
    @JsonProperty
    List<ParquetFileMetadata> files;
    @JsonProperty
    List<String> directories;

    public ParquetTableMetadata() {
      super();
    }

    public ParquetTableMetadata(List<ParquetFileMetadata> files, List<String> directories) {
      this.files = files;
      this.directories = directories;
    }
  }

  public static class FileMetaDirectoryListPair {
    ParquetFileMetadata parquetFileMetadata;
    List<String> directories;

    public FileMetaDirectoryListPair(ParquetFileMetadata parquetFileMetadata, List<String> directories) {
      this.parquetFileMetadata = parquetFileMetadata;
      this.directories = directories;
    }
  }

  public static class ParquetFileMetadata {
    @JsonProperty
    public String path;
    @JsonProperty
    public Long size;
    @JsonProperty
    public List<RowGroupMetadata> rowGroups;

    public ParquetFileMetadata() {
      super();
    }

    public ParquetFileMetadata(String path, Long size, List<RowGroupMetadata> rowGroups) {
      this.path = path;
      this.size = size;
      this.rowGroups = rowGroups;
    }

    @Override
    public String toString() {
      return String.format("path: %s rowGroups: %s", path, rowGroups);
    }
  }

  public static class RowGroupMetadata {
    @JsonProperty
    public Long start;
    @JsonProperty
    public Long length;
    @JsonProperty
    public Long rowCount;
    @JsonProperty
    public Map<String, Float> hostAffinity;
    @JsonProperty
    public List<ColumnMetadata> columns;

    public RowGroupMetadata() {
      super();
    }

    public RowGroupMetadata(Long start, Long length, Long rowCount, Map<String, Float> hostAffinity, List<ColumnMetadata> columns) {
      this.start = start;
      this.length = length;
      this.rowCount = rowCount;
      this.hostAffinity = hostAffinity;
      this.columns = columns;
    }
  }

  public static class ColumnMetadata {
    @JsonProperty
    public String name;
    @JsonProperty
    public PrimitiveTypeName primitiveType;
    @JsonProperty
    public OriginalType originalType;
    @JsonProperty
    public Object max;
    @JsonProperty
    public Object min;
    @JsonProperty
    public Long nulls;

    public ColumnMetadata() {
      super();
    }

    public ColumnMetadata(String name, PrimitiveTypeName primitiveType, OriginalType originalType, Object max, Object min, Long nulls) {
      this.name = name;
      this.primitiveType = primitiveType;
      this.originalType = originalType;
      this.max = max;
      this.min = min;
      this.nulls = nulls;
    }
  }

  public static class ColumnWithValueCount {
    @JsonProperty
    public SchemaPath column;
    @JsonProperty
    public Long valueCount;

    public ColumnWithValueCount() {
      super();
    }

    public ColumnWithValueCount(SchemaPath column, Long valueCount) {
      this.column = column;
      this.valueCount = valueCount;
    }
  }
}
