package org.apache.drill.exec.store.dfs;

import java.util.Map;

import org.apache.drill.common.logical.FormatPluginConfig;
import org.apache.drill.common.logical.StoragePluginConfig;

import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName("file")
public class FileSystemConfig implements StoragePluginConfig{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FileSystemConfig.class);
  
  public String connection;
  public Map<String, String> workspaces;
  public Map<String, FormatPluginConfig> formats;
  
}
