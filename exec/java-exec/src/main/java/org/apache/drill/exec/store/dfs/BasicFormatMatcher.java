package org.apache.drill.exec.store.dfs;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.drill.exec.store.dfs.shim.DrillFileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;

import com.beust.jcommander.internal.Lists;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Range;

public class BasicFormatMatcher extends FormatMatcher{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BasicFormatMatcher.class);

  private final List<Pattern> patterns;
  private final MagicStringMatcher matcher;
  protected final DrillFileSystem fs;
  protected final FormatPlugin plugin;
  
  public BasicFormatMatcher(FormatPlugin plugin, DrillFileSystem fs, List<Pattern> patterns, List<MagicString> magicStrings) {
    super();
    this.patterns = ImmutableList.copyOf(patterns);
    this.matcher = new MagicStringMatcher(magicStrings);
    this.fs = fs;
    this.plugin = plugin;
  }
  
  public BasicFormatMatcher(FormatPlugin plugin, DrillFileSystem fs, String extension){
    this(plugin, fs, //
        Lists.newArrayList(Pattern.compile(".*/\\.")), //
        (List<MagicString>) Collections.EMPTY_LIST);
  }
  
  @Override
  public boolean supportDirectoryReads() {
    return false;
  }

  @Override
  public FormatSelection isReadable(FileSelection file) throws IOException {
    if(isReadable(file.getFirstPath(fs))){
      return new FormatSelection(plugin.getConfig(), file);
    }
    return null;
  }

  protected final boolean isReadable(FileStatus status) throws IOException {
    for(Pattern p : patterns){
      if(p.matcher(status.getPath().toString()).matches()){
        return true;
      }
    }
    
    if(matcher.matches(status)) return true;
    return false;
  }
  
  
  @Override
  public FormatPlugin getFormatPlugin() {
    return plugin;
  }


  private class MagicStringMatcher{
    
    private List<RangeMagics> ranges;
    
    public MagicStringMatcher(List<MagicString> magicStrings){
      ranges = Lists.newArrayList();
      for(MagicString ms : magicStrings){
        ranges.add(new RangeMagics(ms));
      }
    }
    
    public boolean matches(FileStatus status) throws IOException{
      if(ranges.isEmpty()) return false;
      final Range<Long> fileRange = Range.closedOpen( 0L, status.getLen());
      
      try(FSDataInputStream is = fs.open(status.getPath()).getInputStream()){
        for(RangeMagics rMagic : ranges){
          Range<Long> r = rMagic.range;
          if(!fileRange.encloses(r)) continue;
          int len = (int) (r.upperEndpoint() - r.lowerEndpoint());
          byte[] bytes = new byte[len];
          is.readFully(r.lowerEndpoint(), bytes);
          for(byte[] magic : rMagic.magics){
            if(Arrays.equals(magic, bytes)) return true;  
          }
          
        }
      }
      return false;
    }
    
    private class RangeMagics{
      Range<Long> range;
      byte[][] magics;
      
      public RangeMagics(MagicString ms){
        this.range = Range.closedOpen( ms.getOffset(), (long) ms.getBytes().length);
        this.magics = new byte[][]{ms.getBytes()};
      }
    }
  }
}
