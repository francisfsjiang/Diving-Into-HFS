/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.fs;

import java.io.File;
import java.io.IOException;
import java.io.BufferedReader;

import java.util.EnumSet;
import java.util.StringTokenizer;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.util.Shell;

////////////////////////////////////////
//
// 磁盘空间状态
// 继承自{org.apache.hadoop.util.Shell}，在Shell中调用系统工具
// {df -k <path>}实现，该命令无法在Windows环境下使用
// 一种选择是在Windows环境下使用{fsutil volume diskfree <path>}替换
//
/** Filesystem disk space usage statistics.
 * Uses the unix 'df' program to get mount points, and java.io.File for
 * space utilization. Tested on Linux, FreeBSD, Cygwin. */
@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
@InterfaceStability.Evolving
public class DF extends Shell {

  /** Default DF refresh interval. */
  public static final long DF_INTERVAL_DEFAULT = 3 * 1000;

  private final String dirPath;
  private final File dirFile;
  private String filesystem;
  private String mount;

  ////////////////////////////////
  //
  // 操作系统类型
  // 似乎放这不合适
  //
  enum OSType {
    OS_TYPE_UNIX("UNIX"),
    OS_TYPE_WIN("Windows"),
    OS_TYPE_SOLARIS("SunOS"),
    OS_TYPE_MAC("Mac"),
    OS_TYPE_AIX("AIX");

    private String id;
    OSType(String id) {
      this.id = id;
    }
    public boolean match(String osStr) {
      return osStr != null && osStr.indexOf(id) >= 0;
    }
    String getId() {
      return id;
    }
  }

  private static final String OS_NAME = System.getProperty("os.name");
  private static final OSType OS_TYPE = getOSType(OS_NAME);

  protected static OSType getOSType(String osName) {
    for (OSType ost : EnumSet.allOf(OSType.class)) {
      if (ost.match(osName)) {
        return ost;
      }
    }
    return OSType.OS_TYPE_UNIX;
  }

  public DF(File path, Configuration conf) throws IOException {
    this(path, conf.getLong(CommonConfigurationKeys.FS_DF_INTERVAL_KEY, DF.DF_INTERVAL_DEFAULT));
  }

  public DF(File path, long dfInterval) throws IOException {
    super(dfInterval);
    this.dirPath = path.getCanonicalPath();
    this.dirFile = new File(this.dirPath);
  }

  protected OSType getOSType() {
    return OS_TYPE;
  }
  
  /// ACCESSORS

  /** @return the canonical path to the volume we're checking. */
  public String getDirPath() {
    return dirPath;
  }

  /** @return a string indicating which filesystem volume we're checking. */
  public String getFilesystem() throws IOException {
    run();
    return filesystem;
  }

  /** @return the capacity of the measured filesystem in bytes. */
  public long getCapacity() {
    return dirFile.getTotalSpace();
  }

  /** @return the total used space on the filesystem in bytes. */
  public long getUsed() {
    return dirFile.getTotalSpace() - dirFile.getFreeSpace();
  }

  /** @return the usable space remaining on the filesystem in bytes. */
  public long getAvailable() {
    return dirFile.getUsableSpace();
  }

  /** @return the amount of the volume full, as a percent. */
  public int getPercentUsed() {
    double cap = (double) getCapacity();
    double used = (cap - (double) getAvailable());
    return (int) (used * 100.0 / cap);
  }

  /** @return the filesystem mount point for the indicated volume */
  public String getMount() throws IOException {
    run();
    return mount;
  }
  
  public String toString() {
    return
      "df -k " + mount +"\n" +
      filesystem + "\t" +
      getCapacity() / 1024 + "\t" +
      getUsed() / 1024 + "\t" +
      getAvailable() / 1024 + "\t" +
      getPercentUsed() + "%\t" +
      mount;
  }

  // 检测命令
  @Override
  protected String[] getExecString() {
    // ignoring the error since the exit code it enough
    return new String[] {"bash","-c","exec 'df' '-k' '" + dirPath + "' 2>/dev/null"};
  }

  // 解析输出
  @Override
  protected void parseExecResult(BufferedReader lines) throws IOException {
    lines.readLine();                         // skip headings
  
    String line = lines.readLine();
    if (line == null) {
      throw new IOException( "Expecting a line not the end of stream" );
    }
    StringTokenizer tokens = new StringTokenizer(line, " \t\n\r\f%");
    
    this.filesystem = tokens.nextToken();
    if (!tokens.hasMoreTokens()) {            // for long filesystem name
      line = lines.readLine();
      if (line == null) {
        throw new IOException( "Expecting a line not the end of stream" );
      }
      tokens = new StringTokenizer(line, " \t\n\r\f%");
    }

    switch(getOSType()) {
      case OS_TYPE_AIX:
        Long.parseLong(tokens.nextToken()); // capacity
        Long.parseLong(tokens.nextToken()); // available
        Integer.parseInt(tokens.nextToken()); // pct used
        tokens.nextToken();
        tokens.nextToken();
        this.mount = tokens.nextToken();
        break;

      case OS_TYPE_WIN:
      case OS_TYPE_SOLARIS:
      case OS_TYPE_MAC:
      case OS_TYPE_UNIX:
      default:
        Long.parseLong(tokens.nextToken()); // capacity
        Long.parseLong(tokens.nextToken()); // used
        Long.parseLong(tokens.nextToken()); // available
        Integer.parseInt(tokens.nextToken()); // pct used
        this.mount = tokens.nextToken();
        break;
   }
  }

  public static void main(String[] args) throws Exception {
    String path = ".";
    if (args.length > 0)
      path = args[0];

    System.out.println(new DF(new File(path), DF_INTERVAL_DEFAULT).toString());
  }
}
