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

import java.io.*;
import java.util.*;

import org.apache.commons.logging.*;

import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.DiskChecker.DiskErrorException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration; 

/**
 * 该类是创建文件时磁盘分配round-robin模式的一种实现。
 * 它实现的方式就是持续跟踪磁盘最后是被一个文件写在什么位置的。
 */
@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
@InterfaceStability.Unstable
public class LocalDirAllocator {
  
  private static Map <String, AllocatorPerContext> contexts = 
                 new TreeMap<String, AllocatorPerContext>();
  private String contextCfgItemName;

  public static final int SIZE_UNKNOWN = -1;

  public LocalDirAllocator(String contextCfgItemName) {
    this.contextCfgItemName = contextCfgItemName;
  }
  
  private AllocatorPerContext obtainContext(String contextCfgItemName) {
    synchronized (contexts) {
      AllocatorPerContext l = contexts.get(contextCfgItemName);
      if (l == null) {
        contexts.put(contextCfgItemName, 
                    (l = new AllocatorPerContext(contextCfgItemName)));
      }
      return l;
    }
  }
  
  /**
   * 为了写入获得本地文件系统的一个路径
   */
  public Path getLocalPathForWrite(String pathStr, 
      Configuration conf) throws IOException {
    return getLocalPathForWrite(pathStr, SIZE_UNKNOWN, conf);
  }
  
  public Path getLocalPathForWrite(String pathStr, long size, 
      Configuration conf) throws IOException {
    AllocatorPerContext context = obtainContext(contextCfgItemName);
    return context.getLocalPathForWrite(pathStr, size, conf);
  }
  
  /**
   * 为了读入获得本地文件系统的一个路径
   */
  public Path getLocalPathToRead(String pathStr, 
      Configuration conf) throws IOException {
    AllocatorPerContext context = obtainContext(contextCfgItemName);
    return context.getLocalPathToRead(pathStr, conf);
  }

  /**
   * 为了写入创建一个临时文件
   */
  public File createTmpFileForWrite(String pathStr, long size, 
      Configuration conf) throws IOException {
    AllocatorPerContext context = obtainContext(contextCfgItemName);
    return context.createTmpFileForWrite(pathStr, size, conf);
  }
  
  /**
   * 判断内容是否有效
   */
  public static boolean isContextValid(String contextCfgItemName) {
    synchronized (contexts) {
      return contexts.containsKey(contextCfgItemName);
    }
  }
    
  /**
   * 判断路径是否存在
   */
  public boolean ifExists(String pathStr,Configuration conf) {
    AllocatorPerContext context = obtainContext(contextCfgItemName);
    return context.ifExists(pathStr, conf);
  }

  /**
   * 获得当前目录的索引号
   */
  int getCurrentDirectoryIndex() {
    AllocatorPerContext context = obtainContext(contextCfgItemName);
    return context.getCurrentDirectoryIndex();
  }
  
  private static class AllocatorPerContext {

    private final Log LOG =
      LogFactory.getLog(AllocatorPerContext.class);

    private int dirNumLastAccessed;
    private Random dirIndexRandomizer = new Random();
    private FileSystem localFS;
    private DF[] dirDF;
    private String contextCfgItemName;
    private String[] localDirs;
    private String savedLocalDirs = "";

    public AllocatorPerContext(String contextCfgItemName) {
      this.contextCfgItemName = contextCfgItemName;
    }

    /**
     * 改变配置信息
     */
    private void confChanged(Configuration conf) throws IOException {
      String newLocalDirs = conf.get(contextCfgItemName);
      if (!newLocalDirs.equals(savedLocalDirs)) {
        localDirs = conf.getTrimmedStrings(contextCfgItemName);
        localFS = FileSystem.getLocal(conf);
        int numDirs = localDirs.length;
        ArrayList<String> dirs = new ArrayList<String>(numDirs);
        ArrayList<DF> dfList = new ArrayList<DF>(numDirs);
        for (int i = 0; i < numDirs; i++) {
          try {
            Path tmpDir = new Path(localDirs[i]);
            if(localFS.mkdirs(tmpDir)|| localFS.exists(tmpDir)) {
              try {
                DiskChecker.checkDir(new File(localDirs[i]));
                dirs.add(localDirs[i]);
                dfList.add(new DF(new File(localDirs[i]), 30000));
              } catch (DiskErrorException de) {
                LOG.warn( localDirs[i] + "is not writable\n" +
                    StringUtils.stringifyException(de));
              }
            } else {
              LOG.warn( "Failed to create " + localDirs[i]);
            }
          } catch (IOException ie) { 
            LOG.warn( "Failed to create " + localDirs[i] + ": " +
                ie.getMessage() + "\n" + StringUtils.stringifyException(ie));
          } 
        }
        localDirs = dirs.toArray(new String[dirs.size()]);
        dirDF = dfList.toArray(new DF[dirs.size()]);
        savedLocalDirs = newLocalDirs;

        dirNumLastAccessed = dirIndexRandomizer.nextInt(dirs.size());
      }
    }

    /**
     * 创建路径
     */
    private Path createPath(String path) throws IOException {
      Path file = new Path(new Path(localDirs[dirNumLastAccessed]),
                                    path);
      try {
        DiskChecker.checkDir(new File(file.getParent().toUri().getPath()));
        return file;
      } catch (DiskErrorException d) {
        LOG.warn(StringUtils.stringifyException(d));
        return null;
      }
    }

    int getCurrentDirectoryIndex() {
      return dirNumLastAccessed;
    }
    
    public synchronized Path getLocalPathForWrite(String path, 
        Configuration conf) throws IOException {
      return getLocalPathForWrite(path, SIZE_UNKNOWN, conf);
    }

    /**
     * 采用round-robin模式从本地文件系统中获取一个路径
     */
    public synchronized Path getLocalPathForWrite(String pathStr, long size, 
        Configuration conf) throws IOException {
      confChanged(conf);
      int numDirs = localDirs.length;
      int numDirsSearched = 0;
      if (pathStr.startsWith("/")) {
        pathStr = pathStr.substring(1);
      }
      Path returnPath = null;
      
      if(size == SIZE_UNKNOWN) {  
        long[] availableOnDisk = new long[dirDF.length];
        long totalAvailable = 0;
        
        for(int i =0; i < dirDF.length; ++i) {
          availableOnDisk[i] = dirDF[i].getAvailable();
          totalAvailable += availableOnDisk[i];
        }

        Random r = new java.util.Random();
        while (numDirsSearched < numDirs && returnPath == null) {
          long randomPosition = Math.abs(r.nextLong()) % totalAvailable;
          int dir = 0;
          while (randomPosition > availableOnDisk[dir]) {
            randomPosition -= availableOnDisk[dir];
            dir++;
          }
          dirNumLastAccessed = dir;
          returnPath = createPath(pathStr);
          if (returnPath == null) {
            totalAvailable -= availableOnDisk[dir];
            availableOnDisk[dir] = 0; // skip this disk
            numDirsSearched++;
          }
        }
      } else {
        while (numDirsSearched < numDirs && returnPath == null) {
          long capacity = dirDF[dirNumLastAccessed].getAvailable();
          if (capacity > size) {
            returnPath = createPath(pathStr);
          }
          dirNumLastAccessed++;
          dirNumLastAccessed = dirNumLastAccessed % numDirs; 
          numDirsSearched++;
        } 
      }
      if (returnPath != null) {
        return returnPath;
      }
      
      throw new DiskErrorException("Could not find any valid local " +
          "directory for " + pathStr);
    }

    /**
     * 创建一个可供写入的临时文件
     */
    public File createTmpFileForWrite(String pathStr, long size, 
        Configuration conf) throws IOException {

      Path path = getLocalPathForWrite(pathStr, size, conf);
      File dir = new File(path.getParent().toUri().getPath());
      String prefix = path.getName();

      File result = File.createTempFile(prefix, null, dir);
      result.deleteOnExit();
      return result;
    }

    /**
     * 搜索所有的配置目录，返回一个找到的完全路径
     */
    public synchronized Path getLocalPathToRead(String pathStr, 
        Configuration conf) throws IOException {
      confChanged(conf);
      int numDirs = localDirs.length;
      int numDirsSearched = 0;
      if (pathStr.startsWith("/")) {
        pathStr = pathStr.substring(1);
      }
      while (numDirsSearched < numDirs) {
        Path file = new Path(localDirs[numDirsSearched], pathStr);
        if (localFS.exists(file)) {
          return file;
        }
        numDirsSearched++;
      }

      throw new DiskErrorException ("Could not find " + pathStr +" in any of" +
      " the configured local directories");
    }

    /**
     * 搜索所有的配置目录看所要找的文件是否存在。
     */
    public synchronized boolean ifExists(String pathStr,Configuration conf) {
      try {
        int numDirs = localDirs.length;
        int numDirsSearched = 0;
        if (pathStr.startsWith("/")) {
          pathStr = pathStr.substring(1);
        }
        while (numDirsSearched < numDirs) {
          Path file = new Path(localDirs[numDirsSearched], pathStr);
          if (localFS.exists(file)) {
            return true;
          }
          numDirsSearched++;
        }
      } catch (IOException e) {
      }
      return false;
    }
  }
}
