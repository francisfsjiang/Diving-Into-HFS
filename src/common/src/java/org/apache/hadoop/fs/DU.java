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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Shell;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 文件夹的磁盘使用状态
 * 继承自{org.apache.hadoop.util.Shell}，在Shell中调用系统工具
 * 使用<code>du -sk <path></code>实现，该命令无法在Windows环境下使用
 * 在Windows环境下使用<code>dir <path></code>能得到相关信息，但无关的输出过多
 * 内部使用一个线程定期执行命令刷新状态
 */
@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
@InterfaceStability.Evolving
public class DU extends Shell {
  private String  dirPath;

  private AtomicLong used = new AtomicLong();
  private volatile boolean shouldRun = true;
  private Thread refreshUsed;
  private IOException duException = null;
  private long refreshInterval;
  
  /**
   * 设置磁盘监控的路径以及时间间隔，开始保持对磁盘使用量的监控，
   */
  public DU(File path, long interval) throws IOException {
    super(0);
    
    //we set the Shell interval to 0 so it will always run our command
    //and use this one to set the thread sleep interval
    this.refreshInterval = interval;
    this.dirPath = path.getCanonicalPath();
    
    //populate the used variable
    run();
  }

  public DU(File path, Configuration conf) throws IOException {
    this(path, 600000L);
    //10 minutes default refresh interval
  }

  /**
   * 刷新线程
   */
  class DURefreshThread implements Runnable {
    
    public void run() {
      
      while(shouldRun) {

        try {
          Thread.sleep(refreshInterval);
          
          try {
            //update the used variable
            DU.this.run();
          } catch (IOException e) {
            synchronized (DU.this) {
              //save the latest exception so we can return it in getUsed()
              duException = e;
            }
            
            LOG.warn("Could not get disk usage information", e);
          }
        } catch (InterruptedException e) {
        }
      }
    }
  }
  
  /**
   * 减少系统预留的磁盘空间
   */
  public void decDfsUsed(long value) {
    used.addAndGet(-value);
  }

  /**
   * 增加系统预留的磁盘空间
   */
  public void incDfsUsed(long value) {
    used.addAndGet(value);
  }
  
  /**
   * 返回磁盘使用量
   */
  public long getUsed() throws IOException {
    //if the updating thread isn't started, update on demand
    if(refreshUsed == null) {
      run();
    } else {
      synchronized (DU.this) {
        //if an exception was thrown in the last run, rethrow
        if(duException != null) {
          IOException tmp = duException;
          duException = null;
          throw tmp;
        }
      }
    }
    
    return used.longValue();
  }

  /**
   * 返回被监控的路径
   */
  public String getDirPath() {
    return dirPath;
  }
  
  /**
   * 开启一个进程，来监控磁盘使用
   */
  public void start() {
    //only start the thread if the interval is sane
    if (refreshInterval > 0) {
      refreshUsed = new Thread(new DURefreshThread(), "refreshUsed-" + dirPath);
      refreshUsed.setDaemon(true);
      refreshUsed.start();
    }
  }
  
  /**
   * 关闭刷新线程
   */
  public void shutdown() {
    this.shouldRun = false;
    
    if (this.refreshUsed != null) {
      this.refreshUsed.interrupt();
    }
  }
  
  public String toString() {
    return "du -sk " + dirPath +"\n" + used + "\t" + dirPath;
  }

  /**
   * 检测命令
   */
  protected String[] getExecString() {
    return new String[] {"du", "-sk", dirPath};
  }

  /**
   * 解析输出
   */
  protected void parseExecResult(BufferedReader lines) throws IOException {
    String line = lines.readLine();
    if (line == null) {
      throw new IOException("Expecting a line not the end of stream");
    }
    String[] tokens = line.split("\t");
    if (tokens.length == 0) {
      throw new IOException("Illegal du output");
    }
    this.used.set(Long.parseLong(tokens[0]) * 1024);
  }

  public static void main(String[] args) throws Exception {
    String path = ".";
    if (args.length > 0) {
      path = args[0];
    }

    System.out.println(new DU(new File(path), new Configuration()).toString());
  }
}
