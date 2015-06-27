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
package org.apache.hadoop.fs.shell;

import java.io.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ipc.RemoteException;

/**
 * 用于执行CLI命令的抽象类
 */
abstract public class Command extends Configured {
  protected String[] args;
  
  /**
   * 构造函数
   */
  protected Command(Configuration conf) {
    super(conf);
  }
  
  /**
   * 返回命令名
   */
  abstract public String getCommandName();
  
  /**
   * 在指定路径下执行命令
   * 
   * @param path 执行路径
   * @throws IOException
   */
  abstract protected void run(Path path) throws IOException;
  
  /** 
   * 在所有args给定的路径下执行命令
   * 
   * @return 返回 0 表示全部成功，返回 -1 表示出现失败
   */
  public int runAll() {
    int exitCode = 0;
    for (String src : args) {
      try {
        Path srcPath = new Path(src);
        FileSystem fs = srcPath.getFileSystem(getConf());
        FileStatus[] statuses = fs.globStatus(srcPath);
        if (statuses == null) {
          System.err.println("Can not find listing for " + src);
          exitCode = -1;
        } else {
          for(FileStatus s : statuses) {
            run(s.getPath());
          }
        }
      } catch (RemoteException re) {
        exitCode = -1;
        String content = re.getLocalizedMessage();
        int eol = content.indexOf('\n');
        if (eol>=0) {
          content = content.substring(0, eol);
        }
        System.err.println(getCommandName() + ": " + content);
      } catch (IOException e) {
        exitCode = -1;
        System.err.println(getCommandName() + ": " + e.getLocalizedMessage());
      }
    }
    return exitCode;
  }
}
