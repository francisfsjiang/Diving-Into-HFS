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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.*;

/**
 * 实现该接口的流允许从某个位置开始读的
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface PositionedReadable {
  /**
   * 从文件给定的位置读入指定数量的byte数, 并返回读入的byte数
   *  这不会改变当前文件的偏移量，并且是线程安全的.
   */
  public int read(long position, byte[] buffer, int offset, int length)
    throws IOException;
  
  /**
   * 从文件给定的位置读入指定数量的byte数
   * 这不会改变当前文件的偏移量，并且是线程安全的.
   */
  public void readFully(long position, byte[] buffer, int offset, int length)
    throws IOException;
  
  /**
   * 从给定的位置读入与缓存区长度相同的byte数.
   * 这不会改变当前文件的偏移量，并且是线程安全的.
   */
  public void readFully(long position, byte[] buffer) throws IOException;
}
