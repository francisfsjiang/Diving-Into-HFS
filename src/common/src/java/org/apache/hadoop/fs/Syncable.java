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

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/** 
  * 这个接口声明了刷新缓冲区以及同步装饰器的操作
  */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface Syncable {
  /**
   * @deprecated 装饰器,被hflush替代
   */
  @Deprecated  public void sync() throws IOException;
  
 /**
   * @throws IOException if any error occurs
   * 刷新并清空FTPClient对象client的user的缓冲区里存着的数据,然后将这个响应返回,新的用户能看到刷新后的数据
   */
  public void hflush() throws IOException;
  
 /**
   * @throws IOException if error occurs
   * 刷新并清空FTPClient对象client的硬盘驱动里存着的数据
   */
  public void hsync() throws IOException;
}
