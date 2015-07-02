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
 * 这个接口声明了对Client的buffer进行flush的操作，此接口主要被
 * BufferedIOStream实现。
 *
 * 此接口的两个主要方法hflush、hsync的设计目的不同，hsync的目的是在执行
 * 完之后，保证数据
 * 会写在磁盘上，而hflush只会保证对数据源的新Client会读到刚flush的数据。
 *
 * 在现在分析的HDFS版本中，这个接口的hflush和hsync方法一样，hsync会调用
 * hflush。再后来2.x版本中，这两个方法将会有不同的实现。
 *
 *
 *
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface Syncable {

  @Deprecated
  public void sync() throws IOException;
  

  public void hflush() throws IOException;
  

  public void hsync() throws IOException;
}
