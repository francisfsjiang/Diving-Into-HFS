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

/****************************************************************
 *
 * <code>CreateFlag</code>表示了创建文件的语法，用户可以组合多种创建
 * 方式，比如将CREATA和APPEND组合起来，那么则会创建不存在的文件并开始向
 * 文件末尾添加内容，或者向
 * 现有文件末尾添加。
 *
 * 创建好之后可以作为参数传递给{@link FileSystem#create}
 * 方法，来实现文件的创建。
 *
 *****************************************************************/
@InterfaceAudience.Public
@InterfaceStability.Stable
public enum CreateFlag {

  /**
   * 未创建时创建文件，已创建时抛出IOException
   */
  CREATE((short) 0x01),

  /**
   * 如果文件不存在，则创建文件，如果存在，则复写
   */
  OVERWRITE((short) 0x02),

  /**
   * 向文件末尾添加内容，如果文件内容不存在，则抛出<code>IOException</code>
   */
  APPEND((short) 0x04);

  private short mode;

  private CreateFlag(short mode) {
    this.mode = mode;
  }

  short getMode() {
    return mode;
  }
}