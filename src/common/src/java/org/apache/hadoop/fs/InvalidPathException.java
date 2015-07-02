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

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * InvalidPathException类继承自HadoopIllegalArgumentException类,
 * 拥有一个静态的私有常量 serialVersionUID = 1L,
 * 文件目录路径字符串若是无效的可能因为其无效的特征值或者其他文件系统的特定原因.
 * @author benco
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class InvalidPathException extends HadoopIllegalArgumentException {
  private static final long serialVersionUID = 1L;

  /**
   * InvalidPathException第一个构造函数接收一个参数,
   * String path是具有特定细节的信息
   * @param path invalid path.
   */
  public InvalidPathException(final String path) {
    super("Invalid path name " + path);
  }

  /**
   * InvalidPathException第二个构造函数接收两个参数,
   * String path是具有特定细节的信息,
   * String reason是具有解释path是非法的原因的信息.
   * @param path invalid path.
   * @param reason Reason <code>path</code> is invalid
   */
  public InvalidPathException(final String path, final String reason) {
    super("Invalid path " + path
        + (reason == null ? "" : ". (" + reason + ")"));
  }
}
