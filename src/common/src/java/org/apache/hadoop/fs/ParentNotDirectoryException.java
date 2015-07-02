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
 * 当定位一个文件目录的上一层发现不是一个目录时候抛出这个异常,
 * ParentNotDirectoryException类继承自IOException异常类,
 * 有一个静态常量serialVersionUID = 1L,
 * ParentNotDirectoryException()方法和ParentNotDirectoryException(String msg)均调用父类方法
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class ParentNotDirectoryException extends IOException {
  private static final long serialVersionUID = 1L;

  public ParentNotDirectoryException() {
    super();
  }

  public ParentNotDirectoryException(String msg) {
    super(msg);
  }
}
