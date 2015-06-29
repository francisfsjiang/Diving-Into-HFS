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
package org.apache.hadoop.fs.ftp;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * 这个类封装了{@link Throwable} Throwable对象用于抛出指定异常
 * FTPException继承自RuntimeException,final修饰了静态变量serialVersionUID = 1L
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class FTPException extends RuntimeException {

  private static final long serialVersionUID = 1L;
  /**
   * @param message 异常消息
   * 通过传入message,调用父类方法抛出异常
   */
  public FTPException(String message) {
    super(message);
  }
 /**
   * @param Throwable t 异常
   * 通过传入t,调用父类方法抛出异常
   */
  public FTPException(Throwable t) {
    super(t);
  }
 /**
   * @param Throwable t 异常
   * @param String message 异常消息
   * 通过传入t,调用父类方法抛出异常
   */
  public FTPException(String message, Throwable t) {
    super(message, t);
  }
}
