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

import java.util.Arrays;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.Writable;

/** 代表文件校验和的抽象类*/
@InterfaceAudience.Public
@InterfaceStability.Stable
public abstract class FileChecksum implements Writable {
  /** 校验算法名 */ 
  public abstract String getAlgorithmName();

  /** 校验和的byte长度 */ 
  public abstract int getLength();

  /** 校验和的byte值*/ 
  public abstract byte[] getBytes();

  /** 如果算法和byte值都相同，返回真值*/
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    }
    if (other == null || !(other instanceof FileChecksum)) {
      return false;
    }

    final FileChecksum that = (FileChecksum)other;
    return this.getAlgorithmName().equals(that.getAlgorithmName())
      && Arrays.equals(this.getBytes(), that.getBytes());
  }
  
  /** {@inheritDoc} */
  public int hashCode() {
    return getAlgorithmName().hashCode() ^ Arrays.hashCode(getBytes());
  }
}