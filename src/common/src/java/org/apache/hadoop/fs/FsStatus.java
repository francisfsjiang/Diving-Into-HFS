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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.Writable;

/** 
  * FsStatus是用于展示FileSystem的容量,空闲空间和使用空间的情况{@link FileSystem}.
  * FsStatus有三个私有成员变量,capacity,used,remaining
  * 并且引用Writable接口,实现write(DataOutput out)方法和readFields(DataInput in)方法
  * @author benco
  */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class FsStatus implements Writable {
  private long capacity;
  private long used;
  private long remaining;
/**
  * @param capacity
  * @param used
  * @param remaining
  * FsStatus构造函数,通过传入三个参数,为FsStatus的三个私有成员变量赋值
  */
  public FsStatus(long capacity, long used, long remaining) {
    this.capacity = capacity;
    this.used = used;
    this.remaining = remaining;
  }

  /**  
    * @return capacity
    * 返回文件系统容纳字节数的容量
    */
  public long getCapacity() {
    return capacity;
  }

  /**  
	* @return used
	* 返回文件系统已经使用的字节数所占的空间大小
	*/
  public long getUsed() {
    return used;
  }

   /**  
	* @return remaining
	* 返回文件系统剩下的字节数所占的空间大小
	*/
  public long getRemaining() {
    return remaining;
  }

  //////////////////////////////////////////////////
  // Writable接口的实现
  //////////////////////////////////////////////////
  /**
    * @param DataOutput对象 out
    * @throw IOException
    * 分别将capacity,used,remaining的值写入输出流中,每个值由八个字节组成
    */
  public void write(DataOutput out) throws IOException {
    out.writeLong(capacity);
    out.writeLong(used);
    out.writeLong(remaining);
  }
  /**
    * @param DataInput对象 in
    * @throw IOException
    * 分别从输入流中读取capacity,used,remaining的值,每个值由八个字节组成
    */
  public void readFields(DataInput in) throws IOException {
    capacity = in.readLong();
    used = in.readLong();
    remaining = in.readLong();
  }
}
