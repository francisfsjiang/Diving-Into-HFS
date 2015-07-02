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

import java.io.BufferedInputStream;
import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;


/**
 * BufferedFSInputStream继承了@link BufferedInputStream，
 * 通过缓存来优化其内包装的对象@ling FSInputStream的读取
 *
 * @author neveralso
 */

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class BufferedFSInputStream extends BufferedInputStream
implements Seekable, PositionedReadable {
  /**
   * 创建一个指定buffer大小为size的<code>BufferedFSInputStream</code>
   */
  public BufferedFSInputStream(FSInputStream in, int size) {
    super(in, size);
  }

  public long getPos() throws IOException {
    return ((FSInputStream)in).getPos()-(count-pos);
  }

  public long skip(long n) throws IOException {
    if (n <= 0) {
      return 0;
    }

    seek(getPos()+n);
    return n;
  }

  /**
   * 如果要移动到的目标读取位置在目前的buffer中，那么优化他，直接在
   * buffer内移动读取位置。
   */
  public void seek(long pos) throws IOException {
    if( pos<0 ) {
      return;
    }
    long end = ((FSInputStream)in).getPos();
    long start = end - count;
    if( pos>=start && pos<end) {
      this.pos = (int)(pos-start);
      return;
    }

    this.pos = 0;
    this.count = 0;

    ((FSInputStream)in).seek(pos);
  }

  /**
   * 将输入源切换到一个新的输入源，并且将偏移量移动到<code>pos</code>。
   * 此方法在FTP、S3、Local等文件系统上均无实现（直接返回false），现有
   * 的唯一实现在
   * @link org.apache.hadoop.hdfs.DFSInputStream#seekToNewSource(long targetPos)
   * ，其功能是在当前读取的Block失效时，切换到新的Block，并且移动偏移量，操作成
   * 功时返回true
   */
  public boolean seekToNewSource(long targetPos) throws IOException {
    pos = 0;
    count = 0;
    return ((FSInputStream)in).seekToNewSource(targetPos);
  }

  public int read(long position, byte[] buffer, int offset, int length) throws IOException {
    return ((FSInputStream)in).read(position, buffer, offset, length) ;
  }

  public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
    ((FSInputStream)in).readFully(position, buffer, offset, length);
  }

  public void readFully(long position, byte[] buffer) throws IOException {
    ((FSInputStream)in).readFully(position, buffer);
  }
}
