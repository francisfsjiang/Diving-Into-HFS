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

/****************************************************************
 * FSInputStream is a generic old InputStream with a little bit
 * of RAF-style seek ability.
 *
 * FSInputStream 是一个抽象类，继承了基本的{@link java.io.InputStream}, 并且提供了
 * 随机文件读写(Random Access File)的能力。
 *****************************************************************/
@InterfaceAudience.LimitedPrivate({"HDFS"})
@InterfaceStability.Unstable
public abstract class FSInputStream extends InputStream
    implements Seekable, PositionedReadable {
  /**
   * Seek to the given offset from the start of the file.
   * The next read() will be from that location.  Can't
   * seek past the end of the file.
   *
   * Seek方法将当前偏移量移动到<code>pos</code>，偏移量都是相对
   * 于文件的开头，下一次read()调用将会从<code>pos</code>处开始
   * 读取。该方法不能将偏移量设置到超出文件长度的位置。
   *
   */
  public abstract void seek(long pos) throws IOException;

  /**
   * Return the current offset from the start of the file
   *
   * 返回当前的偏移量，该偏移量相对于文件开始处。
   */
  public abstract long getPos() throws IOException;

  /**
   * Seeks a different copy of the data.  Returns true if
   * found a new source, false otherwise.
   *
   * 将输入源切换到一个新的输入源，并且将偏移量移动到<code>pos</code>。
   * 此方法在FTP、S3、Local等文件系统上均无实现（直接返回false），现有
   * 的唯一实现在
   * {@link org.apache.hadoop.hdfs.DFSInputStream#seekToNewSource(long targetPos)}
   * ，其功能是在当前读取的Block失效时，切换到新的Block，并且移动偏移量，操作成
   * 功时返回true。
   *
   * @param targetPos 目标偏移量
   * @return 成功true or 失败false
   * @throws IOException
   */
  public abstract boolean seekToNewSource(long targetPos) throws IOException;

  /**
   *
   * 该方法包装了{@link java.io.InputStream#read(byte[], int, int)}，
   * 该方法可以从指定的文件偏移量处，往buffer指定的字节数组的offset位置读入length
   * 个字节，并且可以恢复文件偏移量到读取之前的状态。
   *
   * @param position
   * @param buffer
   * @param offset
   * @param length
   * @return  成功写入的字节数
   * @throws IOException
   */
  public int read(long position, byte[] buffer, int offset, int length)
    throws IOException {
    synchronized (this) {
      long oldPos = getPos();
      int nread = -1;
      try {
        seek(position);
        nread = read(buffer, offset, length);
      } finally {
        seek(oldPos);
      }
      return nread;
    }
  }

  /**
   * 该方法与{@link FSInputStream#read(long, byte[], int, int)}类似，
   * 但是该方法会不断地读取，直到读满或者抛出EOF或者出现{@link java.io.IOException}。
   *
   * @param position
   * @param buffer
   * @param offset
   * @param length
   * @throws IOException
   */

  public void readFully(long position, byte[] buffer, int offset, int length)
    throws IOException {
    int nread = 0;
    while (nread < length) {
      int nbytes = read(position+nread, buffer, offset+nread, length-nread);
      if (nbytes < 0) {
        throw new EOFException("End of file reached before reading fully.");
      }
      nread += nbytes;
    }
  }

  /**
   * {@link FSInputStream#readFully(long, byte[], int, int)}的另一个版本，
   * 即从buffer的开始处读取buffer长度个字节。
   * @param position
   * @param buffer
   * @throws IOException
   */
  public void readFully(long position, byte[] buffer)
    throws IOException {
    readFully(position, buffer, 0, buffer.length);
  }
}
