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
 * 随机文件读写(RAF)的能力。
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
   * 此方法在FTP、S3、Local
   */
  public abstract boolean seekToNewSource(long targetPos) throws IOException;

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

  public void readFully(long position, byte[] buffer)
    throws IOException {
    readFully(position, buffer, 0, buffer.length);
  }
}
