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

import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileSystem;

/**
  * FTPInputStream继承自FsInputStream类
  * 并重写了InputStream接口中的各种方法
  * @author benco
  */

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class FTPInputStream extends FSInputStream {

  InputStream wrappedStream;
  FTPClient client;
  FileSystem.Statistics stats;
  boolean closed;
  long pos;
 /**
   * 构造函数接收一个InputStream对象作为第一参数
   * 并且判断,当InputStream对象为null的时候则throw IllegalArgumentException("Null InputStream")
   * 接收FTPClient对象作为第二参数
   * 当FTPClient对象为null或者FTPClient对象连接没有建立的时候
   * 则throw IllegalArgumentException("FTP client null or not connected")
   * @param stream InputStream对象
   * @param client FTPClient对象
   * @param stats FileSystem.Statistics对象
   */
  public FTPInputStream(InputStream stream, FTPClient client,
      FileSystem.Statistics stats) {
    if (stream == null) {
      throw new IllegalArgumentException("Null InputStream");
    }
    if (client == null || !client.isConnected()) {
      throw new IllegalArgumentException("FTP client null or not connected");
    }
    this.wrappedStream = stream;
    this.client = client;
    this.stats = stats;
    this.pos = 0;
    this.closed = false;
  }
 /**
   * @return long pos 返回值是一个记录了多少个字节的数量
   */
  public long getPos() throws IOException {
    return pos;
  }
 /**
   * @throw FTPInputStream不支持定点寻找内容
   */
  // We don't support seek.
  public void seek(long pos) throws IOException {
    throw new IOException("Seek not supported");
  }

  public boolean seekToNewSource(long targetPos) throws IOException {
    throw new IOException("Seek not supported");
  }
 /**
   * 使用线程安全(synchronized)的read,当从wrappedStream.read()读取的字节数大于0的时候,pos值记录读入的字节数增一
   * FileSystem.Statistics类用来保存与一个文件系统相关的统计信息,主要包括从该文件系统读取和写入的总的字节数
   * 当FileSystem.Statistics对象非空并且读入字节数不为零时,更新统计信息
   * @throw IOException("Stream closed") 当InputStream对象被关闭时候抛出异常
   * @return 返回读取的总字节数
   */
  public synchronized int read() throws IOException {
    if (closed) {
      throw new IOException("Stream closed");
    }

    int byteRead = wrappedStream.read();
    if (byteRead >= 0) {
      pos++;
    }
    if (stats != null & byteRead >= 0) {
      stats.incrementBytesRead(1);
    }
    return byteRead;
  }
 /**
   * 使用线程安全(synchronized)的read,当从wrappedStream.read()读取的字节数大于0的时候,pos值记录读入的总字节数值
   * FileSystem.Statistics类用来保存与一个文件系统相关的统计信息,主要包括从该文件系统读取和写入的总的字节数
   * 当FileSystem.Statistics对象非空并且读入字节数不为零时,更新统计信息
   * @param buf[] 存放result个总字节的byte数组
   * @param off 
   * @param len
   * @throw IOException("Stream closed") 当InputStream对象被关闭时候抛出异常
   * @return 返回读取的总字节数
   */
  public synchronized int read(byte buf[], int off, int len) throws IOException {
    if (closed) {
      throw new IOException("Stream closed");
    }

    int result = wrappedStream.read(buf, off, len);
    if (result > 0) {
      pos += result;
    }
    if (stats != null & result > 0) {
      stats.incrementBytesRead(result);
    }

    return result;
  }
 /**
   * 使用线程安全(synchronized)的close方法,调用超类的方法关闭InputStream流,closed赋值为True
   * 若FTPClient对象没有连接,抛出FTPException("Client not connected")
   * 然后关闭FTPClient连接
   * @param len
   * @throw IOException("Stream closed") 当InputStream对象被关闭时候抛出异常
   */
  public synchronized void close() throws IOException {
    if (closed) {
      throw new IOException("Stream closed");
    }
    super.close();
    closed = true;
    if (!client.isConnected()) {
      throw new FTPException("Client not connected");
    }

    boolean cmdCompleted = client.completePendingCommand();
    client.logout();
    client.disconnect();
    if (!cmdCompleted) {
      throw new FTPException("Could not complete transfer, Reply Code - "
          + client.getReplyCode());
    }
  }

  // 不支持这些方法

  public boolean markSupported() {
    return false;
  }

  public void mark(int readLimit) {
    // Do nothing
  }

  public void reset() throws IOException {
    throw new IOException("Mark not supported");
  }
}
