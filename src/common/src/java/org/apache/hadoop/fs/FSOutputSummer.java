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
import java.io.OutputStream;
import java.util.zip.Checksum;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * 此类是一个抽象类，主要功能是在写入流之前进行校验
 *
 * @author neveralso
 */
@InterfaceAudience.LimitedPrivate({"HDFS"})
@InterfaceStability.Unstable
abstract public class FSOutputSummer extends OutputStream {
  // data checksum
  private Checksum sum;
  /**
   * 数据缓冲区
   */
  private byte buf[];
  /**
   * 校验和缓冲区
   */
  private byte checksum[];
  /**
   * 缓冲区内字节数量
   */
  private int count;

  protected FSOutputSummer(Checksum sum, int maxChunkSize, int checksumSize) {
    this.sum = sum;
    this.buf = new byte[maxChunkSize];
    this.checksum = new byte[checksumSize];
    this.count = 0;
  }

  /**
   * 向输出流写入Chunk和其校验和，数据长度为len，位于b字节数组中偏移量为offset的位置。
   *
   * @param b 输出数据所在的字节数组
   * @param offset 数据在字节数组中的偏移量
   * @param len 数据长度
   * @param checksum 数据所对应的校验和
   * @throws IOException
   */
  protected abstract void writeChunk(byte[] b, int offset, int len, byte[] checksum)
  throws IOException;

  /**
   * 向buffer写入一个字节，在当前buffer满时，flush当前buffer
   */
  public synchronized void write(int b) throws IOException {
    sum.update(b);
    buf[count++] = (byte)b;
    if(count == buf.length) {
      flushBuffer();
    }
  }

  /**
   * 写入数据，数据长度为len，位于b字节数组中偏移量为offset的位置，该方法通过多次
   * 写入，保证一定会写入len长度的数据，除非发生{@link IOException}。
   *
   * @param      b     the data.
   * @param      off   the start offset in the data.
   * @param      len   the number of bytes to write.
   * @exception  IOException  if an I/O error occurs.
   */
  public synchronized void write(byte b[], int off, int len)
  throws IOException {
    if (off < 0 || len < 0 || off > b.length - len) {
      throw new ArrayIndexOutOfBoundsException();
    }

    for (int n=0;n<len;n+=write1(b, off+n, len-n)) {
    }
  }

  /**
   * 写入数据，数据长度为len，位于b字节数组中偏移量为offset的位置，如果len大于buf的
   * 长度，即一个Chunk的大小，那么只写入一个Chunk。
   * 如果写入的数据小于一个Chunk，那么写入buf，如果len大于buf的剩余容量，则将len
   * 个字节拷贝到buf中，否则将buf写满，
   * 如果buf写满，则flush。
   */
  private int write1(byte b[], int off, int len) throws IOException {
    if(count==0 && len>=buf.length) {
      // local buffer is empty and user data has one chunk
      // checksum and output data
      final int length = buf.length;
      sum.update(b, off, length);
      writeChecksumChunk(b, off, length, false);
      return length;
    }
    
    // copy user data to local buffer
    int bytesToCopy = buf.length-count;
    bytesToCopy = (len<bytesToCopy) ? len : bytesToCopy;
    sum.update(b, off, bytesToCopy);
    System.arraycopy(b, off, buf, count, bytesToCopy);
    count += bytesToCopy;
    if (count == buf.length) {
      // local buffer is full
      flushBuffer();
    } 
    return bytesToCopy;
  }

  /**
   * flush当前的buf，计算并写入checksum，写完后清空buf
   */
  protected synchronized void flushBuffer() throws IOException {
    flushBuffer(false);
  }

  /**
   * flush当前的buf，计算并写入checksum，如果keep为true，那么在flush任然保持
   * 写入之前的状态，即不清空buf。
   */
  protected synchronized void flushBuffer(boolean keep) throws IOException {
    if (count != 0) {
      int chunkLen = count;
      count = 0;
      writeChecksumChunk(buf, 0, chunkLen, keep);
      if (keep) {
        count = chunkLen;
      }
    }
  }
  
  /**
   * 将buf里的数据Chunk和其对应的校验和写入输出，如果keep为true，则会保持原
   * buf的checksum，而不清空。
   */
  private void writeChecksumChunk(byte b[], int off, int len, boolean keep)
  throws IOException {
    int tempChecksum = (int)sum.getValue();
    if (!keep) {
      sum.reset();
    }
    int2byte(tempChecksum, checksum);
    writeChunk(b, off, len, checksum);
  }

  /**
   * 计算buf中的数据的checksum，返回存放sum的字节数组。
   */
  static public byte[] convertToByteStream(Checksum sum, int checksumSize) {
    return int2byte((int)sum.getValue(), new byte[checksumSize]);
  }

  static byte[] int2byte(int integer, byte[] bytes) {
    bytes[0] = (byte)((integer >>> 24) & 0xFF);
    bytes[1] = (byte)((integer >>> 16) & 0xFF);
    bytes[2] = (byte)((integer >>>  8) & 0xFF);
    bytes[3] = (byte)((integer >>>  0) & 0xFF);
    return bytes;
  }

  /**
   * 重设buf的大小，新的大小为size。
   */
  protected synchronized void resetChecksumChunk(int size) {
    sum.reset();
    this.buf = new byte[size];
    this.count = 0;
  }
}
