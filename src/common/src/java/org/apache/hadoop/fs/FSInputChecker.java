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
import java.io.InputStream;
import java.util.zip.Checksum;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.util.StringUtils;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;

/**
 * FSInputChecker是一个抽象类，继承了{@link FSInputStream}，
 * 当<code>verifyChecksum</code>设置为true时，所用从该类读取
 * 的字节都是经过校验和校验的。
 */
@InterfaceAudience.LimitedPrivate({"HDFS"})
@InterfaceStability.Unstable
abstract public class FSInputChecker extends FSInputStream {
  public static final Log LOG
  = LogFactory.getLog(FSInputChecker.class);

  /**
   * 保存被读取对象的文件路径。
   */
  protected Path file;
  /**
   * 提供校验和方法的类
   */
  private Checksum sum;
  /**
   * 标志是否需要在读取文件时进行校验和
   */
  private boolean verifyChecksum = true;
  /**
   * 每次读取的Chunk的字节数
   */
  private int maxChunkSize; // data bytes for checksum (eg 512)
  /**
   * 缓存从文件中已经读取的内容
   */
  private byte[] buf; // buffer for non-chunk-aligned reading
  /**
   * 缓存从文件中已经读取的校验和
   */
  private byte[] checksum;
  private IntBuffer checksumInts; // wrapper on checksum buffer
  /**
   * buf中目前的读取位置
   */
  private int pos; // the position of the reader inside buf
  /**
   * 目前在buf中的字节数
   */
  private int count; // the number of bytes currently in buf
  /**
   * 校验和失败之后的重试次数
   */
  private int numOfRetries;

  /**
   * 保存了被读取文件的目前读取位置，应该是<code>maxChunkSize</code>
   * 的整数倍
   */
  // cached file position
  // this should always be a multiple of maxChunkSize
  private long chunkPos = 0;

  /**
   * 每次可以被读取到用户buffer的最大chunk数量，也就是说每次可以被读取到
   * 用户buffer的字节数最大为<code>CHUNKS_PER_READ * maxChunkSize</code>。
   * 此处值为实际测试所获取的值，更大的值也不会降低CPU使用量。
   */
  // Number of checksum chunks that can be read at once into a user
  // buffer. Chosen by benchmarks - higher values do not reduce
  // CPU usage. The size of the data reads made to the underlying stream
  // will be CHUNKS_PER_READ * maxChunkSize.
  private static final int CHUNKS_PER_READ = 32;
  /**
   * 数据校验和默认为32bit
   */
  protected static final int CHECKSUM_SIZE = 4; // 32-bit checksum

  /**
   * 构造函数
   * @param file 要读取的目标文件
   * @param numOfRetries 当校验和失败是需要的重试次数
   */
  protected FSInputChecker(Path file, int numOfRetries) {
    this.file = file;
    this.numOfRetries = numOfRetries;
  }

  /**
   * 构造函数
   * @param file 要读取的目标文件
   * @param numOfRetries 当校验和失败是需要的重试次数
   * @param sum 提供校验和方法的类
   * @param chunkSize 指定Chunk的字节数
   * @param checksumSize 指定Checksum值的大小
   */
  protected FSInputChecker( Path file, int numOfRetries,
      boolean verifyChecksum, Checksum sum, int chunkSize, int checksumSize ) {
    this(file, numOfRetries);
    set(verifyChecksum, sum, chunkSize, checksumSize);
  }

  /**
   * 从文件中读取数据和校验和，将数据放入buf，将校验和放入checksum。
   * 如果len不是ChunkSize的整数倍，那么将会读取少于len个字节，以保证
   * 读取的数据量为ChunkSize的整数倍。
   * 如果校验和功能被禁用，则在checksum位置传入null，并且在读取时不会
   * 因为len不是ChunkSize的整数倍而导致读取字节数小于len。
   *
   * @param pos chunk在文件中的位置
   * @param buf
   * @param offset offset in buf at which to store data
   * @param len maximum number of bytes to read
   * @param checksum the data buffer into which to write checksums
   * @return number of bytes read
   */
  abstract protected int readChunk(long pos, byte[] buf, int offset, int len,
      byte[] checksum) throws IOException;

  /** Return position of beginning of chunk containing pos.
   *
   * @param pos a postion in the file
   * @return the starting position of the chunk which contains the byte
   */
  abstract protected long getChunkPosition(long pos);

  /** Return true if there is a need for checksum verification */
  protected synchronized boolean needChecksum() {
    return verifyChecksum && sum != null;
  }

  /**
   * 从当前缓冲区读取一个字节
   */

  public synchronized int read() throws IOException {
    if (pos >= count) {
      fill();
      if (pos >= count) {
        return -1;
      }
    }
    return buf[pos++] & 0xff;
  }

  /**
   * Read checksum verified bytes from this byte-input stream into
   * the specified byte array, starting at the given offset.
   * 从缓冲区
   * <p> This method implements the general contract of the corresponding
   * <code>{@link InputStream#read(byte[], int, int) read}</code> method of
   * the <code>{@link InputStream}</code> class.  As an additional
   * convenience, it attempts to read as many bytes as possible by repeatedly
   * invoking the <code>read</code> method of the underlying stream.  This
   * iterated <code>read</code> continues until one of the following
   * conditions becomes true: <ul>
   *
   *   <li> The specified number of bytes have been read,
   *
   *   <li> The <code>read</code> method of the underlying stream returns
   *   <code>-1</code>, indicating end-of-file.
   *
   * </ul> If the first <code>read</code> on the underlying stream returns
   * <code>-1</code> to indicate end-of-file then this method returns
   * <code>-1</code>.  Otherwise this method returns the number of bytes
   * actually read.
   *
   * @param      b     destination buffer.
   * @param      off   offset at which to start storing bytes.
   * @param      len   maximum number of bytes to read.
   * @return     the number of bytes read, or <code>-1</code> if the end of
   *             the stream has been reached.
   * @exception  IOException  if an I/O error occurs.
   *             ChecksumException if any checksum error occurs
   */
  public synchronized int read(byte[] b, int off, int len) throws IOException {
    // parameter check
    if ((off | len | (off + len) | (b.length - (off + len))) < 0) {
      throw new IndexOutOfBoundsException();
    } else if (len == 0) {
      return 0;
    }

    int n = 0;
    for (;;) {
      int nread = read1(b, off + n, len - n);
      if (nread <= 0)
        return (n == 0) ? nread : n;
      n += nread;
      if (n >= len)
        return n;
    }
  }

  /**
   * Fills the buffer with a chunk data.
   * No mark is supported.
   * This method assumes that all data in the buffer has already been read in,
   * hence pos > count.
   */
  private void fill(  ) throws IOException {
    assert(pos>=count);
    // fill internal buffer
    count = readChecksumChunk(buf, 0, maxChunkSize);
    if (count < 0) count = 0;
  }

  /**
   * Read characters into a portion of an array, reading from the underlying
   * stream at most once if necessary.
   */
  private int read1(byte b[], int off, int len)
  throws IOException {
    int avail = count-pos;
    if( avail <= 0 ) {
      if(len >= maxChunkSize) {
        // read a chunk to user buffer directly; avoid one copy
        int nread = readChecksumChunk(b, off, len);
        return nread;
      } else {
        // read a chunk into the local buffer
         fill();
        if( count <= 0 ) {
          return -1;
        } else {
          avail = count;
        }
      }
    }

    // copy content of the local buffer to the user buffer
    int cnt = (avail < len) ? avail : len;
    System.arraycopy(buf, pos, b, off, cnt);
    pos += cnt;
    return cnt;
  }

  /** Read up one or more checksum chunk to array <i>b</i> at pos <i>off</i>
   * It requires at least one checksum chunk boundary
   * in between <cur_pos, cur_pos+len>
   * and it stops reading at the last boundary or at the end of the stream;
   * Otherwise an IllegalArgumentException is thrown.
   * This makes sure that all data read are checksum verified.
   *
   * 读取一个或多个经过校验和检查的Chunk到b指定的字节数组的偏移量offset处
   *
   *
   * @param b   读取数据将要放入的字节数组
   * @param off 数据放入字节数组的偏移量
   * @param len 要读取的字节数
   * @return    返回实际读取的字节数，如果碰到EOF则返回-1
   * @throws IOException 如果有IO错误发生.
   */
  private int readChecksumChunk(byte b[], final int off, final int len)
  throws IOException {
    // invalidate buffer
    count = pos = 0;

    int read = 0;
    boolean retry = true;
    int retriesLeft = numOfRetries;
    do {
      retriesLeft--;

      try {
        read = readChunk(chunkPos, b, off, len, checksum);
        if( read > 0) {
          if( needChecksum() ) {
            verifySums(b, off, read);
          }
          chunkPos += read;
        }
        retry = false;
      } catch (ChecksumException ce) {
          LOG.info("Found checksum error: b[" + off + ", " + (off+read) + "]="
              + StringUtils.byteToHexString(b, off, off + read), ce);
          if (retriesLeft == 0) {
            throw ce;
          }

          // try a new replica
          if (seekToNewSource(chunkPos)) {
            // Since at least one of the sources is different,
            // the read might succeed, so we'll retry.
            seek(chunkPos);
          } else {
            // Neither the data stream nor the checksum stream are being read
            // from different sources, meaning we'll still get a checksum error
            // if we try to do the read again.  We throw an exception instead.
            throw ce;
          }
        }
    } while (retry);
    return read;
  }

  private void verifySums(final byte b[], final int off, int read)
    throws ChecksumException
  {
    int leftToVerify = read;
    int verifyOff = 0;
    checksumInts.rewind();
    checksumInts.limit((read - 1)/maxChunkSize + 1);

    while (leftToVerify > 0) {
      sum.update(b, off + verifyOff, Math.min(leftToVerify, maxChunkSize));
      int expected = checksumInts.get();
      int calculated = (int)sum.getValue();
      sum.reset();

      if (expected != calculated) {
        long errPos = chunkPos + verifyOff;
        throw new ChecksumException(
          "Checksum error: "+file+" at "+ errPos +
          " exp: " + expected + " got: " + calculated, errPos);
      }
      leftToVerify -= maxChunkSize;
      verifyOff += maxChunkSize;
    }
  }

  /**
   * 将一个checksum转换为一个long数字
   */
  @Deprecated
  static public long checksum2long(byte[] checksum) {
    long crc = 0L;
    for(int i=0; i<checksum.length; i++) {
      crc |= (0xffL&(long)checksum[i])<<((checksum.length-i-1)*8);
    }
    return crc;
  }

  /**
   * 返回当前文件的读取位置
   */
  @Override
  public synchronized long getPos() throws IOException {
    return chunkPos-Math.max(0L, count - pos);
  }

  /**
   * 返回当前缓冲区中尚未读取的字节。
   */
  @Override
  public synchronized int available() throws IOException {
    return Math.max(0, count - pos);
  }

  /**
   * 在输入流中跳过并忽略n个字节，如果n是负数，则不会跳过任何字节。
   *
   * @param      n   the number of bytes to be skipped.
   * @return     the actual number of bytes skipped.
   * @exception  IOException  if an I/O error occurs.
   *             ChecksumException if the chunk to skip to is corrupted
   */
  public synchronized long skip(long n) throws IOException {
    if (n <= 0) {
      return 0;
    }

    seek(getPos()+n);
    return n;
  }

  /**
   * 移动读取位置到pos，下一次read将会从pos位置读取。
   *
   * @param      pos   要移动到的指定位置
   * @exception IOException
   */

  public synchronized void seek(long pos) throws IOException {
    if( pos<0 ) {
      return;
    }
    // optimize: check if the pos is in the buffer
    long start = chunkPos - this.count;
    if( pos>=start && pos<chunkPos) {
      this.pos = (int)(pos-start);
      return;
    }

    // reset the current state
    resetState();

    // seek to a checksum boundary
    chunkPos = getChunkPosition(pos);

    // scan to the desired position
    int delta = (int)(pos - chunkPos);
    if( delta > 0) {
      readFully(this, new byte[delta], 0, delta);
    }
  }

  /**
   * A utility function that tries to read up to <code>len</code> bytes from
   * <code>stm</code>
   * 一个静态工具类，从<code>stm</code>中读取<code>len</code>个字节，放入buf
   * 中的offset位置。
   * @param stm    输入流
   * @param buf    用来存放读取的数据的buf
   * @param offset buf中位置的偏移量
   * @param len    number of bytes to read
   * @return actual number of bytes read
   * @throws IOException if there is any IO error
   */
  protected static int readFully(InputStream stm,
      byte[] buf, int offset, int len) throws IOException {
    int n = 0;
    for (;;) {
      int nread = stm.read(buf, offset + n, len - n);
      if (nread <= 0)
        return (n == 0) ? nread : n;
      n += nread;
      if (n >= len)
        return n;
    }
  }

  /**
   * 设置校验和相关参数
   *
   * @param verifyChecksum 是否需要进行校验和
   * @param sum 提供校验和方法的类
   * @param chunkSize 指定Chunk的字节数
   * @param checksumSize 指定Checksum值的大小
   */
  final protected synchronized void set(boolean verifyChecksum,
      Checksum sum, int maxChunkSize, int checksumSize) {

    // The code makes assumptions that checksums are always 32-bit.
    assert !verifyChecksum || sum == null || checksumSize == CHECKSUM_SIZE;

    this.maxChunkSize = maxChunkSize;
    this.verifyChecksum = verifyChecksum;
    this.sum = sum;
    this.buf = new byte[maxChunkSize];
    // The size of the checksum array here determines how much we can
    // read in a single call to readChunk
    this.checksum = new byte[CHUNKS_PER_READ * checksumSize];
    this.checksumInts = ByteBuffer.wrap(checksum).asIntBuffer();
    this.count = 0;
    this.pos = 0;
  }

  /**
   * 不支持mark
   */
  final public boolean markSupported() {
    return false;
  }
  /**
   * 不支持mark
   */
  final public void mark(int readlimit) {
  }
  /**
   * 不支持reset
   */
  final public void reset() throws IOException {
    throw new IOException("mark/reset not supported");
  }

  /**
   * 重置FSInputChecker的state
   */
  /* reset this FSInputChecker's state */
  private void resetState() {
    // invalidate buffer
    count = 0;
    pos = 0;
    // reset Checksum
    if (sum != null) {
      sum.reset();
    }
  }
}
