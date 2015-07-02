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
 * FSInputChecker是一个抽象类，继承了@link FSInputStream，
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
  private int maxChunkSize; 
  
  /**
   * 缓存从文件中已经读取的内容
   */
  private byte[] buf; 
  /**
   * 缓存从文件中已经读取的校验和
   */
  private byte[] checksum;
  private IntBuffer checksumInts; /
  /**
   * buf中目前的读取位置
   */
  private int pos; 
  /**
   * 目前在buf中的字节数
   */
  private int count; 
  /**
   * 校验和失败之后的重试次数
   */
  private int numOfRetries;

  /**
   * 保存了被读取文件的目前读取位置，应该是<code>maxChunkSize</code>
   * 的整数倍
   */
  private long chunkPos = 0;

  /**
   * 每次可以被读取到用户buffer的最大chunk数量，也就是说每次可以被读取到
   * 用户buffer的字节数最大为<code>CHUNKS_PER_READ * maxChunkSize</code>。
   * 此处值为实际测试所获取的值，更大的值也不会降低CPU使用量。
   */
  private static final int CHUNKS_PER_READ = 32;
  /**
   * 数据校验和默认为32bit
   */
  protected static final int CHECKSUM_SIZE = 4; // 32-bit checksum

  /**
   * 构造函数
   */
  protected FSInputChecker(Path file, int numOfRetries) {
    this.file = file;
    this.numOfRetries = numOfRetries;
  }

  /**
   * 构造函数
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
   */
  abstract protected int readChunk(long pos, byte[] buf, int offset, int len,
      byte[] checksum) throws IOException;

  /**
   * 返回pos位置所在的Chunk
   */
  abstract protected long getChunkPosition(long pos);

  /**
   * 返回是否需要校验
   */
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
   * 读取len个字节到字节数组b的偏移量off处，读取的数据都经过校验。
   * 此方法会尝试不断读取，知道读取到len个字节，直到EOF或者IOException
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
   * 向buf中填充一个经过校验的chunk
   */
  private void fill(  ) throws IOException {
    assert(pos>=count);
    count = readChecksumChunk(buf, 0, maxChunkSize);
    if (count < 0) count = 0;
  }

  /**
   * 读取len个字节到字节数组b的偏移量off处，如果len超出了Chunk的大小
   * 则会先向缓冲区中填入一个Chunk，再从buf复制到b中。
   */
  private int read1(byte b[], int off, int len)
  throws IOException {
    int avail = count-pos;
    if( avail <= 0 ) {
      if(len >= maxChunkSize) {
        int nread = readChecksumChunk(b, off, len);
        return nread;
      } else {
         fill();
        if( count <= 0 ) {
          return -1;
        } else {
          avail = count;
        }
      }
    }

    int cnt = (avail < len) ? avail : len;
    System.arraycopy(buf, pos, b, off, cnt);
    pos += cnt;
    return cnt;
  }

  /**
   * 读取一个或多个经过校验和检查的Chunk到b指定的字节数组的偏移量offset处，
   * 这个方法保证所有读取到的数据都是经过校验。
   */
  private int readChecksumChunk(byte b[], final int off, final int len)
  throws IOException {
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

          if (seekToNewSource(chunkPos)) {
            seek(chunkPos);
          } else {
            throw ce;
          }
        }
    } while (retry);
    return read;
  }

  /**
   * 计算字节数组b中的要进行校验的数据的校验和，并与checksum中的，即当前读取的
   * chunk的校验和进行比较
   */
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
   */

  public synchronized void seek(long pos) throws IOException {
    if( pos<0 ) {
      return;
    }
    long start = chunkPos - this.count;
    if( pos>=start && pos<chunkPos) {
      this.pos = (int)(pos-start);
      return;
    }

    resetState();

    chunkPos = getChunkPosition(pos);

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
   */
  final protected synchronized void set(boolean verifyChecksum,
      Checksum sum, int maxChunkSize, int checksumSize) {

    assert !verifyChecksum || sum == null || checksumSize == CHECKSUM_SIZE;

    this.maxChunkSize = maxChunkSize;
    this.verifyChecksum = verifyChecksum;
    this.sum = sum;
    this.buf = new byte[maxChunkSize];
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
  private void resetState() {

    count = 0;
    pos = 0;
    if (sum != null) {
      sum.reset();
    }
  }
}
