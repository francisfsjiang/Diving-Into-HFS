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
import java.util.Arrays;
import java.util.zip.CRC32;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.PureJavaCrc32;
import org.apache.hadoop.util.StringUtils;

/****************************************************************************** 
  * HDFS 会对写入的所有数据计算校验和(checksum)，并在读取数据时验证校验和。
  * 针对指定字节的数目计算校验和。字节数默认是512 字节，可以通过io.bytes.per.checksum属性设置。
  * 通过CRC-32编码后为4字节。   
  * Datanode 在保存数据前负责验证checksum。
  * client 会把数据和校验和一起发送到一个由多个datanode 组成的队列中，
  * 最后一个Datanode 负责验证checksum。
  * 如果验证失败，会抛出一个ChecksumException。客户端需要处理这种异常。   
  * 客户端从datanode读取数据时，也会验证checksum。
  * 每个Datanode 都保存了一个验证checksum的日志。
  * 每次客户端成功验证一个数据块后，都会告知datanode，datanode会更新日志。
  * 每个datanode 也会在一个后台线程中运行一个DataBlockScanner，
  * 定期验证这个 datanode 上的所有数据块。   
  * 在用Hadoop fs get命令读取文件时，可以用-ignoreCrc忽略验证。
  * 如果是通过FileSystem API 读取时，可以通过setVerifyChecksum(false)，忽略验证。 
  **********************************************************************************/


/****************************************************************
 * 抽象检验文件系统 从文件系统过滤器类继承
 * 提供一个基本的文件校验系统的实现
 * 在客户端生成并且检验检验和，检验数据的完整性
 * 每个512byte的数据，生成一个4byte的检验和
 * 冗余备份的情况下，多个节点储存，以防止校验和本身损坏
 *****************************************************************/
@InterfaceAudience.Public
@InterfaceStability.Stable
public abstract class ChecksumFileSystem extends FilterFileSystem {
  private static final byte[] CHECKSUM_VERSION = new byte[] {'c', 'r', 'c', 0};
  private int bytesPerChecksum = 512;
  private boolean verifyChecksum = true;

  public static double getApproxChkSumLength(long size) {
    return ChecksumFSOutputSummer.CHKSUM_AS_FRACTION * size;
  }
  
  public ChecksumFileSystem(FileSystem fs) {
    super(fs);
  }

  public void setConf(Configuration conf) {
    super.setConf(conf);
    if (conf != null) {
      bytesPerChecksum = conf.getInt(LocalFileSystemConfigKeys.LOCAL_FS_BYTES_PER_CHECKSUM_KEY,
		                     LocalFileSystemConfigKeys.LOCAL_FS_BYTES_PER_CHECKSUM_DEFAULT);
    }
  }
  
  /**
   * 布尔值 设置是否检验了校验和
   */
  public void setVerifyChecksum(boolean verifyChecksum) {
    this.verifyChecksum = verifyChecksum;
  }

  /** 获取初始的文件系统 */
  public FileSystem getRawFileSystem() {
    return fs;
  }

  /** 返回检验和文件关联文件的文件名*/
  public Path getChecksumFile(Path file) {
    return new Path(file.getParent(), "." + file.getName() + ".crc");
  }

  /** 当文件名是校验和文件名时，返回真值*/
  public static boolean isChecksumFile(Path file) {
    String name = file.getName();
    return name.startsWith(".") && name.endsWith(".crc");
  }

  /** 返回校验和文件的长度和源文件的大小
   **/
  public long getChecksumFileLength(Path file, long fileSize) {
    return getChecksumLength(fileSize, getBytesPerSum());
  }

  /** 返回每个校验和的byte数*/
  public int getBytesPerSum() {
    return bytesPerChecksum;
  }

  private int getSumBufferSize(int bytesPerSum, int bufferSize) {
    int defaultBufferSize = getConf().getInt(
                       LocalFileSystemConfigKeys.LOCAL_FS_STREAM_BUFFER_SIZE_KEY,
                       LocalFileSystemConfigKeys.LOCAL_FS_STREAM_BUFFER_SIZE_DEFAULT);
    int proportionalBufferSize = bufferSize / bytesPerSum;
    return Math.max(bytesPerSum,
                    Math.max(proportionalBufferSize, defaultBufferSize));
  }

  /*******************************************************
   * open()方法的FS输入流
   * 确认数据和检验和是否匹配
   *******************************************************/
  private static class ChecksumFSInputChecker extends FSInputChecker {
    public static final Log LOG 
      = LogFactory.getLog(FSInputChecker.class);
    
    private ChecksumFileSystem fs;
    private FSDataInputStream datas;
    private FSDataInputStream sums;
    
    private static final int HEADER_LENGTH = 8;
    
    private int bytesPerSum = 1;
    private long fileLen = -1L;
    
    public ChecksumFSInputChecker(ChecksumFileSystem fs, Path file)
      throws IOException {
      this(fs, file, fs.getConf().getInt(
                       LocalFileSystemConfigKeys.LOCAL_FS_STREAM_BUFFER_SIZE_KEY, 
                       LocalFileSystemConfigKeys.LOCAL_FS_STREAM_BUFFER_SIZE_DEFAULT));
    }
    
    public ChecksumFSInputChecker(ChecksumFileSystem fs, Path file, int bufferSize)
      throws IOException {
      super( file, fs.getFileStatus(file).getReplication() );
      this.datas = fs.getRawFileSystem().open(file, bufferSize);
      this.fs = fs;
      Path sumFile = fs.getChecksumFile(file);
      try {
        int sumBufferSize = fs.getSumBufferSize(fs.getBytesPerSum(), bufferSize);
        sums = fs.getRawFileSystem().open(sumFile, sumBufferSize);

        byte[] version = new byte[CHECKSUM_VERSION.length];
        sums.readFully(version);
        if (!Arrays.equals(version, CHECKSUM_VERSION))
          throw new IOException("Not a checksum file: "+sumFile);
        this.bytesPerSum = sums.readInt();
        set(fs.verifyChecksum, new PureJavaCrc32(), bytesPerSum, 4);
      } catch (FileNotFoundException e) {         // 无提示忽略
        set(fs.verifyChecksum, null, 1, 0);
      } catch (IOException e) {                   // 有提示忽略
        LOG.warn("Problem opening checksum file: "+ file + 
                 ".  Ignoring exception: " + 
                 StringUtils.stringifyException(e));
        set(fs.verifyChecksum, null, 1, 0);
      }
    }
    
    private long getChecksumFilePos( long dataPos ) {
      return HEADER_LENGTH + 4*(dataPos/bytesPerSum);
    }
    
    protected long getChunkPosition( long dataPos ) {
      return dataPos/bytesPerSum*bytesPerSum;
    }
    
    public int available() throws IOException {
      return datas.available() + super.available();
    }
    
    public int read(long position, byte[] b, int off, int len)
      throws IOException {
      // 参数校验
      if ((off | len | (off + len) | (b.length - (off + len))) < 0) {
        throw new IndexOutOfBoundsException();
      } else if (len == 0) {
        return 0;
      }
      if( position<0 ) {
        throw new IllegalArgumentException(
            "Parameter position can not to be negative");
      }

      ChecksumFSInputChecker checker = new ChecksumFSInputChecker(fs, file);
      checker.seek(position);
      int nread = checker.read(b, off, len);
      checker.close();
      return nread;
    }
    
    public void close() throws IOException {
      datas.close();
      if( sums != null ) {
        sums.close();
      }
      set(fs.verifyChecksum, null, 1, 0);
    }
    

    @Override
    public boolean seekToNewSource(long targetPos) throws IOException {
      long sumsPos = getChecksumFilePos(targetPos);
      fs.reportChecksumFailure(file, datas, targetPos, sums, sumsPos);
      boolean newDataSource = datas.seekToNewSource(targetPos);
      return sums.seekToNewSource(sumsPos) || newDataSource;
    }

    @Override
    protected int readChunk(long pos, byte[] buf, int offset, int len,
        byte[] checksum) throws IOException {

      boolean eof = false;
      if (needChecksum()) {
        assert checksum != null; // we have a checksum buffer
        assert checksum.length % CHECKSUM_SIZE == 0; // it is sane length
        assert len >= bytesPerSum; // we must read at least one chunk

        final int checksumsToRead = Math.min(
          len/bytesPerSum, // number of checksums based on len to read
          checksum.length / CHECKSUM_SIZE); // size of checksum buffer
        long checksumPos = getChecksumFilePos(pos); 
        if(checksumPos != sums.getPos()) {
          sums.seek(checksumPos);
        }

        int sumLenRead = sums.read(checksum, 0, CHECKSUM_SIZE * checksumsToRead);
        if (sumLenRead >= 0 && sumLenRead % CHECKSUM_SIZE != 0) {
          throw new ChecksumException(
            "Checksum file not a length multiple of checksum size " +
            "in " + file + " at " + pos + " checksumpos: " + checksumPos +
            " sumLenread: " + sumLenRead,
            pos);
        }
        if (sumLenRead <= 0) { // we're at the end of the file
          eof = true;
        } else {
          // 根据读入的校验和块的数量调整读入数据的大小
          len = Math.min(len, bytesPerSum * (sumLenRead / CHECKSUM_SIZE));
        }
      }
      if(pos != datas.getPos()) {
        datas.seek(pos);
      }
      int nread = readFully(datas, buf, offset, len);
      if (eof && nread > 0) {
        throw new ChecksumException("Checksum error: "+file+" at "+pos, pos);
      }
      return nread;
    }
    
    /* 返回文件长度*/
    private long getFileLength() throws IOException {
      if( fileLen==-1L ) {
        fileLen = fs.getContentSummary(file).getLength();
      }
      return fileLen;
    }
    
    /**
     * 忽略或者弃用输入流中n byte的数据
     * 在总计忽略或者弃用n byte的数据前，用Skip方法忽略一些小的bytes
     * 实际忽略的byte数会被返回。如果n是负数，则不忽略任何byte。
     *
     * @param      n   忽略的byte数
     * @return     实际忽略的byte数
     * @exception  发生返回错误时抛出IOException
     *             当跳过的数据块损坏时，抛出ChecksumException 
     */
    public synchronized long skip(long n) throws IOException {
      long curPos = getPos();
      long fileLength = getFileLength();
      if( n+curPos > fileLength ) {
        n = fileLength - curPos;
      }
      return super.skip(n);
    }
    
    /**
     * 在流中查找给定的位置
     * 下一次read()从此位置开始
     * 
     * <p>此方法不允许搜索超过文件末，会抛出IOException
     *
     * @param      pos   查找的位置
     * @exception  发生IO错误或查找超过文件末尾时抛出IOException  
     *             查找的数据块损坏时抛出ChecksumException 
     */

    public synchronized void seek(long pos) throws IOException {
      if(pos>getFileLength()) {
        throw new IOException("Cannot seek after EOF");
      }
      super.seek(pos);
    }

  }

  /**
   * 在指定的路径下开启一个FSData的输入流
   * @param f 需要打开的文件名
   * @param bufferSize buffer需要的空间大小
   */
  @Override
  public FSDataInputStream open(Path f, int bufferSize) throws IOException {
    return new FSDataInputStream(
        new ChecksumFSInputChecker(this, f, bufferSize));
  }

  /** {@inheritDoc} */
  public FSDataOutputStream append(Path f, int bufferSize,
      Progressable progress) throws IOException {
    throw new IOException("Not supported");
  }

  /**
   * 按byte计算校验和文件的长度
   * @param size 按byte计算的数据文件的大小
   * @param bytesPerSum 一个检验和块的byte数
   * @return 检验和文件的byte数t
   */
  public static long getChecksumLength(long size, int bytesPerSum) {
    /**
    *校验和的长度等于the checksum length is equal to size passed divided by bytesPerSum +
    *bytes written in the beginning of the checksum file.  
    */
    return ((size + bytesPerSum - 1) / bytesPerSum) * 4 +
             CHECKSUM_VERSION.length + 4;  
  }

  /** 给校验过的文件提供一个输出流
   * 为数据生成检验和. */
  private static class ChecksumFSOutputSummer extends FSOutputSummer {
    private FSDataOutputStream datas;    
    private FSDataOutputStream sums;
    private static final float CHKSUM_AS_FRACTION = 0.01f;
    
    public ChecksumFSOutputSummer(ChecksumFileSystem fs, 
                          Path file, 
                          boolean overwrite, 
                          short replication,
                          long blockSize,
                          Configuration conf)
      throws IOException {
      this(fs, file, overwrite, 
           conf.getInt(LocalFileSystemConfigKeys.LOCAL_FS_STREAM_BUFFER_SIZE_KEY,
		       LocalFileSystemConfigKeys.LOCAL_FS_STREAM_BUFFER_SIZE_DEFAULT),
           replication, blockSize, null);
    }
    
    public ChecksumFSOutputSummer(ChecksumFileSystem fs, 
                          Path file, 
                          boolean overwrite,
                          int bufferSize,
                          short replication,
                          long blockSize,
                          Progressable progress)
      throws IOException {
      super(new PureJavaCrc32(), fs.getBytesPerSum(), 4);
      int bytesPerSum = fs.getBytesPerSum();
      this.datas = fs.getRawFileSystem().create(file, overwrite, bufferSize, 
                                         replication, blockSize, progress);
      int sumBufferSize = fs.getSumBufferSize(bytesPerSum, bufferSize);
      this.sums = fs.getRawFileSystem().create(fs.getChecksumFile(file), true, 
                                               sumBufferSize, replication,
                                               blockSize);
      sums.write(CHECKSUM_VERSION, 0, CHECKSUM_VERSION.length);
      sums.writeInt(bytesPerSum);
    }
    
    public void close() throws IOException {
      flushBuffer();
      sums.close();
      datas.close();
    }
    
    @Override
    protected void writeChunk(byte[] b, int offset, int len, byte[] checksum)
    throws IOException {
      datas.write(b, offset, len);
      sums.write(checksum);
    }
  }

  /** {@inheritDoc} */
  @Override
  public FSDataOutputStream create(Path f, FsPermission permission,
      boolean overwrite, int bufferSize, short replication, long blockSize,
      Progressable progress) throws IOException {
    Path parent = f.getParent();
    if (parent != null && !mkdirs(parent)) {
      throw new IOException("Mkdirs failed to create " + parent);
    }
    final FSDataOutputStream out = new FSDataOutputStream(
        new ChecksumFSOutputSummer(this, f, overwrite, bufferSize, replication,
            blockSize, progress), null);
    if (permission != null) {
      setPermission(f, permission);
    }
    return out;
  }

  /**
   * Set replication for an existing file.
   * Implement the abstract <tt>setReplication</tt> of <tt>FileSystem</tt>
   * @param src file name
   * @param replication new replication
   * @throws IOException
   * @return true if successful;
   *         false if file does not exist or is a directory
   */
  public boolean setReplication(Path src, short replication) throws IOException {
    boolean value = fs.setReplication(src, replication);
    if (!value)
      return false;

    Path checkFile = getChecksumFile(src);
    if (exists(checkFile))
      fs.setReplication(checkFile, replication);

    return true;
  }

  /**
   * 重命名 files/dirs
   */
  public boolean rename(Path src, Path dst) throws IOException {
    if (fs.isDirectory(src)) {
      return fs.rename(src, dst);
    } else {

      boolean value = fs.rename(src, dst);
      if (!value)
        return false;

      Path checkFile = getChecksumFile(src);
      if (fs.exists(checkFile)) { //try to rename checksum
        if (fs.isDirectory(dst)) {
          value = fs.rename(checkFile, dst);
        } else {
          value = fs.rename(checkFile, getChecksumFile(dst));
        }
      }

      return value;
    }
  }

  /**
   * 在校验和文件系统中执行delete(Path, boolean)
   */
  public boolean delete(Path f, boolean recursive) throws IOException{
    FileStatus fstatus = null;
    try {
      fstatus = fs.getFileStatus(f);
    } catch(FileNotFoundException e) {
      return false;
    }
    if (fstatus.isDirectory()) {
      //this works since the crcs are in the same
      //directories and the files. so we just delete
      //everything in the underlying filesystem
      return fs.delete(f, recursive);
    } else {
      Path checkFile = getChecksumFile(f);
      if (fs.exists(checkFile)) {
        fs.delete(checkFile, true);
      }
      return fs.delete(f, true);
    }
  }
    
  final private static PathFilter DEFAULT_FILTER = new PathFilter() {
    public boolean accept(Path file) {
      return !isChecksumFile(file);
    }
  };

  /**
   * List the statuses of the files/directories in the given path if the path is
   * a directory.
   * 
   * @param f
   *          given path
   * @return the statuses of the files/directories in the given patch
   * @throws IOException
   */
  @Override
  public FileStatus[] listStatus(Path f) throws IOException {
    return fs.listStatus(f, DEFAULT_FILTER);
  }
  
  @Override
  public boolean mkdirs(Path f) throws IOException {
    return fs.mkdirs(f);
  }

  @Override
  public void copyFromLocalFile(boolean delSrc, Path src, Path dst)
    throws IOException {
    Configuration conf = getConf();
    FileUtil.copy(getLocal(conf), src, this, dst, delSrc, conf);
  }

  /**
   * The src file is under FS, and the dst is on the local disk.
   * Copy it from FS control to the local dst name.
   */
  @Override
  public void copyToLocalFile(boolean delSrc, Path src, Path dst)
    throws IOException {
    Configuration conf = getConf();
    FileUtil.copy(this, src, getLocal(conf), dst, delSrc, conf);
  }

  /**
   * The src file is under FS, and the dst is on the local disk.
   * Copy it from FS control to the local dst name.
   * If src and dst are directories, the copyCrc parameter
   * determines whether to copy CRC files.
   */
  public void copyToLocalFile(Path src, Path dst, boolean copyCrc)
    throws IOException {
    if (!fs.isDirectory(src)) { // source is a file
      fs.copyToLocalFile(src, dst);
      FileSystem localFs = getLocal(getConf()).getRawFileSystem();
      if (localFs.isDirectory(dst)) {
        dst = new Path(dst, src.getName());
      }
      dst = getChecksumFile(dst);
      if (localFs.exists(dst)) { //remove old local checksum file
        localFs.delete(dst, true);
      }
      Path checksumFile = getChecksumFile(src);
      if (copyCrc && fs.exists(checksumFile)) { //copy checksum file
        fs.copyToLocalFile(checksumFile, dst);
      }
    } else {
      FileStatus[] srcs = listStatus(src);
      for (FileStatus srcFile : srcs) {
        copyToLocalFile(srcFile.getPath(), 
                        new Path(dst, srcFile.getPath().getName()), copyCrc);
      }
    }
  }

  @Override
  public Path startLocalOutput(Path fsOutputFile, Path tmpLocalFile)
    throws IOException {
    return tmpLocalFile;
  }

  @Override
  public void completeLocalOutput(Path fsOutputFile, Path tmpLocalFile)
    throws IOException {
    moveFromLocalFile(tmpLocalFile, fsOutputFile);
  }

  /**
   * 给文件系统报告一个校验和的错误
   * @param f 包含错误的文件名
   * @param in 打开的文件的流
   * @param inPos 文件中坏数据的起始位置
   * @param sums 打开的校验文件流the stream open on the checksum file
   * @param sumsPos the position of the beginning of the bad data in the checksum file
   * @return 是否有必要重试 bool类型判断
   */
  public boolean reportChecksumFailure(Path f, FSDataInputStream in,
                                       long inPos, FSDataInputStream sums, long sumsPos) {
    return false;
  }
}
