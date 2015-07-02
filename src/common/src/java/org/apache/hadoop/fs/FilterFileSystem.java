package org.apache.hadoop.fs;

import java.io.*;
import java.net.URI;
import java.util.EnumSet;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

/****************************************************************
 * FilterFileSystem继承了FileSystem类，重写了父类的所有方法 。
 * FilterFileSystem是一个代理, 或者说, wrapper。
 * 类中包含一个FileSystem实例，用来作为基础的文件系统，在其中给
 * 它转换数据或者增加方法，加以封装，相当于起到了过滤文件系统的作用。
 * Filter意为过滤器,FilterFileSystem选择的过滤策略是"直通",
 * 即什么都不做, 直接把参数传递给public方法对应的protected方法.
 * FilterFileSystem包装了一个FileSystem对象，所有提供的方法都是与
 * FileSystem中相同的方法，通过调用FileSystem对象对应的函数来实现，
 * 来为各方法添加更多地功能。
 *
 *****************************************************************/
@InterfaceAudience.Public
@InterfaceStability.Stable
public class FilterFileSystem extends FileSystem {

  protected FileSystem fs;
  public FilterFileSystem() {
  }

  public FilterFileSystem(FileSystem fs) {
    this.fs = fs;
    this.statistics = fs.statistics;
  }

  public void initialize(URI name, Configuration conf) throws IOException {
    fs.initialize(name, conf);
  }

  public URI getUri() {
    return fs.getUri();
  }

  public Path makeQualified(Path path) {
    return fs.makeQualified(path);
  }

  protected void checkPath(Path path) {
    fs.checkPath(path);
  }

  /**
   * 获取文件的块位置信息
   * @param file
   * @param start
   * @param len
   * @return
   * @throws IOException
   */
  public BlockLocation[] getFileBlockLocations(FileStatus file, long start,
    long len) throws IOException {
      return fs.getFileBlockLocations(file, start, len);
  }

  /**
   * 打开一个文件数据的输出流
   * @param f the file name to open
   * @param bufferSize the size of the buffer to be used.
   */
  public FSDataInputStream open(Path f, int bufferSize) throws IOException {
    return fs.open(f, bufferSize);
  }


  /**
   * 进行输出流的追加操作
   */
  public FSDataOutputStream append(Path f, int bufferSize,
      Progressable progress) throws IOException {
    return fs.append(f, bufferSize, progress);
  }
<<<<<<< HEAD

  /** {@inheritDoc} */
  /**
   * 创建一个文件输出流
   * @param f the file name to open
   * @param permission
   * @param overwrite if a file with this name already exists, then if true,
   *   the file will be overwritten, and if false an error will be thrown.
   * @param bufferSize the size of the buffer to be used.
   * @param replication required block replication for the file.
   * @param blockSize
   * @param progress
   * @return
   * @throws IOException
   */
=======
>>>>>>> benco
  @Override
  public FSDataOutputStream create(Path f, FsPermission permission,
      boolean overwrite, int bufferSize, short replication, long blockSize,
      Progressable progress) throws IOException {
    return fs.create(f, permission,
        overwrite, bufferSize, replication, blockSize, progress);
  }


  /**
   * 复制一个存在的文件
   * @param src file name
   * @param replication new replication
   * @throws IOException
   * @return true if successful;
   *         false if file does not exist or is a directory
   */

  public boolean setReplication(Path src, short replication) throws IOException {
    return fs.setReplication(src, replication);
  }


  /**
   * 重命名文件，文件可以是本地文件系统和远程分布式文件系统
   */

  public boolean rename(Path src, Path dst) throws IOException {
    return fs.rename(src, dst);
  }

  /**
   * 删除文件，可选择是否递归删除
   */
  public boolean delete(Path f, boolean recursive) throws IOException {
    return fs.delete(f, recursive);
  }

  public boolean deleteOnExit(Path f) throws IOException {
    return fs.deleteOnExit(f);
  }


  /**
   * 列出文件状态
   * @param f given path
   * @return
   * @throws IOException
   */

  public FileStatus[] listStatus(Path f) throws IOException {
    return fs.listStatus(f);
  }

  /**
   * 获取home目录
   */
  public Path getHomeDirectory() {
    return fs.getHomeDirectory();
  }


  /**
   * 设置工作目录
   */

  public void setWorkingDirectory(Path newDir) {
    fs.setWorkingDirectory(newDir);
  }

  /**
   * 获取工作目录
   * @return the directory pathname
   */
  public Path getWorkingDirectory() {
    return fs.getWorkingDirectory();
  }

  /**
   * 获取初始工作目录
   */
  protected Path getInitialWorkingDirectory() {
    return fs.getInitialWorkingDirectory();
  }

  /**
   * 获取文件状态
   * */

  @Override
  public FsStatus getStatus(Path p) throws IOException {
    return fs.getStatus(p);
  }

<<<<<<< HEAD
  /** {@inheritDoc} */
  /**
   * 根据路径和权限创建目录
   * @param f
   * @param permission
   * @return
   * @throws IOException
   */
=======
>>>>>>> benco
  @Override
  public boolean mkdirs(Path f, FsPermission permission) throws IOException {
    return fs.mkdirs(f, permission);
  }

  public void copyFromLocalFile(boolean delSrc, Path src, Path dst)
    throws IOException {
    fs.copyFromLocalFile(delSrc, src, dst);
  }

  public void copyFromLocalFile(boolean delSrc, boolean overwrite,
                                Path[] srcs, Path dst)
    throws IOException {
    fs.copyFromLocalFile(delSrc, overwrite, srcs, dst);
  }

  /**
   * 从本地文件系统复制
   */

  public void copyFromLocalFile(boolean delSrc, boolean overwrite,
                                Path src, Path dst)
    throws IOException {
    fs.copyFromLocalFile(delSrc, overwrite, src, dst);
  }


  /**
   * 复制到本地文件系统
   */
  public void copyToLocalFile(boolean delSrc, Path src, Path dst)
    throws IOException {
    fs.copyToLocalFile(delSrc, src, dst);
  }

  public Path startLocalOutput(Path fsOutputFile, Path tmpLocalFile)
    throws IOException {
    return fs.startLocalOutput(fsOutputFile, tmpLocalFile);
  }

  public void completeLocalOutput(Path fsOutputFile, Path tmpLocalFile)
    throws IOException {
    fs.completeLocalOutput(fsOutputFile, tmpLocalFile);
  }

  public long getUsed() throws IOException{
    return fs.getUsed();

  }

  /**
   * 获取默认块大小
   */
   public long getDefaultBlockSize() {
    return fs.getDefaultBlockSize();
  }

  /**
   * 获得默认副本
   */
  public short getDefaultReplication() {
    return fs.getDefaultReplication();
  }


  /**
   * Get file status.
   * 获取文件状态信息
   */
  public FileStatus getFileStatus(Path f) throws IOException {
    return fs.getFileStatus(f);
  }


  public FileChecksum getFileChecksum(Path f) throws IOException {
    return fs.getFileChecksum(f);
  }

  /**
   * 设置确认校验和的布尔变量
   * @param verifyChecksum
   */
  public void setVerifyChecksum(boolean verifyChecksum) {
    fs.setVerifyChecksum(verifyChecksum);
  }

  @Override
  public Configuration getConf() {
    return fs.getConf();
  }

  /**
   * 关闭文件系统实例
   * @throws IOException
   */
  @Override
  public void close() throws IOException {
    super.close();
    fs.close();
  }

  /**
   * 设置文件拥有者
   * @param p The path
   * @param username If it is null, the original username remains unchanged.
   * @param groupname If it is null, the original groupname remains unchanged.
   * @throws IOException
   */
  @Override
  public void setOwner(Path p, String username, String groupname
      ) throws IOException {
    fs.setOwner(p, username, groupname);
  }


  /**
   * 设置时间，包括修改时间和访问时间
   * */
  @Override
  public void setTimes(Path p, long mtime, long atime
      ) throws IOException {
    fs.setTimes(p, mtime, atime);
  }


  /**
   * 设置文件权限
   */
  @Override
  public void setPermission(Path p, FsPermission permission
      ) throws IOException {
    fs.setPermission(p, permission);
  }

  @Override
  protected FSDataOutputStream primitiveCreate(Path f,
      FsPermission absolutePermission, EnumSet<CreateFlag> flag,
      int bufferSize, short replication, long blockSize, Progressable progress, int bytesPerChecksum)
      throws IOException {
    return fs.primitiveCreate(f, absolutePermission, flag,
        bufferSize, replication, blockSize, progress, bytesPerChecksum);
  }

  @Override
  protected boolean primitiveMkdir(Path f, FsPermission abdolutePermission)
      throws IOException {
    return fs.primitiveMkdir(f, abdolutePermission);
  }
}
