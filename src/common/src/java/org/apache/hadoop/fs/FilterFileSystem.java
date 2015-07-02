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
 * FilterFileSystem是一个代理, 或者说, wrapper,
 * 它的成员属性包含一个@link FileSystem实例fs,
 * 把fs的所有protected方法导出成了public方法.
 *
 * Filter意为过滤器, @link FilterFileSystem选择的过滤策略是"直通",
 * 即什么都不做, 直接把参数传递给public方法对应的protected方法.
 * <code>FilterFileSystem</code> contains

 * FilterFileSystem继承了FileSystem类，重写了父类的所有方法 
 * 它转换数据或者增加方法，加以封装，相当于起到了过滤文件系统的作用。
 * FilterFileSystem包装了一个FileSystem对象，所有提供的方法都是与
 * FileSystem中相同的方法，通过调用FileSystem对象对应的函数来实现。
 * 此类的目的是通过继承此类来重写其中的方法，来添加更多地功能
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

  public BlockLocation[] getFileBlockLocations(FileStatus file, long start,
    long len) throws IOException {
      return fs.getFileBlockLocations(file, start, len);
  }

  public FSDataInputStream open(Path f, int bufferSize) throws IOException {
    return fs.open(f, bufferSize);
  }

  public FSDataOutputStream append(Path f, int bufferSize,
      Progressable progress) throws IOException {
    return fs.append(f, bufferSize, progress);
  }
  @Override
  public FSDataOutputStream create(Path f, FsPermission permission,
      boolean overwrite, int bufferSize, short replication, long blockSize,
      Progressable progress) throws IOException {
    return fs.create(f, permission,
        overwrite, bufferSize, replication, blockSize, progress);
  }

  public boolean setReplication(Path src, short replication) throws IOException {
    return fs.setReplication(src, replication);
  }

  public boolean rename(Path src, Path dst) throws IOException {
    return fs.rename(src, dst);
  }

  public boolean delete(Path f, boolean recursive) throws IOException {
    return fs.delete(f, recursive);
  }

  public boolean deleteOnExit(Path f) throws IOException {
    return fs.deleteOnExit(f);
  }

  public FileStatus[] listStatus(Path f) throws IOException {
    return fs.listStatus(f);
  }

  public Path getHomeDirectory() {
    return fs.getHomeDirectory();
  }

  public void setWorkingDirectory(Path newDir) {
    fs.setWorkingDirectory(newDir);
  }


  public Path getWorkingDirectory() {
    return fs.getWorkingDirectory();
  }

  protected Path getInitialWorkingDirectory() {
    return fs.getInitialWorkingDirectory();
  }

  @Override
  public FsStatus getStatus(Path p) throws IOException {
    return fs.getStatus(p);
  }

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

  public void copyFromLocalFile(boolean delSrc, boolean overwrite,
                                Path src, Path dst)
    throws IOException {
    fs.copyFromLocalFile(delSrc, overwrite, src, dst);
  }

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

  public long getDefaultBlockSize() {
    return fs.getDefaultBlockSize();
  }


  public short getDefaultReplication() {
    return fs.getDefaultReplication();
  }

 
  public FileStatus getFileStatus(Path f) throws IOException {
    return fs.getFileStatus(f);
  }


  public FileChecksum getFileChecksum(Path f) throws IOException {
    return fs.getFileChecksum(f);
  }


  public void setVerifyChecksum(boolean verifyChecksum) {
    fs.setVerifyChecksum(verifyChecksum);
  }

  @Override
  public Configuration getConf() {
    return fs.getConf();
  }

  @Override
  public void close() throws IOException {
    super.close();
    fs.close();
  }


  @Override
  public void setOwner(Path p, String username, String groupname
      ) throws IOException {
    fs.setOwner(p, username, groupname);
  }


  @Override
  public void setTimes(Path p, long mtime, long atime
      ) throws IOException {
    fs.setTimes(p, mtime, atime);
  }


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
