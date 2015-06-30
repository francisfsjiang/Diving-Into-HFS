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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

/**
 * 由FTP服务器支持的文件系统,基于FTP协议和FTP服务器交互的FileSystem API实现
 * 继承自FileSystem{@link FileSystem}
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class FTPFileSystem extends FileSystem {

  public static final Log LOG = LogFactory
      .getLog(FTPFileSystem.class);

  public static final int DEFAULT_BUFFER_SIZE = 1024 * 1024;

  public static final int DEFAULT_BLOCK_SIZE = 4 * 1024;

  private URI uri;
/**
  * 使用超类FileSystem调用initialize,通过uri获得host,port,和userAndPassword
  * 初始化的函数通过获得的host,port,user,password配置Configuration对象.
  * host通过Configuration对象设置保存在fs.ftp.host
  * port通过Configuration对象设置保存在fs.ftp.host.port
  * user通过Configuration对象设置保存在"fs.ftp.user." + host 
  * password通过Configuration对象设置保存在"fs.ftp.password." + host
  * @param uri
  * @param conf
  * @throw IOException
  */
  @Override
  public void initialize(URI uri, Configuration conf) throws IOException { // get
    super.initialize(uri, conf);
    // get host information from uri (overrides info in conf)
    String host = uri.getHost();
    host = (host == null) ? conf.get("fs.ftp.host", null) : host;
    if (host == null) {
      throw new IOException("Invalid host specified");
    }
    conf.set("fs.ftp.host", host);

    // get port information from uri, (overrides info in conf)
    int port = uri.getPort();
    port = (port == -1) ? FTP.DEFAULT_PORT : port;
    conf.setInt("fs.ftp.host.port", port);

    // get user/password information from URI (overrides info in conf)
    String userAndPassword = uri.getUserInfo();
    if (userAndPassword == null) {
      userAndPassword = (conf.get("fs.ftp.user." + host, null) + ":" + conf
          .get("fs.ftp.password." + host, null));
      if (userAndPassword == null) {
        throw new IOException("Invalid user/passsword specified");
      }
    }
    String[] userPasswdInfo = userAndPassword.split(":");
    conf.set("fs.ftp.user." + host, userPasswdInfo[0]);
    if (userPasswdInfo.length > 1) {
      conf.set("fs.ftp.password." + host, userPasswdInfo[1]);
    } else {
      conf.set("fs.ftp.password." + host, null);
    }
    setConf(conf);
    this.uri = uri;
  }

  /**
   * 使用获取得到的Configuration配置FTP Server
   * 通过在从初始化里保存好的host,port,user,password获取相应的值
   * 使用FTPClient类生成FTPClient对象,并通过给定主机地址和端口号进行连接
   * 使用FTPClient对象和从Configuration对象中取出的user和password Sign In
   * 设置FTP.BLOCK_TRANSFER_MODE,FTP.BINARY_FILE_TYPE and DEFAULT_BUFFER_SIZE
   * @return FTPClient对象
   * @throws IOException
   */
  private FTPClient connect() throws IOException {
    FTPClient client = null;
    Configuration conf = getConf();
    String host = conf.get("fs.ftp.host");
    int port = conf.getInt("fs.ftp.host.port", FTP.DEFAULT_PORT);
    String user = conf.get("fs.ftp.user." + host);
    String password = conf.get("fs.ftp.password." + host);
    client = new FTPClient();
    client.connect(host, port);
    int reply = client.getReplyCode();
    if (!FTPReply.isPositiveCompletion(reply)) {
      throw new IOException("Server - " + host
          + " refused connection on port - " + port);
    } else if (client.login(user, password)) {
      client.setFileTransferMode(FTP.BLOCK_TRANSFER_MODE);
      client.setFileType(FTP.BINARY_FILE_TYPE);
      client.setBufferSize(DEFAULT_BUFFER_SIZE);
    } else {
      throw new IOException("Login failed on server - " + host + ", port - "
          + port);
    }

    return client;
  }

  /**
   * FTPFileSystem类断开连接API
   * 判断FTPClient对象是否为null,不为null则进一步判断是否没有连接
   * 当判断为有连接,则通过FTPClient调用disconnect方法断开连接
   * logoutSuccess记录断开连接登出的状态,判断是否断开连接成功
   * @param client
   * @throws IOException
   */
  private void disconnect(FTPClient client) throws IOException {
    if (client != null) {
      if (!client.isConnected()) {
        throw new FTPException("Client not connected");
      }
      boolean logoutSuccess = client.logout();
      client.disconnect();
      if (!logoutSuccess) {
        LOG.warn("Logout failed while disconnecting, error code - "
            + client.getReplyCode());
      }
    }
  }

  /**
   * Resolve against given working directory. *
   * 通过给定workDir工作目录和path路径
   * @param workDir
   * @param path
   * @return Path对象,为当前路径下的工作目录
   */
  private Path makeAbsolute(Path workDir, Path path) {
    if (path.isAbsolute()) {
      return path;
    }
    return new Path(workDir, path);
  }
 /** 
   * open函数接收两个参数,Path对象file和基本数据类型int bufferSize
   * 获得连接的FTPClient对象,并得到工作目录的绝对路径
   * 判断fileStat是不是一个目录,若是则断开FTPClient对象client连接
   * 通过client获得bufferSize大小的字节数
   * 获得当前工作目录的父路径,只有这样才能在使用InputStream状态下读
   * 当在FSDataInputStream内调用close()方法,FTPClient对象断开连接
   * 当FTPClient对象处于inconsistent状态,则要在FTP Server端登出和断开连接,并关闭文件流操作
   * @param file
   * @param bufferSize
   * @return FSDataInputStream对象 fis
   * @throw IOException
   */
  @Override
  public FSDataInputStream open(Path file, int bufferSize) throws IOException {
    FTPClient client = connect();
    Path workDir = new Path(client.printWorkingDirectory());
    Path absolute = makeAbsolute(workDir, file);
    FileStatus fileStat = getFileStatus(client, absolute);
    if (fileStat.isDirectory()) {
      disconnect(client);
      throw new IOException("Path " + file + " is a directory.");
    }
    client.allocate(bufferSize);
    Path parent = absolute.getParent();
    client.changeWorkingDirectory(parent.toUri().getPath());
    InputStream is = client.retrieveFileStream(file.getName());
    FSDataInputStream fis = new FSDataInputStream(new FTPInputStream(is,
        client, statistics));
    if (!FTPReply.isPositivePreliminary(client.getReplyCode())) {
      fis.close();
      throw new IOException("Unable to open file: " + file + ", Aborting");
    }
    return fis;
  }

  /**
   * @param file Path对象,file的URI路径
   * @param permission FsPermission对象, FsPermission实现Writeable接口,强调文件系统的写入许可
   * @param overwrite 布尔类型判断文件路径是否被覆盖
   * @param bufferSize 字节缓冲区大小
   * @param replication  
   * @param blockSize 
   * @return FSDataOutputStream对象 fos
   * create方法获得连接的FTPClient对象,并得到工作目录的绝对路径
   * 判断client对象是否存在file路径并判断是否被重写了,若存在,并重写了则删了这个路径,不然则断开FTPClient连接
   * 抛出异常IOException("File already exists: " + file)
   * 获得当前工作目录的父路径,若没有父路径或者无法在父路径中有可写准许,则断开FTPClient连接
   * 通过client获得bufferSize大小的字节数
   * 当在FSDataInputStream内FTPClient对象调用close()方法,FTPClient对象断开连接
   * 当FTPClient对象处于inconsistent状态,则要在FTP Server端登出和断开连接,并关闭文件流操作
   */
  @Override
  public FSDataOutputStream create(Path file, FsPermission permission,
      boolean overwrite, int bufferSize, short replication, long blockSize,
      Progressable progress) throws IOException {
    final FTPClient client = connect();
    Path workDir = new Path(client.printWorkingDirectory());
    Path absolute = makeAbsolute(workDir, file);
    if (exists(client, file)) {
      if (overwrite) {
        delete(client, file);
      } else {
        disconnect(client);
        throw new IOException("File already exists: " + file);
      }
    }
    
    Path parent = absolute.getParent();
    if (parent == null || !mkdirs(client, parent, FsPermission.getDefault())) {
      parent = (parent == null) ? new Path("/") : parent;
      disconnect(client);
      throw new IOException("create(): Mkdirs failed to create: " + parent);
    }
    client.allocate(bufferSize);
    client.changeWorkingDirectory(parent.toUri().getPath());
    FSDataOutputStream fos = new FSDataOutputStream(client.storeFileStream(file
        .getName()), statistics) {
      @Override
      public void close() throws IOException {
        super.close();
        if (!client.isConnected()) {
          throw new FTPException("Client not connected");
        }
        boolean cmdCompleted = client.completePendingCommand();
        disconnect(client);
        if (!cmdCompleted) {
          throw new FTPException("Could not complete transfer, Reply Code - "
              + client.getReplyCode());
        }
      }
    };
    if (!FTPReply.isPositivePreliminary(client.getReplyCode())) {
      fos.close();
      throw new IOException("Unable to create file: " + file + ", Aborting");
    }
    return fos;
  }

  /** append方法目前尚不支持 */
  public FSDataOutputStream append(Path f, int bufferSize,
      Progressable progress) throws IOException {
    throw new IOException("Not supported");
  }
  
  /**
   * exists方法可以不需要打开一个新的连接可以判断FTPClient对象client是否有文件目录file
   * @param FTPClient对象 client
   * @param Path对象 file
   * @return boolean 
   */
  private boolean exists(FTPClient client, Path file) {
    try {
      return getFileStatus(client, file) != null;
    } catch (FileNotFoundException fnfe) {
      return false;
    } catch (IOException ioe) {
      throw new FTPException("Failed to get file status", ioe);
    }
  }
  /**
   * Override的delete方法通过传入文件目录file,当recursive为true,使用建立连接的FTPClient对象删除这个文件目录
   * @param Path对象 file
   * @param FTPClient对象 client
   * @throw IOException IO异常
   * @return boolean recursive
   */
  @Override
  public boolean delete(Path file, boolean recursive) throws IOException {
    FTPClient client = connect();
    try {
      boolean success = delete(client, file, recursive);
      return success;
    } finally {
      disconnect(client);
    }
  }

  /**
   * @param Path对象 file
   * @param FTPClient对象 client
   * delete装饰器调用上面提到的Override的delete方法
   */
  @Deprecated
  private boolean delete(FTPClient client, Path file) throws IOException {
    return delete(client, file, false);
  }

  /**
   * @param FTPClient对象 client
   * @param Path对象 file
   * @param boolean recursive
   * 获得client的工作目录并与file组合得到文件目录的绝对路径
   * 获得绝对路径下的所有文件,并调用delete方法全部删除
   */
  private boolean delete(FTPClient client, Path file, boolean recursive)
      throws IOException {
    Path workDir = new Path(client.printWorkingDirectory());
    Path absolute = makeAbsolute(workDir, file);
    String pathName = absolute.toUri().getPath();
    FileStatus fileStat = getFileStatus(client, absolute);
    if (fileStat.isFile()) {
      return client.deleteFile(pathName);
    }
    FileStatus[] dirEntries = listStatus(client, absolute);
    if (dirEntries != null && dirEntries.length > 0 && !(recursive)) {
      throw new IOException("Directory: " + file + " is not empty.");
    }
    if (dirEntries != null) {
      for (int i = 0; i < dirEntries.length; i++) {
        delete(client, new Path(absolute, dirEntries[i].getPath()), recursive);
      }
    }
    return client.removeDirectory(pathName);
  }

/**
  * @param int accessGroup 
  * @param FTPfile对象 ftpFile
  * @return FsAction对象 action
  * 判断ftpFile与accessGroup是否有可读允许,有则将FsAction对象action赋值为FsAction.READ
  * 判断ftpFile与accessGroup是否有可写允许,有则将FsAction对象action赋值为FsAction.WRITE
  * 判断ftpFile与accessGroup是否有可操作允许,有则将FsAction对象action赋值为FsAction.EXECUTE
  */
  private FsAction getFsAction(int accessGroup, FTPFile ftpFile) {
    FsAction action = FsAction.NONE;
    if (ftpFile.hasPermission(accessGroup, FTPFile.READ_PERMISSION)) {
      action.or(FsAction.READ);
    }
    if (ftpFile.hasPermission(accessGroup, FTPFile.WRITE_PERMISSION)) {
      action.or(FsAction.WRITE);
    }
    if (ftpFile.hasPermission(accessGroup, FTPFile.EXECUTE_PERMISSION)) {
      action.or(FsAction.EXECUTE);
    }
    return action;
  }
/**
  * @param FTPFile对象 ftpFile
  * @return FsPermission对象
  * 通过给user,group,others设置FsAction属性,创建FsPermission对象并返回
  */
  private FsPermission getPermissions(FTPFile ftpFile) {
    FsAction user, group, others;
    user = getFsAction(FTPFile.USER_ACCESS, ftpFile);
    group = getFsAction(FTPFile.GROUP_ACCESS, ftpFile);
    others = getFsAction(FTPFile.WORLD_ACCESS, ftpFile);
    return new FsPermission(user, group, others);
  }
/**
  * @return URI对象
  * 返回uri格式
  */ 
  @Override
  public URI getUri() {
    return uri;
  }
/**
  * @param Path对象 file文件目录
  * @throw IOException 
  * 使用FTPClient对象client获得连接,并返回当前文件目录下的文件列表
  */ 
  @Override
  public FileStatus[] listStatus(Path file) throws IOException {
    FTPClient client = connect();
    try {
      FileStatus[] stats = listStatus(client, file);
      return stats;
    } finally {
      disconnect(client);
    }
  }

  /**
   * @param FTPClient对象 client
   * @param Path对象 file
   * listStatus方法也是与上述方法实现类似功能,获得当前FTPClient对象client的工作目录
   * 然后获取其绝对路径,最后返回绝对路径下的所有文件列表.
   */
  private FileStatus[] listStatus(FTPClient client, Path file)
      throws IOException {
    Path workDir = new Path(client.printWorkingDirectory());
    Path absolute = makeAbsolute(workDir, file);
    FileStatus fileStat = getFileStatus(client, absolute);
    if (fileStat.isFile()) {
      return new FileStatus[] { fileStat };
    }
    FTPFile[] ftpFiles = client.listFiles(absolute.toUri().getPath());
    FileStatus[] fileStats = new FileStatus[ftpFiles.length];
    for (int i = 0; i < ftpFiles.length; i++) {
      fileStats[i] = getFileStatus(ftpFiles[i], absolute);
    }
    return fileStats;
  }
/**
  * @param Path对象 file
  * @return FileStatus对象 status
  * 通过getFileStatus的参数file,获得FTPClient对象client文件目录的状态
  */
  @Override
  public FileStatus getFileStatus(Path file) throws IOException {
    FTPClient client = connect();
    try {
      FileStatus status = getFileStatus(client, file);
      return status;
    } finally {
      disconnect(client);
    }
  }

  /**
   * @param FTPClient对象 client
   * @param Path对象 file
   * @return FileStatus对象 fileStat
   * @throw IOException
   * 获取当前工作路径的绝对路径,再获得绝对路径的父路径
   * 若父路径不存在,则对它进行设置
   * 若父路径存在,则遍历其文件列表,判断是否存在ftpFile.getName() == file.getName()
   * 若存在则继续递归下去寻找getFileStatus(ftpFile, parentPath)
   */
  private FileStatus getFileStatus(FTPClient client, Path file)
      throws IOException {
    FileStatus fileStat = null;
    Path workDir = new Path(client.printWorkingDirectory());
    Path absolute = makeAbsolute(workDir, file);
    Path parentPath = absolute.getParent();
    if (parentPath == null) { // root dir
      long length = -1; // Length of root dir on server not known
      boolean isDir = true;
      int blockReplication = 1;
      long blockSize = DEFAULT_BLOCK_SIZE; // Block Size not known.
      long modTime = -1; // Modification time of root dir not known.
      Path root = new Path("/");
      return new FileStatus(length, isDir, blockReplication, blockSize,
          modTime, root.makeQualified(this));
    }
    String pathName = parentPath.toUri().getPath();
    FTPFile[] ftpFiles = client.listFiles(pathName);
    if (ftpFiles != null) {
      for (FTPFile ftpFile : ftpFiles) {
        if (ftpFile.getName().equals(file.getName())) { // 文件目录找到,向下递归
          fileStat = getFileStatus(ftpFile, parentPath);
          break;
        }
      }
      if (fileStat == null) {
        throw new FileNotFoundException("File " + file + " does not exist.");
      }
    } else {
      throw new FileNotFoundException("File " + file + " does not exist.");
    }
    return fileStat;
  }

  /**
   * 覆盖在FTPFile{@link FileStatus}文件目录信息
   * 使用默认的blockSize当FTP Client不清楚Server的Block Size
   * @param ftpFile
   * @param parentPath
   * @return FileStatus
   */
  private FileStatus getFileStatus(FTPFile ftpFile, Path parentPath) {
    long length = ftpFile.getSize();
    boolean isDir = ftpFile.isDirectory();
    int blockReplication = 1;
    // Using default block size since there is no way in FTP client to know of
    // block sizes on server. The assumption could be less than ideal.
    long blockSize = DEFAULT_BLOCK_SIZE;
    long modTime = ftpFile.getTimestamp().getTimeInMillis();
    long accessTime = 0;
    FsPermission permission = getPermissions(ftpFile);
    String user = ftpFile.getUser();
    String group = ftpFile.getGroup();
    Path filePath = new Path(parentPath, ftpFile.getName());
    return new FileStatus(length, isDir, blockReplication, blockSize, modTime,
        accessTime, permission, user, group, filePath.makeQualified(this));
  }
  /**
   * 使用FTPClient对象client获得连接,并判断是否能创建文件目录
   * @param Path对象 file
   * @param FsPermission对象 permission
   * @thros IOException
   * @return boolean 是否能创建文件目录
   */
  @Override
  public boolean mkdirs(Path file, FsPermission permission) throws IOException {
    FTPClient client = connect();
    try {
      boolean success = mkdirs(client, file, permission);
      return success;
    } finally {
      disconnect(client);
    }
  }

  /**
   * @param FTPClient对象 client
   * @param Path对象 file
   * @param FsPermission对象 permission
   * @thros IOException
   * @return boolean 是否能创建文件目录
   * 通过给定的client,file和permission判断是否可以在当前工作目录的绝对路径下创建一个文件目录
   */
  private boolean mkdirs(FTPClient client, Path file, FsPermission permission)
      throws IOException {
    boolean created = true;
    Path workDir = new Path(client.printWorkingDirectory());
    Path absolute = makeAbsolute(workDir, file);
    String pathName = absolute.getName();
    if (!exists(client, absolute)) {
      Path parent = absolute.getParent();
      created = (parent == null || mkdirs(client, parent, FsPermission
          .getDefault()));
      if (created) {
        String parentDir = parent.toUri().getPath();
        client.changeWorkingDirectory(parentDir);
        created = created & client.makeDirectory(pathName);
      }
    } else if (isFile(client, absolute)) {
      throw new IOException(String.format(
          "Can't make directory for path %s since it is a file.", absolute));
    }
    return created;
  }

  /**
   * @param FTPClient对象 client
   * @param Path对象 file
   * @thros IOException
   * @return boolean 是否是文件目录
   * 通过调用getFileStatus(client, file).isFile()方法判断当前client实例在file目录下是否存在文件目录
   */
  private boolean isFile(FTPClient client, Path file) {
    try {
      return getFileStatus(client, file).isFile();
    } catch (FileNotFoundException e) {
      return false; // file does not exist
    } catch (IOException ioe) {
      throw new FTPException("File check failed", ioe);
    }
  }

  /**
   * @param Path对象 src
   * @param Path对象 dst
   * @return 判断源文件目录是否改名为dst
   * 调用 rename(FTPClient client, Path src, Path dst)函数,并返回结果
   */
  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    FTPClient client = connect();
    try {
      boolean success = rename(client, src, dst);
      return success;
    } finally {
      disconnect(client);
    }
  }

  /**
   * 
   * @param client
   * @param src
   * @param dst
   * @return
   * @throws IOException
   * 获取src的绝对路径和dst的绝对路径,若src的绝对路径的父路径与dst的不相等,则无法重命名
   * 重命名成功后则返回true
   */
  private boolean rename(FTPClient client, Path src, Path dst)
      throws IOException {
    Path workDir = new Path(client.printWorkingDirectory());
    Path absoluteSrc = makeAbsolute(workDir, src);
    Path absoluteDst = makeAbsolute(workDir, dst);
    if (!exists(client, absoluteSrc)) {
      throw new IOException("Source path " + src + " does not exist");
    }
    if (exists(client, absoluteDst)) {
      throw new IOException("Destination path " + dst
          + " already exist, cannot rename!");
    }
    String parentSrc = absoluteSrc.getParent().toUri().toString();
    String parentDst = absoluteDst.getParent().toUri().toString();
    String from = src.getName();
    String to = dst.getName();
    if (!parentSrc.equals(parentDst)) {
      throw new IOException("Cannot rename parent(source): " + parentSrc
          + ", parent(destination):  " + parentDst);
    }
    client.changeWorkingDirectory(parentSrc);
    boolean renamed = client.rename(from, to);
    return renamed;
  }
/**
  * @return 调用getHomeDirectory()返回home文件路径
  */
  @Override
  public Path getWorkingDirectory() {
    return getHomeDirectory();
  }
/**
  * @return Path 返回当前工作目录
  */
  @Override
  public Path getHomeDirectory() {
    FTPClient client = null;
    try {
      client = connect();
      Path homeDir = new Path(client.printWorkingDirectory());
      return homeDir;
    } catch (IOException ioe) {
      throw new FTPException("Failed to get home directory", ioe);
    } finally {
      try {
        disconnect(client);
      } catch (IOException ioe) {
        throw new FTPException("Failed to disconnect", ioe);
      }
    }
  }

  @Override
  public void setWorkingDirectory(Path newDir) {
    // 不保持工作路径的状态,所以这个方法并没有实现
  }
}
