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

import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Options.CreateOpts;
import org.apache.hadoop.fs.Options.Rename;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.io.MultipleIOException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.ReflectionUtils;

/****************************************************************
 * FileSystem(下简称FS)是一般的通用文件系统的抽象基类
 * 继承了Configured类，提供了访问配置文件的方法
 * 实现了Closeable接口，实现了关闭流的方法
 * FS可以被实现为分布式的文件系统，如HDFS，亦或是一个本地的文件系统，
 * 所有具有访问HDFS的潜在可能的用户代码都应该被封装在一个FileSystem实例里。
 * HDFS是一个对外表现如同单一磁盘的多机系统，
 * 它的容错能力和对大容量存储的支持使得HDFS尤为实用。
 * ==
 * 1. 域:
 * \begin{XeEnum}
 *      \item 文件系统的缓存
 *      \item 键
 *      \item 文件系统的子类与统计信息的映射
 *      \item 统计信息
 *      \item 缓存中文件路径的集合
 * \end{XeEnum}
 * ==
 * 2.内部类:
 * \begin{XeEnum}
 *      \item Cache类:用来缓存文件系统
 *      \item ClientFinalizer类:JVM关闭是调用进行清理
 *      \item Key类:保存了与文件系统uri的信息，与文件系统对应
 *      \item Statistics:用来记录统计信息的类
 * \end{XeEnum}
 * ==
 * 3.方法:
 * ==
 * 文件系统的关闭:
 * \begin{XeEnum}
 *      \item closeAll()
 *      \item close()
 * \end{XeEnum}
 * ==
 * 文件系统的读取数据:
 * \begin{quote}
 * FSDataInputStream open(Path f, int bufferSize)
 * \end{quote}
 * ==
 * 文件系统的写入数据:
 * \begin{quote}
 * FSDataOutputStream create(Path f) \\
 * FSDataOutputStream append(Path f) 
 * \end{quote}
 * ==
 * 重命名操作: \\
 * boolean rename(Path src, Path dst)
 * ==
 * 文件删除操作: \\
 * boolean delete(Path f)
 * ==
 * 文件或路径测试: \\
 * \begin{XeEnum}
 *      \item boolean exists(Path f)
 *      \item boolean isDirectory(Path f)
 *      \item boolean isFile(Path f)
 * \end{XeEnum}
 * ==
 * 文件复制操作:
 * \begin{XeEnum}
 *     \item copyFromLocalFile(Path src, Path dst)实现将文件从本地复制到其它路径
 *     \item copyToLocalFile(Path src, Path dst)负责将FS下的文件复制到本地
 * \end{XeEnum}
 * ==
 * 文件移动操作: \\
 * \begin{XeEnum}
 *      \item moveFromLocalFile(Path[] srcs, Path dst)负责将本地文件移动到FS的其它位置
 *      \item moveToLocalFile(Path src, Path dst)将FS下的文件移动到本地
 * \end{XeEnum}
 * == 
 * 文件查询: \\
 * \begin{XeEnum}
 *      \item FileStatus[] listStatus(Path f)
 *      \item FileStatus[] globStatus(Path pathPattern)
 * \end{XeEnum}
 * == 
 * 通配格式: \\
 * interface PathFilter
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public abstract class FileSystem extends Configured implements Closeable {
  public static final String FS_DEFAULT_NAME_KEY =
                   CommonConfigurationKeys.FS_DEFAULT_NAME_KEY;
  public static final String DEFAULT_FS =
                   CommonConfigurationKeys.FS_DEFAULT_NAME_DEFAULT;

  public static final Log LOG = LogFactory.getLog(FileSystem.class);

  /**
   * 文件系统缓存
   */
  static final Cache CACHE = new Cache();

  /**
   * 文件系统在缓存中的键
   */
  private Cache.Key key;

  /**
   * 被所有FileSystem所继承的共享的统计量
   */
  private static final Map<Class<? extends FileSystem>, Statistics>
    statisticsTable =
      new IdentityHashMap<Class<? extends FileSystem>, Statistics>();

  /**
   * 文件系统的统计信息
   */
  protected Statistics statistics;

  /**
   * 缓存中文件对应的路径，用来在JVM退出的时候清空缓存中的文件
   */
  private Set<Path> deleteOnExit = new TreeSet<Path>();

  /**
   * 通过URI、配置对象和用户来获取缓存中的一个文件系统对象
   * 具体过程为先判断用户字符串，若为空，则获取用户组信息的当前用户；
   * 若不为空，则创建远程用户。最后根据URI和配置对象返回文件系统对象
   * @param uri
   * @param conf
   * @param user
   * @return the filesystem instance
   * @throws IOException
   * @throws InterruptedException
   */
  public static FileSystem get(final URI uri, final Configuration conf,
        final String user) throws IOException, InterruptedException {
    UserGroupInformation ugi;
    if (user == null) {
      ugi = UserGroupInformation.getCurrentUser();
    } else {
      ugi = UserGroupInformation.createRemoteUser(user);
    }
    return ugi.doAs(new PrivilegedExceptionAction<FileSystem>() {
      public FileSystem run() throws IOException {
        return get(uri, conf);
      }
    });
  }


  public static FileSystem get(Configuration conf) throws IOException {
    return get(getDefaultUri(conf), conf);
  }

  /**
   * 通过配置对象获取默认URI
   * @param conf the configuration to access
   * @return the uri of the default filesystem
   */
  public static URI getDefaultUri(Configuration conf) {
    return URI.create(fixName(conf.get(FS_DEFAULT_NAME_KEY, DEFAULT_FS)));
  }

  /**
   * 通过配置对象和URI设置默认URI
   * @param conf the configuration to alter
   * @param uri the new default filesystem uri
   */
  public static void setDefaultUri(Configuration conf, URI uri) {
    conf.set(FS_DEFAULT_NAME_KEY, uri.toString());
  }


  public static void setDefaultUri(Configuration conf, String uri) {
    setDefaultUri(conf, URI.create(fixName(uri)));
  }

  /**
   * 通过URI和配置对象初始化，文件系统实例化后调用
   * @param name a uri whose authority section names the host, port, etc.
   *   for this FileSystem
   * @param conf the configuration
   */
  public void initialize(URI name, Configuration conf) throws IOException {
    statistics = getStatistics(name.getScheme(), getClass());
  }

  /**
   * 返回可以标示改文件系统的URI
   */
  public abstract URI getUri();

  @Deprecated
  public String getName() { return getUri().toString(); }

  @Deprecated
  public static FileSystem getNamed(String name, Configuration conf)
    throws IOException {
    return get(URI.create(fixName(name)), conf);
  }

  /**
   * 为了向下兼容，更新旧格式的文件系统名字
   */
  private static String fixName(String name) {
    // convert old-format name to new-format name
    if (name.equals("local")) {         // "local" is now "file:///".
      LOG.warn("\"local\" is a deprecated filesystem name."
               +" Use \"file:///\" instead.");
      name = "file:///";
    } else if (name.indexOf('/')==-1) {   // unqualified is "hdfs://"
      LOG.warn("\""+name+"\" is a deprecated filesystem name."
               +" Use \"hdfs://"+name+"/\" instead.");
      name = "hdfs://"+name;
    }
    return name;
  }

  /**
   * 获取本地文件系统实例
   * @param conf the configuration to configure the file system with
   * @return a LocalFileSystem
   */
  public static LocalFileSystem getLocal(Configuration conf)
    throws IOException {
    return (LocalFileSystem)get(LocalFileSystem.NAME, conf);
  }


  public static FileSystem get(URI uri, Configuration conf) throws IOException {
    String scheme = uri.getScheme();
    String authority = uri.getAuthority();

    if (scheme == null) {                       // no scheme: use default FS
      return get(conf);
    }

    if (authority == null) {                       // no authority
      URI defaultUri = getDefaultUri(conf);
      if (scheme.equals(defaultUri.getScheme())    // if scheme matches default
          && defaultUri.getAuthority() != null) {  // & default has authority
        return get(defaultUri, conf);              // return default
      }
    }

    String disableCacheName = String.format("fs.%s.impl.disable.cache", scheme);
    if (conf.getBoolean(disableCacheName, false)) {
      return createFileSystem(uri, conf);
    }

    return CACHE.get(uri, conf);
  }

  /**
   * Returns the FileSystem for this URI's scheme and authority and the
   * passed user. Internally invokes {@link #newInstance(URI, Configuration)}
   * 通过URI模式、权限和用户名返回文件系统实例
   * @param uri
   * @param conf
   * @param user
   * @return filesystem instance
   * @throws IOException
   * @throws InterruptedException
   */
  public static FileSystem newInstance(final URI uri, final Configuration conf,
      final String user) throws IOException, InterruptedException {
    UserGroupInformation ugi;
    if (user == null) {
      ugi = UserGroupInformation.getCurrentUser();
    } else {
      ugi = UserGroupInformation.createRemoteUser(user);
    }
    return ugi.doAs(new PrivilegedExceptionAction<FileSystem>() {
      public FileSystem run() throws IOException {
        return newInstance(uri,conf);
      }
    });
  }
  /**
   * 通过URI模式、权限和用户名返回文件系统实例。
   *
   * 具体实现是通过读取配置文件中的<code>fs.[scheme].Impl</code>对应
   * 的值。
   *
   * 该方法总返回一个新的文件系统的值。
   */
  public static FileSystem newInstance(URI uri, Configuration conf) throws IOException {
    String scheme = uri.getScheme();
    String authority = uri.getAuthority();

    if (scheme == null) {                       // no scheme: use default FS
      return newInstance(conf);
    }

    if (authority == null) {                       // no authority
      URI defaultUri = getDefaultUri(conf);
      if (scheme.equals(defaultUri.getScheme())    // if scheme matches default
          && defaultUri.getAuthority() != null) {  // & default has authority
        return newInstance(defaultUri, conf);              // return default
      }
    }
    return CACHE.getUnique(uri, conf);
  }



   public static FileSystem newInstance(Configuration conf) throws IOException {
    return newInstance(getDefaultUri(conf), conf);
  }

  /**
   * 获取一个唯一的本地文件系统实例
   * @param conf the configuration to configure the file system with
   * @return a LocalFileSystem
   */
  public static LocalFileSystem newInstanceLocal(Configuration conf)
    throws IOException {
    return (LocalFileSystem)newInstance(LocalFileSystem.NAME, conf);
  }

  /**
   * 关闭所有缓存的文件系统实例。
   * @throws IOException
   */
  public static void closeAll() throws IOException {
    CACHE.closeAll();
  }

  /**
   * 该方法确认输入的路径可以确定一个文件系统
   * */
  public Path makeQualified(Path path) {
    checkPath(path);
    return path.makeQualified(this.getUri(), this.getWorkingDirectory());
  }

  /**
   * 通过提供的权限创建一个文件。
   *
   * 通常的实现是使用两个RPC，虽然低效，但是是线程安全的。另一种可能是
   * 在设置中将umask设置为0，但这样就无法保证线程安全。
   * @param fs file system handle
   * @param file the name of the file to be created
   * @param permission the permission of the file
   * @return an output stream
   * @throws IOException
   */
  public static FSDataOutputStream create(FileSystem fs,
      Path file, FsPermission permission) throws IOException {
    // create the file with default permission
    FSDataOutputStream out = fs.create(file);
    // set its permission to the supplied one
    fs.setPermission(file, permission);
    return out;
  }

  /**
   * 通过提供的权限创建一个目录。
   *
   * @param fs file system handle
   * @param dir the name of the directory to be created
   * @param permission the permission of the directory
   * @return true if the directory creation succeeds; false otherwise
   * @throws IOException
   */
  public static boolean mkdirs(FileSystem fs, Path dir, FsPermission permission)
  throws IOException {
    // create the directory using the default permission
    boolean result = fs.mkdirs(dir);
    // set its permission to be the supplied one
    fs.setPermission(dir, permission);
    return result;
  }

  ///////////////////////////////////////////////////////////////
  // FileSystem
  ///////////////////////////////////////////////////////////////

  protected FileSystem() {
    super(null);
  }

  /**
   * 检查路径是否属于该文件系统
   * */
  protected void checkPath(Path path) {
    URI uri = path.toUri();
    if (uri.getScheme() == null)                // fs is relative
      return;
    String thisScheme = this.getUri().getScheme();
    String thatScheme = uri.getScheme();
    String thisAuthority = this.getUri().getAuthority();
    String thatAuthority = uri.getAuthority();
    //authority and scheme are not case sensitive
    if (thisScheme.equalsIgnoreCase(thatScheme)) {// schemes match
      if (thisAuthority == thatAuthority ||       // & authorities match
          (thisAuthority != null &&
           thisAuthority.equalsIgnoreCase(thatAuthority)))
        return;

      if (thatAuthority == null &&                // path's authority is null
          thisAuthority != null) {                // fs has an authority
        URI defaultUri = getDefaultUri(getConf()); // & is the conf default
        if (thisScheme.equalsIgnoreCase(defaultUri.getScheme()) &&
            thisAuthority.equalsIgnoreCase(defaultUri.getAuthority()))
          return;
        try {                                     // or the default fs's uri
          defaultUri = get(getConf()).getUri();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
        if (thisScheme.equalsIgnoreCase(defaultUri.getScheme()) &&
            thisAuthority.equalsIgnoreCase(defaultUri.getAuthority()))
          return;
      }
    }
    throw new IllegalArgumentException("Wrong FS: "+path+
                                       ", expected: "+this.getUri());
  }

  /**
   * 返回一个文件的主机名、偏移量和分配的大小。
   * 对于一个不存在的文件，返回NULL。
   *
   * 此方法对于DFS非常重要，DFSClient通过此方法来获取
   * 指定文件的Block所在的datanode。
   *
   */
  public BlockLocation[] getFileBlockLocations(FileStatus file,
      long start, long len) throws IOException {
    if (file == null) {
      return null;
    }

    if ( (start<0) || (len < 0) ) {
      throw new IllegalArgumentException("Invalid start or len parameter");
    }

    if (file.getLen() < start) {
      return new BlockLocation[0];

    }
    String[] name = { "localhost:50010" };
    String[] host = { "localhost" };
    return new BlockLocation[] { new BlockLocation(name, host, 0, file.getLen()) };
  }



  public BlockLocation[] getFileBlockLocations(Path p,
      long start, long len) throws IOException {
    if (p == null) {
      throw new NullPointerException();
    }
    FileStatus file = getFileStatus(p);
    return getFileBlockLocations(file, start, len);
  }

  /**
   * 返回默认服务器配置变量值
   * @return server default configuration values
   * @throws IOException
   */
  public FsServerDefaults getServerDefaults() throws IOException {
    Configuration conf = getConf();
    return new FsServerDefaults(getDefaultBlockSize(),
        conf.getInt("io.bytes.per.checksum", 512),
        64 * 1024,
        getDefaultReplication(),
        conf.getInt("io.file.buffer.size", 4096));
  }

  /**
   * 在指示的路径上打开一个文件系统的数据输入流
   * @param f the file name to open
   * @param bufferSize the size of the buffer to be used.
   */
  public abstract FSDataInputStream open(Path f, int bufferSize)
    throws IOException;


  public FSDataInputStream open(Path f) throws IOException {
    return open(f, getConf().getInt("io.file.buffer.size", 4096));
  }


  public FSDataOutputStream create(Path f) throws IOException {
    return create(f, true);
  }


  public FSDataOutputStream create(Path f, boolean overwrite)
    throws IOException {
    return create(f, overwrite,
                  getConf().getInt("io.file.buffer.size", 4096),
                  getDefaultReplication(),
                  getDefaultBlockSize());
  }

  /**
   * 在指定的路径上打开一个{@link FSDataOutputStream}，同时会打开一个
   * 进度报告。文件默认会被覆盖。
   */
  public FSDataOutputStream create(Path f, Progressable progress) throws IOException {
    return create(f, true,
                  getConf().getInt("io.file.buffer.size", 4096),
                  getDefaultReplication(),
                  getDefaultBlockSize(), progress);
  }

  /**
   * Opens an FSDataOutputStream at the indicated Path.
   * Files are overwritten by default.
   */
  public FSDataOutputStream create(Path f, short replication)
    throws IOException {
    return create(f, true,
                  getConf().getInt("io.file.buffer.size", 4096),
                  replication,
                  getDefaultBlockSize());
  }

  /**
   * Opens an FSDataOutputStream at the indicated Path with write-progress
   * reporting.
   * Files are overwritten by default.
   */
  public FSDataOutputStream create(Path f, short replication, Progressable progress)
    throws IOException {
    return create(f, true,
                  getConf().getInt("io.file.buffer.size", 4096),
                  replication,
                  getDefaultBlockSize(), progress);
  }


  /**
   * Opens an FSDataOutputStream at the indicated Path.
   * @param f the file name to open
   * @param overwrite if a file with this name already exists, then if true,
   *   the file will be overwritten, and if false an error will be thrown.
   * @param bufferSize the size of the buffer to be used.
   */
  public FSDataOutputStream create(Path f,
                                   boolean overwrite,
                                   int bufferSize
                                   ) throws IOException {
    return create(f, overwrite, bufferSize,
                  getDefaultReplication(),
                  getDefaultBlockSize());
  }

  /**
   * Opens an FSDataOutputStream at the indicated Path with write-progress
   * reporting.
   * @param f the file name to open
   * @param overwrite if a file with this name already exists, then if true,
   *   the file will be overwritten, and if false an error will be thrown.
   * @param bufferSize the size of the buffer to be used.
   */
  public FSDataOutputStream create(Path f,
                                   boolean overwrite,
                                   int bufferSize,
                                   Progressable progress
                                   ) throws IOException {
    return create(f, overwrite, bufferSize,
                  getDefaultReplication(),
                  getDefaultBlockSize(), progress);
  }


  /**
   * Opens an FSDataOutputStream at the indicated Path.
   * @param f the file name to open
   * @param overwrite if a file with this name already exists, then if true,
   *   the file will be overwritten, and if false an error will be thrown.
   * @param bufferSize the size of the buffer to be used.
   * @param replication required block replication for the file.
   */
  public FSDataOutputStream create(Path f,
                                   boolean overwrite,
                                   int bufferSize,
                                   short replication,
                                   long blockSize
                                   ) throws IOException {
    return create(f, overwrite, bufferSize, replication, blockSize, null);
  }

  /**
   * Opens an FSDataOutputStream at the indicated Path with write-progress
   * reporting.
   * @param f the file name to open
   * @param overwrite if a file with this name already exists, then if true,
   *   the file will be overwritten, and if false an error will be thrown.
   * @param bufferSize the size of the buffer to be used.
   * @param replication required block replication for the file.
   */
  public FSDataOutputStream create(Path f,
                                            boolean overwrite,
                                            int bufferSize,
                                            short replication,
                                            long blockSize,
                                            Progressable progress
                                            ) throws IOException {
    return this.create(f, FsPermission.getDefault(), overwrite, bufferSize,
        replication, blockSize, progress);
  }

  /**
   * Opens an FSDataOutputStream at the indicated Path with write-progress
   * reporting.
   * @param f the file name to open
   * @param permission
   * @param overwrite if a file with this name already exists, then if true,
   *   the file will be overwritten, and if false an error will be thrown.
   * @param bufferSize the size of the buffer to be used.
   * @param replication required block replication for the file.
   * @param blockSize
   * @param progress
   * @throws IOException
   * @see #setPermission(Path, FsPermission)
   */
  public abstract FSDataOutputStream create(Path f,
      FsPermission permission,
      boolean overwrite,
      int bufferSize,
      short replication,
      long blockSize,
      Progressable progress) throws IOException;


  /*.
   * This create has been added to support the FileContext that processes
   * the permission
   * with umask before calling this method.
   * This a temporary method added to support the transition from FileSystem
   * to FileContext for user applications.
   */
  @Deprecated
  protected FSDataOutputStream primitiveCreate(Path f,
     FsPermission absolutePermission, EnumSet<CreateFlag> flag, int bufferSize,
     short replication, long blockSize, Progressable progress,
     int bytesPerChecksum) throws IOException {

    // Default impl  assumes that permissions do not matter and
    // nor does the bytesPerChecksum  hence
    // calling the regular create is good enough.
    // FSs that implement permissions should override this.

    if (exists(f)) {
      if (flag.contains(CreateFlag.APPEND)) {
        return append(f, bufferSize, progress);
      } else if (!flag.contains(CreateFlag.OVERWRITE)) {
        throw new IOException("File already exists: " + f);
      }
    } else {
      if (flag.contains(CreateFlag.APPEND) && !flag.contains(CreateFlag.CREATE))
        throw new IOException("File already exists: " + f.toString());
    }

    return this.create(f, absolutePermission, flag.contains(CreateFlag.OVERWRITE), bufferSize, replication,
        blockSize, progress);
  }

  /**
   * This version of the mkdirs method assumes that the permission is absolute.
   * It has been added to support the FileContext that processes the permission
   * with umask before calling this method.
   * This a temporary method added to support the transition from FileSystem
   * to FileContext for user applications.
   */
  @Deprecated
  protected boolean primitiveMkdir(Path f, FsPermission absolutePermission)
    throws IOException {
    // Default impl is to assume that permissions do not matter and hence
    // calling the regular mkdirs is good enough.
    // FSs that implement permissions should override this.
   return this.mkdirs(f, absolutePermission);
  }


  /**
   * This version of the mkdirs method assumes that the permission is absolute.
   * It has been added to support the FileContext that processes the permission
   * with umask before calling this method.
   * This a temporary method added to support the transition from FileSystem
   * to FileContext for user applications.
   */
  @Deprecated
  protected void primitiveMkdir(Path f, FsPermission absolutePermission,
                    boolean createParent)
    throws IOException {

    if (!createParent) { // parent must exist.
      // since the this.mkdirs makes parent dirs automatically
      // we must throw exception if parent does not exist.
      final FileStatus stat = getFileStatus(f.getParent());
      if (stat == null) {
        throw new FileNotFoundException("Missing parent:" + f);
      }
      if (!stat.isDirectory()) {
        throw new ParentNotDirectoryException("parent is not a dir");
      }
      // parent does exist - go ahead with mkdir of leaf
    }
    // Default impl is to assume that permissions do not matter and hence
    // calling the regular mkdirs is good enough.
    // FSs that implement permissions should override this.
    if (!this.mkdirs(f, absolutePermission)) {
      throw new IOException("mkdir of "+ f + " failed");
    }
  }


  /**
   * 在所给路径上创建一个全新的零长度的文件，如果文件已经存在，则会返回false。
   */
  public boolean createNewFile(Path f) throws IOException {
    if (exists(f)) {
      return false;
    } else {
      create(f, false, getConf().getInt("io.file.buffer.size", 4096)).close();
      return true;
    }
  }

  /**
   * 在一个存在文件上进行追加操作
   * @param f the existing file to be appended.
   * @throws IOException
   */
  public FSDataOutputStream append(Path f) throws IOException {
    return append(f, getConf().getInt("io.file.buffer.size", 4096), null);
  }
  /**
   * Append to an existing file (optional operation).
   * Same as append(f, bufferSize, null).
   * @param f the existing file to be appended.
   * @param bufferSize the size of the buffer to be used.
   * @throws IOException
   */
  public FSDataOutputStream append(Path f, int bufferSize) throws IOException {
    return append(f, bufferSize, null);
  }

  /**
   * Append to an existing file (optional operation).
   * @param f the existing file to be appended.
   * @param bufferSize the size of the buffer to be used.
   * @param progress for reporting progress if it is not null.
   * @throws IOException
   */
  public abstract FSDataOutputStream append(Path f, int bufferSize,
      Progressable progress) throws IOException;

 /**
   * 获取文件系统的副本数量。
   *
   * @deprecated Use getFileStatus() instead
   * @param src file name
   * @return file replication
   * @throws IOException
   */
  @Deprecated
  public short getReplication(Path src) throws IOException {
    return getFileStatus(src).getReplication();
  }

  /**
   * 对一个存在的文件进行复制操作
   * @param src file name
   * @param replication new replication
   * @throws IOException
   * @return true if successful;
   *         false if file does not exist or is a directory
   */
  public boolean setReplication(Path src, short replication)
    throws IOException {
    return true;
  }


  public abstract boolean rename(Path src, Path dst) throws IOException;

  /**
   * 重命名一个文件。
   *
   * 会在如下情形失败:
   *
   * - src是文件而dst是目录。
   *
   * - src是目录而dst是文件。
   *
   * - src或者dst的上级目录是文件。
   *
   * - 如果没有设置OVERWITE，且dst已经存在，则会失败。
   *
   * - 设置了OVERWRITE，但是dst是一个非的目录。
   *
   * 注意，原子性的重命名操作取决于文件系统的实现，默认的实现是非原子的。
   *
   */
  @Deprecated
  protected void rename(final Path src, final Path dst,
      final Rename... options) throws IOException {
    // Default implementation
    final FileStatus srcStatus = getFileStatus(src);
    if (srcStatus == null) {
      throw new FileNotFoundException("rename source " + src + " not found.");
    }

    boolean overwrite = false;
    if (null != options) {
      for (Rename option : options) {
        if (option == Rename.OVERWRITE) {
          overwrite = true;
        }
      }
    }

    FileStatus dstStatus;
    try {
      dstStatus = getFileStatus(dst);
    } catch (IOException e) {
      dstStatus = null;
    }
    if (dstStatus != null) {
      if (srcStatus.isDirectory() != dstStatus.isDirectory()) {
        throw new IOException("Source " + src + " Destination " + dst
            + " both should be either file or directory");
      }
      if (!overwrite) {
        throw new FileAlreadyExistsException("rename destination " + dst
            + " already exists.");
      }
      // Delete the destination that is a file or an empty directory
      if (dstStatus.isDirectory()) {
        FileStatus[] list = listStatus(dst);
        if (list != null && list.length != 0) {
          throw new IOException(
              "rename cannot overwrite non empty destination directory " + dst);
        }
      }
      delete(dst, false);
    } else {
      final Path parent = dst.getParent();
      final FileStatus parentStatus = getFileStatus(parent);
      if (parentStatus == null) {
        throw new FileNotFoundException("rename destination parent " + parent
            + " not found.");
      }
      if (!parentStatus.isDirectory()) {
        throw new ParentNotDirectoryException("rename destination parent " + parent
            + " is a file.");
      }
    }
    if (!rename(src, dst)) {
      throw new IOException("rename from " + src + " to " + dst + " failed.");
    }
  }

  /**
   * Delete a file
   * @deprecated Use {@link #delete(Path, boolean)} instead.
   */
  @Deprecated
  public boolean delete(Path f) throws IOException {
    return delete(f, true);
  }

  /**
   * 删除指定的文件
   */
  public abstract boolean delete(Path f, boolean recursive) throws IOException;

  /**
   * 标记一个在文件系统关闭的时候要被删除掉的路径。
   * 当JVM关闭的时候，所有的文件系统对象都会被自动关闭，
   * 那么所有被标记的路径都会被删除掉。
   * @param f the path to delete.
   * @return  true if deleteOnExit is successful, otherwise false.
   * @throws IOException
   */
  public boolean deleteOnExit(Path f) throws IOException {
    if (!exists(f)) {
      return false;
    }
    synchronized (deleteOnExit) {
      deleteOnExit.add(f);
    }
    return true;
  }

  /**
   * 退出时进行删除操作。该方法会递归删除所有指定路径的文件。
   */
  protected void processDeleteOnExit() {
    synchronized (deleteOnExit) {
      for (Iterator<Path> iter = deleteOnExit.iterator(); iter.hasNext();) {
        Path path = iter.next();
        try {
          delete(path, true);
        }
        catch (IOException e) {
          LOG.info("Ignoring failure to deleteOnExit for path " + path);
        }
        iter.remove();
      }
    }
  }

  /**
   * 检查路径是否存在
   * @param f source file
   */
  public boolean exists(Path f) throws IOException {
    try {
      return getFileStatus(f) != null;
    } catch (FileNotFoundException e) {
      return false;
    }
  }

  /**
   * 判断路径是否为目录.
   */
  public boolean isDirectory(Path f) throws IOException {
    try {
      return getFileStatus(f).isDirectory();
    } catch (FileNotFoundException e) {
      return false;               // f does not exist
    }
  }

  /**
   * 判断路径是否为常规文件
   */
  public boolean isFile(Path f) throws IOException {
    try {
      return getFileStatus(f).isFile();
    } catch (FileNotFoundException e) {
      return false;               // f does not exist
    }
  }

  /**
   * 返回文件的字节数
   */
  @Deprecated
  public long getLength(Path f) throws IOException {
    return getFileStatus(f).getLen();
  }

  /**
   * 获取文件的内容总和，包括长度的总和、文件数量的总和
   * 和文件目录的总和。
   * */
  public ContentSummary getContentSummary(Path f) throws IOException {
    FileStatus status = getFileStatus(f);
    if (status.isFile()) {
      // f is a file
      return new ContentSummary(status.getLen(), 1, 0);
    }
    // f is a directory
    long[] summary = {0, 0, 1};
    for(FileStatus s : listStatus(f)) {
      ContentSummary c = s.isDirectory() ? getContentSummary(s.getPath()) :
                                     new ContentSummary(s.getLen(), 1, 0);
      summary[0] += c.getLength();
      summary[1] += c.getFileCount();
      summary[2] += c.getDirectoryCount();
    }
    return new ContentSummary(summary[0], summary[1], summary[2]);
  }

  final private static PathFilter DEFAULT_FILTER = new PathFilter() {
      public boolean accept(Path file) {
        return true;
      }
    };

  /**
   * 列出指定目录下的所有文件的{@link FileStatus}对象
   *
   * @param f given path
   * @return the statuses of the files/directories in the given patch
   * @throws FileNotFoundException when the path does not exist;
   *         IOException see specific implementation
   */
  public abstract FileStatus[] listStatus(Path f) throws FileNotFoundException,
                                                         IOException;

  /*
   * Filter files/directories in the given path using the user-supplied path
   * filter. Results are added to the given array <code>results</code>.
   */
  private void listStatus(ArrayList<FileStatus> results, Path f,
      PathFilter filter) throws FileNotFoundException, IOException {
    FileStatus listing[] = listStatus(f);

    for (int i = 0; i < listing.length; i++) {
      if (filter.accept(listing[i].getPath())) {
        results.add(listing[i]);
      }
    }
  }

  /**
   * 根据用户提供的路径过滤，列出查找到的文件的状态
   * @param f
   *          a path name
   * @param filter
   *          the user-supplied path filter
   * @return an array of FileStatus objects for the files under the given path
   *         after applying the filter
   * @throws FileNotFoundException when the path does not exist;
   *         IOException see specific implementation
   */
  public FileStatus[] listStatus(Path f, PathFilter filter)
                                   throws FileNotFoundException, IOException {
    ArrayList<FileStatus> results = new ArrayList<FileStatus>();
    listStatus(results, f, filter);
    return results.toArray(new FileStatus[results.size()]);
  }

  /**
   * Filter files/directories in the given list of paths using default
   * path filter.
   *
   * @param files
   *          a list of paths
   * @return a list of statuses for the files under the given paths after
   *         applying the filter default Path filter
   * @throws FileNotFoundException when the path does not exist;
   *         IOException see specific implementation
   */
  public FileStatus[] listStatus(Path[] files)
      throws FileNotFoundException, IOException {
    return listStatus(files, DEFAULT_FILTER);
  }

  /**
   * Filter files/directories in the given list of paths using user-supplied
   * path filter.
   *
   * @param files
   *          a list of paths
   * @param filter
   *          the user-supplied path filter
   * @return a list of statuses for the files under the given paths after
   *         applying the filter
   * @throws FileNotFoundException when the path does not exist;
   *         IOException see specific implementation
   */
  public FileStatus[] listStatus(Path[] files, PathFilter filter)
      throws FileNotFoundException, IOException {
    ArrayList<FileStatus> results = new ArrayList<FileStatus>();
    for (int i = 0; i < files.length; i++) {
      listStatus(results, files[i], filter);
    }
    return results.toArray(new FileStatus[results.size()]);
  }

  /**
   * <p>Return all the files that match filePattern and are not checksum
   * files. Results are sorted by their names.
   *
   * <p>
   * A filename pattern is composed of <i>regular</i> characters and
   * <i>special pattern matching</i> characters, which are:
   *
   * <dl>
   *  <dd>
   *   <dl>
   *    <p>
   *    <dt> <tt> ? </tt>
   *    <dd> Matches any single character.
   *
   *    <p>
   *    <dt> <tt> * </tt>
   *    <dd> Matches zero or more characters.
   *
   *    <p>
   *    <dt> <tt> [<i>abc</i>] </tt>
   *    <dd> Matches a single character from character set
   *     <tt>{<i>a,b,c</i>}</tt>.
   *
   *    <p>
   *    <dt> <tt> [<i>a</i>-<i>b</i>] </tt>
   *    <dd> Matches a single character from the character range
   *     <tt>{<i>a...b</i>}</tt>.  Note that character <tt><i>a</i></tt> must be
   *     lexicographically less than or equal to character <tt><i>b</i></tt>.
   *
   *    <p>
   *    <dt> <tt> [^<i>a</i>] </tt>
   *    <dd> Matches a single character that is not from character set or range
   *     <tt>{<i>a</i>}</tt>.  Note that the <tt>^</tt> character must occur
   *     immediately to the right of the opening bracket.
   *
   *    <p>
   *    <dt> <tt> \<i>c</i> </tt>
   *    <dd> Removes (escapes) any special meaning of character <i>c</i>.
   *
   *    <p>
   *    <dt> <tt> {ab,cd} </tt>
   *    <dd> Matches a string from the string set <tt>{<i>ab, cd</i>} </tt>
   *
   *    <p>
   *    <dt> <tt> {ab,c{de,fh}} </tt>
   *    <dd> Matches a string from the string set <tt>{<i>ab, cde, cfh</i>}</tt>
   *
   *   </dl>
   *  </dd>
   * </dl>
   *
   * @param pathPattern a regular expression specifying a pth pattern

   * @return an array of paths that match the path pattern
   * @throws IOException
   */
  public FileStatus[] globStatus(Path pathPattern) throws IOException {
    return globStatus(pathPattern, DEFAULT_FILTER);
  }

  /**
   * 返回匹配路径模式并且满足用户提供的路径过滤的文件状态对象数组，
   * @param pathPattern
   *          a regular expression specifying the path pattern
   * @param filter
   *          a user-supplied path filter
   * @return an array of FileStatus objects
   * @throws IOException if any I/O error occurs when fetching file status
   */
  public FileStatus[] globStatus(Path pathPattern, PathFilter filter)
      throws IOException {
    String filename = pathPattern.toUri().getPath();
    List<String> filePatterns = GlobExpander.expand(filename);
    if (filePatterns.size() == 1) {
      return globStatusInternal(pathPattern, filter);
    } else {
      List<FileStatus> results = new ArrayList<FileStatus>();
      for (String filePattern : filePatterns) {
        FileStatus[] files = globStatusInternal(new Path(filePattern), filter);
        for (FileStatus file : files) {
          results.add(file);
        }
      }
      return results.toArray(new FileStatus[results.size()]);
    }
  }

  private FileStatus[] globStatusInternal(Path pathPattern, PathFilter filter)
      throws IOException {
    Path[] parents = new Path[1];
    int level = 0;
    String filename = pathPattern.toUri().getPath();

    // path has only zero component
    if ("".equals(filename) || Path.SEPARATOR.equals(filename)) {
      return getFileStatus(new Path[]{pathPattern});
    }

    // path has at least one component
    String[] components = filename.split(Path.SEPARATOR);
    // get the first component
    if (pathPattern.isAbsolute()) {
      parents[0] = new Path(Path.SEPARATOR);
      level = 1;
    } else {
      parents[0] = new Path(Path.CUR_DIR);
    }

    // glob the paths that match the parent path, i.e., [0, components.length-1]
    boolean[] hasGlob = new boolean[]{false};
    Path[] parentPaths = globPathsLevel(parents, components, level, hasGlob);
    FileStatus[] results;
    if (parentPaths == null || parentPaths.length == 0) {
      results = null;
    } else {
      // Now work on the last component of the path
      GlobFilter fp = new GlobFilter(components[components.length - 1], filter);
      if (fp.hasPattern()) { // last component has a pattern
        // list parent directories and then glob the results
        results = listStatus(parentPaths, fp);
        hasGlob[0] = true;
      } else { // last component does not have a pattern
        // get all the path names
        ArrayList<Path> filteredPaths = new ArrayList<Path>(parentPaths.length);
        for (int i = 0; i < parentPaths.length; i++) {
          parentPaths[i] = new Path(parentPaths[i],
            components[components.length - 1]);
          if (fp.accept(parentPaths[i])) {
            filteredPaths.add(parentPaths[i]);
          }
        }
        // get all their statuses
        results = getFileStatus(
            filteredPaths.toArray(new Path[filteredPaths.size()]));
      }
    }

    // Decide if the pathPattern contains a glob or not
    if (results == null) {
      if (hasGlob[0]) {
        results = new FileStatus[0];
      }
    } else {
      if (results.length == 0 ) {
        if (!hasGlob[0]) {
          results = null;
        }
      } else {
        Arrays.sort(results);
      }
    }
    return results;
  }

  /*
   * For a path of N components, return a list of paths that match the
   * components [<code>level</code>, <code>N-1</code>].
   */
  private Path[] globPathsLevel(Path[] parents, String[] filePattern,
      int level, boolean[] hasGlob) throws IOException {
    if (level == filePattern.length - 1)
      return parents;
    if (parents == null || parents.length == 0) {
      return null;
    }
    GlobFilter fp = new GlobFilter(filePattern[level]);
    if (fp.hasPattern()) {
      parents = FileUtil.stat2Paths(listStatus(parents, fp));
      hasGlob[0] = true;
    } else {
      for (int i = 0; i < parents.length; i++) {
        parents[i] = new Path(parents[i], filePattern[level]);
      }
    }
    return globPathsLevel(parents, filePattern, level + 1, hasGlob);
  }

  /* A class that could decide if a string matches the glob or not */
  private static class GlobFilter implements PathFilter {
    private PathFilter userFilter = DEFAULT_FILTER;
    private Pattern regex;
    private boolean hasPattern = false;

    /** Default pattern character: Escape any special meaning. */
    private static final char  PAT_ESCAPE = '\\';
    /** Default pattern character: Any single character. */
    private static final char  PAT_ANY = '.';
    /** Default pattern character: Character set close. */
    private static final char  PAT_SET_CLOSE = ']';

    GlobFilter(String filePattern) throws IOException {
      setRegex(filePattern);
    }

    GlobFilter(String filePattern, PathFilter filter) throws IOException {
      userFilter = filter;
      setRegex(filePattern);
    }

    private boolean isJavaRegexSpecialChar(char pChar) {
      return pChar == '.' || pChar == '$' || pChar == '(' || pChar == ')' ||
             pChar == '|' || pChar == '+';
    }
    void setRegex(String filePattern) throws IOException {
      int len;
      int setOpen;
      int curlyOpen;
      boolean setRange;

      StringBuilder fileRegex = new StringBuilder();

      // Validate the pattern
      len = filePattern.length();
      if (len == 0)
        return;

      setOpen = 0;
      setRange = false;
      curlyOpen = 0;

      for (int i = 0; i < len; i++) {
        char pCh;

        // Examine a single pattern character
        pCh = filePattern.charAt(i);
        if (pCh == PAT_ESCAPE) {
          fileRegex.append(pCh);
          i++;
          if (i >= len)
            error("An escaped character does not present", filePattern, i);
          pCh = filePattern.charAt(i);
        } else if (isJavaRegexSpecialChar(pCh)) {
          fileRegex.append(PAT_ESCAPE);
        } else if (pCh == '*') {
          fileRegex.append(PAT_ANY);
          hasPattern = true;
        } else if (pCh == '?') {
          pCh = PAT_ANY;
          hasPattern = true;
        } else if (pCh == '{') {
          fileRegex.append('(');
          pCh = '(';
          curlyOpen++;
          hasPattern = true;
        } else if (pCh == ',' && curlyOpen > 0) {
          fileRegex.append(")|");
          pCh = '(';
        } else if (pCh == '}' && curlyOpen > 0) {
          // End of a group
          curlyOpen--;
          fileRegex.append(")");
          pCh = ')';
        } else if (pCh == '[' && setOpen == 0) {
          setOpen++;
          hasPattern = true;
        } else if (pCh == '^' && setOpen > 0) {
        } else if (pCh == '-' && setOpen > 0) {
          // Character set range
          setRange = true;
        } else if (pCh == PAT_SET_CLOSE && setRange) {
          // Incomplete character set range
          error("Incomplete character set range", filePattern, i);
        } else if (pCh == PAT_SET_CLOSE && setOpen > 0) {
          // End of a character set
          if (setOpen < 2)
            error("Unexpected end of set", filePattern, i);
          setOpen = 0;
        } else if (setOpen > 0) {
          // Normal character, or the end of a character set range
          setOpen++;
          setRange = false;
        }
        fileRegex.append(pCh);
      }

      // Check for a well-formed pattern
      if (setOpen > 0 || setRange || curlyOpen > 0) {
        // Incomplete character set or character range
        error("Expecting set closure character or end of range, or }",
            filePattern, len);
      }
      regex = Pattern.compile(fileRegex.toString());
    }

    boolean hasPattern() {
      return hasPattern;
    }

    public boolean accept(Path path) {
      return regex.matcher(path.getName()).matches() && userFilter.accept(path);
    }

    private void error(String s, String pattern, int pos) throws IOException {
      throw new IOException("Illegal file pattern: "
                            +s+ " for glob "+ pattern + " at " + pos);
    }
  }

  /**
   * 返回当前用户在这个文件系统中的home目录
   * 默认返回"/user/$USER/"
   */
  public Path getHomeDirectory() {
    return this.makeQualified(
        new Path("/user/"+System.getProperty("user.name")));
  }


  /**
   * 设置工作目录
   * @param new_dir
   */
  public abstract void setWorkingDirectory(Path new_dir);

  /**
   * 返回工作目录
   * Get the current working directory for the given file system
   * @return the directory pathname
   */
  public abstract Path getWorkingDirectory();


  /**
   * 获取初始的工作目录.
   *
   * @return if there is built in notion of workingDir then it
   * is returned; else a null is returned.
   */
  protected Path getInitialWorkingDirectory() {
    return null;
  }

  /**
   * Call {@link #mkdirs(Path, FsPermission)} with default permission.
   */
  public boolean mkdirs(Path f) throws IOException {
    return mkdirs(f, FsPermission.getDefault());
  }

  /**
   * 根据路径和权限创建目录，功能上类似<code>mkdir -p</code>
   */
  public abstract boolean mkdirs(Path f, FsPermission permission
      ) throws IOException;

  /**
   * 将本地文件复制到该{@link FileSystem}
   */
  public void copyFromLocalFile(Path src, Path dst)
    throws IOException {
    copyFromLocalFile(false, src, dst);
  }

  /**
   * 将本地文件移动到该{@link FileSystem}，源文件在被移动后会被删除。
   */
  public void moveFromLocalFile(Path[] srcs, Path dst)
    throws IOException {
    copyFromLocalFile(true, true, srcs, dst);
  }

  /**
   * The src file is on the local disk.  Add it to FS at
   * the given dst name, removing the source afterwards.
   */
  public void moveFromLocalFile(Path src, Path dst)
    throws IOException {
    copyFromLocalFile(true, src, dst);
  }

  /**
   * The src file is on the local disk.  Add it to FS at
   * the given dst name.
   * delSrc indicates if the source should be removed
   */
  public void copyFromLocalFile(boolean delSrc, Path src, Path dst)
    throws IOException {
    copyFromLocalFile(delSrc, true, src, dst);
  }

  /**
   * The src files are on the local disk.  Add it to FS at
   * the given dst name.
   * delSrc indicates if the source should be removed
   */
  public void copyFromLocalFile(boolean delSrc, boolean overwrite,
                                Path[] srcs, Path dst)
    throws IOException {
    Configuration conf = getConf();
    FileUtil.copy(getLocal(conf), srcs, this, dst, delSrc, overwrite, conf);
  }

  /**
   * The src file is on the local disk.  Add it to FS at
   * the given dst name.
   * delSrc indicates if the source should be removed
   */
  public void copyFromLocalFile(boolean delSrc, boolean overwrite,
                                Path src, Path dst)
    throws IOException {
    Configuration conf = getConf();
    FileUtil.copy(getLocal(conf), src, this, dst, delSrc, overwrite, conf);
  }

  /**
   * 将{@link FileSystem}中的文件复制到本地文件系统，源文件在被移动后会被删除。
   */
  public void copyToLocalFile(Path src, Path dst) throws IOException {
    copyToLocalFile(false, src, dst);
  }

  /**
   * 将{@link FileSystem}中的文件移动到本地文件系统，源文件在被移动后会被删除。
   */
  public void moveToLocalFile(Path src, Path dst) throws IOException {
    copyToLocalFile(true, src, dst);
  }

  /**
   * The src file is under FS, and the dst is on the local disk.
   * Copy it from FS control to the local dst name.
   * delSrc indicates if the src will be removed or not.
   */
  public void copyToLocalFile(boolean delSrc, Path src, Path dst)
    throws IOException {
    FileUtil.copy(this, src, getLocal(getConf()), dst, delSrc, getConf());
  }

  /**
   *
   * 返回一个本地文件，用户可以向此文件进行输出，而输出内容则会同步到
   * 目标文件系统
   *
   */
  public Path startLocalOutput(Path fsOutputFile, Path tmpLocalFile)
    throws IOException {
    return tmpLocalFile;
  }

  /**
   * Called when we're all done writing to the target.  A local FS will
   * do nothing, because we've written to exactly the right place.  A remote
   * FS will copy the contents of tmpLocalFile to the correct target at
   * fsOutputFile.
   * 完成本地文件输出。对于本地文件系统，此操作无用。但是对于远程文件系统，此操作会将
   * tmpLocalFile拷贝到远程系统。
   */
  public void completeLocalOutput(Path fsOutputFile, Path tmpLocalFile)
    throws IOException {
    moveFromLocalFile(tmpLocalFile, fsOutputFile);
  }

  /**
   * 关闭此文件系统。
   */
  public void close() throws IOException {
    // delete all files that were marked as delete-on-exit.
    processDeleteOnExit();
    CACHE.remove(this.key, this);
  }

  /**
   * 返回此文件系统的所有文件的大小。
   */
  public long getUsed() throws IOException{
    long used = 0;
    FileStatus[] files = listStatus(new Path("/"));
    for(FileStatus file:files){
      used += file.getLen();
    }
    return used;
  }

  /**
   * 获取指定文件大小
   * @return the number of bytes in a block
   */
  @Deprecated
  public long getBlockSize(Path f) throws IOException {
    return getFileStatus(f).getBlockSize();
  }

  /**
   * 获得默认的块大小。
   * */
  public long getDefaultBlockSize() {
    // default to 32MB: large enough to minimize the impact of seeks
    return getConf().getLong("fs.local.block.size", 32 * 1024 * 1024);
  }

  /**
   * Get the default replication.
   * 获得默认的文件副本数量。
   */
  public short getDefaultReplication() { return 1; }

  /**
   * 给定目录的{@link FileStatus}
   * @param f The path we want information from
   * @return a FileStatus object
   * @throws FileNotFoundException when the path does not exist;
   *         IOException see specific implementation
   */
  public abstract FileStatus getFileStatus(Path f) throws IOException;

  /**
   * 获得文件的校验和
   */
  public FileChecksum getFileChecksum(Path f) throws IOException {
    return null;
  }

  /**
   * 设置是否确认检验和的布尔标识
   * @param verifyChecksum
   */
  public void setVerifyChecksum(boolean verifyChecksum) {
    //doesn't do anything
  }


  private FileStatus[] getFileStatus(Path[] paths) throws IOException {
    if (paths == null) {
      return null;
    }
    ArrayList<FileStatus> results = new ArrayList<FileStatus>(paths.length);
    for (int i = 0; i < paths.length; i++) {
      try {
        results.add(getFileStatus(paths[i]));
      } catch (FileNotFoundException e) { // do nothing
      }
    }
    return results.toArray(new FileStatus[results.size()]);
  }

  /**
   *
   * @return a FsStatus object
   * @throws IOException
   *           see specific implementation
   */
  public FsStatus getStatus() throws IOException {
    return getStatus(null);
  }

  /**
   * 该方法返回一个指定路径所在的描述文件系统的已用量和容量的状态对象。
   * @param p Path for which status should be obtained. null means
   * the default partition.
   * @return a FsStatus object
   * @throws IOException
   *           see specific implementation
   */
  public FsStatus getStatus(Path p) throws IOException {
    return new FsStatus(Long.MAX_VALUE, 0, Long.MAX_VALUE);
  }

  /**
   * 设置路径的权限
   * Set permission of a path.
   * @param p
   * @param permission
   */
  public void setPermission(Path p, FsPermission permission
      ) throws IOException {
  }

  /**
   * 设置路径的拥有者
   * @param p The path
   * @param username If it is null, the original username remains unchanged.
   * @param groupname If it is null, the original groupname remains unchanged.
   */
  public void setOwner(Path p, String username, String groupname
      ) throws IOException {
  }

  /**
   * 设置文件的访问时间
   * @param p The path
   * @param mtime Set the modification time of this file.
   *              The number of milliseconds since Jan 1, 1970.
   *              A value of -1 means that this call should not set modification time.
   * @param atime Set the access time of this file.
   *              The number of milliseconds since Jan 1, 1970.
   *              A value of -1 means that this call should not set access time.
   */
  public void setTimes(Path p, long mtime, long atime
      ) throws IOException {
  }

  private static FileSystem createFileSystem(URI uri, Configuration conf
      ) throws IOException {
    Class<?> clazz = conf.getClass("fs." + uri.getScheme() + ".impl", null);
    if (clazz == null) {
      throw new IOException("No FileSystem for scheme: " + uri.getScheme());
    }
    FileSystem fs = (FileSystem)ReflectionUtils.newInstance(clazz, conf);
    fs.initialize(uri, conf);
    return fs;
  }

  /**
   * 缓存文件系统对象的静态内部类
   * 1.主要记录了Key和文件系统的映射，以及Key的集合
   * Key是Cache的内部类
   * 主要记录了模式信息，授权信息以及用户组信息
   * 2.其主要方法包括获取、删除、关闭Key所对应的文件系统实例
   * */
  static class Cache {
    private final ClientFinalizer clientFinalizer = new ClientFinalizer();

    private final Map<Key, FileSystem> map = new HashMap<Key, FileSystem>();
    private final Set<Key> toAutoClose = new HashSet<Key>();

    /** A variable that makes all objects in the cache unique */
    private static AtomicLong unique = new AtomicLong(1);

    FileSystem get(URI uri, Configuration conf) throws IOException{
      Key key = new Key(uri, conf);
      return getInternal(uri, conf, key);
    }

    /** The objects inserted into the cache using this method are all unique */
    FileSystem getUnique(URI uri, Configuration conf) throws IOException{
      Key key = new Key(uri, conf, unique.getAndIncrement());
      return getInternal(uri, conf, key);
    }

    private FileSystem getInternal(URI uri, Configuration conf, Key key) throws IOException{
      FileSystem fs;
      synchronized (this) {
        fs = map.get(key);
      }
      if (fs != null) {
        return fs;
      }

      fs = createFileSystem(uri, conf);
      synchronized (this) { // refetch the lock again
        FileSystem oldfs = map.get(key);
        if (oldfs != null) { // a file system is created while lock is releasing
          fs.close(); // close the new file system
          return oldfs;  // return the old file system
        }

        // now insert the new file system into the map
        if (map.isEmpty() && !clientFinalizer.isAlive()) {
          Runtime.getRuntime().addShutdownHook(clientFinalizer);
        }
        fs.key = key;
        map.put(key, fs);
        if (conf.getBoolean("fs.automatic.close", true)) {
          toAutoClose.add(key);
        }
        return fs;
      }
    }

    synchronized void remove(Key key, FileSystem fs) {
      if (map.containsKey(key) && fs == map.get(key)) {
        map.remove(key);
        toAutoClose.remove(key);
        if (map.isEmpty() && !clientFinalizer.isAlive()) {
          if (!Runtime.getRuntime().removeShutdownHook(clientFinalizer)) {
            LOG.info("Could not cancel cleanup thread, though no " +
                     "FileSystems are open");
          }
        }
      }
    }

    synchronized void closeAll() throws IOException {
      closeAll(false);
    }

    /**
     * Close all FileSystem instances in the Cache.
     * @param onlyAutomatic only close those that are marked for automatic closing
     */
    synchronized void closeAll(boolean onlyAutomatic) throws IOException {
      List<IOException> exceptions = new ArrayList<IOException>();

      // Make a copy of the keys in the map since we'll be modifying
      // the map while iterating over it, which isn't safe.
      List<Key> keys = new ArrayList<Key>();
      keys.addAll(map.keySet());

      for (Key key : keys) {
        final FileSystem fs = map.get(key);

        if (onlyAutomatic && !toAutoClose.contains(key)) {
          continue;
        }

        //remove from cache
        remove(key, fs);

        if (fs != null) {
          try {
            fs.close();
          }
          catch(IOException ioe) {
            exceptions.add(ioe);
          }
        }
      }

      if (!exceptions.isEmpty()) {
        throw MultipleIOException.createIOException(exceptions);
      }
    }


    /**
     * ClientFinalizer类继承了Thread类
     * 当Java虚拟机停止运行时,该线程才会启动
     * 调用run进关闭清理工作
     */
    private class ClientFinalizer extends Thread {
      public synchronized void run() {
        try {
          closeAll(true);
        } catch (IOException e) {
          LOG.info("FileSystem.Cache.closeAll() threw an exception:\n" + e);
        }
      }
    }

    /** FileSystem.Cache.Key*/
    static class Key {
      final String scheme;
      final String authority;
      final UserGroupInformation ugi;
      final long unique;   // an artificial way to make a key unique

      Key(URI uri, Configuration conf) throws IOException {
        this(uri, conf, 0);
      }

      Key(URI uri, Configuration conf, long unique) throws IOException {
        scheme = uri.getScheme()==null?"":uri.getScheme().toLowerCase();
        authority = uri.getAuthority()==null?"":uri.getAuthority().toLowerCase();
        this.unique = unique;

        this.ugi = UserGroupInformation.getCurrentUser();
      }

      /** {@inheritDoc} */
      public int hashCode() {
        return (scheme + authority).hashCode() + ugi.hashCode() + (int)unique;
      }

      static boolean isEqual(Object a, Object b) {
        return a == b || (a != null && a.equals(b));
      }

      /** {@inheritDoc} */
      public boolean equals(Object obj) {
        if (obj == this) {
          return true;
        }
        if (obj != null && obj instanceof Key) {
          Key that = (Key)obj;
          return isEqual(this.scheme, that.scheme)
                 && isEqual(this.authority, that.authority)
                 && isEqual(this.ugi, that.ugi)
                 && (this.unique == that.unique);
        }
        return false;
      }

      /** {@inheritDoc} */
      public String toString() {
        return "("+ugi.toString() + ")@" + scheme + "://" + authority;
      }
    }
  }

  /**
   * 统计信息——静态内部类
   * 主要包含了URI的模式信息，被用于AbstractFileSystem和link FileSystem
   * URI格式是scheme://authority/path
   * 对HDFS文件系统，scheme是hdfs;对本地文件系统，scheme是file
   * 该类包含了对读写的字节数、读取的操作次数内容进行的记录
   */
  public static final class Statistics {
    private final String scheme;
    private AtomicLong bytesRead = new AtomicLong();
    private AtomicLong bytesWritten = new AtomicLong();

    public Statistics(String scheme) {
      this.scheme = scheme;
    }

    /**
     * Increment the bytes read in the statistics
     * @param newBytes the additional bytes read
     */
    public void incrementBytesRead(long newBytes) {
      bytesRead.getAndAdd(newBytes);
    }

    /**
     * Increment the bytes written in the statistics
     * @param newBytes the additional bytes written
     */
    public void incrementBytesWritten(long newBytes) {
      bytesWritten.getAndAdd(newBytes);
    }

    /**
     * Get the total number of bytes read
     * @return the number of bytes
     */
    public long getBytesRead() {
      return bytesRead.get();
    }

    /**
     * Get the total number of bytes written
     * @return the number of bytes
     */
    public long getBytesWritten() {
      return bytesWritten.get();
    }

    public String toString() {
      return bytesRead + " bytes read and " + bytesWritten +
             " bytes written";
    }

    /**
     * Reset the counts of bytes to 0.
     */
    public void reset() {
      bytesWritten.set(0);
      bytesRead.set(0);
    }

    /**
     * Get the uri scheme associated with this statistics object.
     * @return the schema associated with this set of statistics
     */
    public String getScheme() {
      return scheme;
    }
  }

  /**
   * 获取一个Map对象，该对象键为文件的scheme，值为统计对象。
   * @return a Map having a key as URI scheme and value as Statistics object
   * @deprecated use {@link #getAllStatistics} instead
   */
  @Deprecated
  public static synchronized Map<String, Statistics> getStatistics() {
    Map<String, Statistics> result = new HashMap<String, Statistics>();
    for(Statistics stat: statisticsTable.values()) {
      result.put(stat.getScheme(), stat);
    }
    return result;
  }

  /**
   * 返回所有文件系统的统计对象。
   */
  public static synchronized List<Statistics> getAllStatistics() {
    return new ArrayList<Statistics>(statisticsTable.values());
  }

  /**
   * 获取指定文件系统的统计对象。
   * @param cls the class to lookup
   * @return a statistics object
   */
  public static synchronized
  Statistics getStatistics(String scheme, Class<? extends FileSystem> cls) {
    Statistics result = statisticsTable.get(cls);
    if (result == null) {
      result = new Statistics(scheme);
      statisticsTable.put(cls, result);
    }
    return result;
  }

  public static synchronized void clearStatistics() {
    for(Statistics stat: statisticsTable.values()) {
      stat.reset();
    }
  }

  public static synchronized
  void printStatistics() throws IOException {
    for (Map.Entry<Class<? extends FileSystem>, Statistics> pair:
            statisticsTable.entrySet()) {
      System.out.println("  FileSystem " + pair.getKey().getName() +
                         ": " + pair.getValue());
    }
  }
}
