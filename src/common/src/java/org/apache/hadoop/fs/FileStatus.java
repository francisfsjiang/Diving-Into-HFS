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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/** Interface that represents the client side information for a file.
 * 该类抽象提供了文件信息的抽象，屏蔽了具体文件系统的具体实现。
 * 文件的状态信息具体包含有文件路径、文件长度、是否是目录、复制块大小、
 * 块大小、修改时间、访问时间、权限、拥有者、所属的组、符号链接等
 * @author neveralso
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class FileStatus implements Writable, Comparable {

  private Path path;
  private long length;
  private boolean isdir;
  private short block_replication;
  private long blocksize;
  private long modification_time;
  private long access_time;
  private FsPermission permission;
  private String owner;
  private String group;
  private Path symlink;
  
  public FileStatus() { this(0, false, 0, 0, 0, 0, null, null, null, null); }

  /**
   * 构造函数的另一个版本，包装了主要的构造函数来实现，即将被抛弃
   */
  public FileStatus(long length, boolean isdir, int block_replication,
                    long blocksize, long modification_time, Path path) {

    this(length, isdir, block_replication, blocksize, modification_time,
         0, null, null, null, path);
  }

  /**
   * 构造函数的另一个版本，包装了主要的构造函数来实现
   */
  public FileStatus(long length, boolean isdir,
                    int block_replication,
                    long blocksize, long modification_time, long access_time,
                    FsPermission permission, String owner, String group, 
                    Path path) {
    this(length, isdir, block_replication, blocksize, modification_time,
         access_time, permission, owner, group, null, path);
  }

  /**
   * 构造函数，设置了此文件的基本参数
   * @param length
   * @param isdir
   * @param block_replication
   * @param blocksize
   * @param modification_time
   * @param access_time
   * @param permission
   * @param owner
   * @param group
   * @param symlink
   * @param path
   */
  public FileStatus(long length, boolean isdir,
                    int block_replication,
                    long blocksize, long modification_time, long access_time,
                    FsPermission permission, String owner, String group, 
                    Path symlink,
                    Path path) {
    this.length = length;
    this.isdir = isdir;
    this.block_replication = (short)block_replication;
    this.blocksize = blocksize;
    this.modification_time = modification_time;
    this.access_time = access_time;
    this.permission = (permission == null) ? 
                      FsPermission.getDefault() : permission;
    this.owner = (owner == null) ? "" : owner;
    this.group = (group == null) ? "" : group;
    this.symlink = symlink;
    this.path = path;
  }

  /**
   * 返回文件的字节长度
   * @return 文件的字节长度
   */
  public long getLen() {
    return length;
  }

  /**
   * 判断此文件是否为文件，以区别文件目录和文件链接
   * @return true if this is a file
   */
  public boolean isFile() {
    return !isdir && !isSymlink();
  }

  /**
   * 判断是否为文件目录
   * @return true if this is a directory
   */
  public boolean isDirectory() {
    return isdir;
  }
  
  /**
   * 判断是否为文件目录，此方法将被isDirectory代替。
   */
  @Deprecated
  public boolean isDir() {
    return isdir;
  }
  
  /**
   * 判断是否为文件链接
   */
  public boolean isSymlink() {
    return symlink != null;
  }

  /**
   * 返回文件的块大小
   */
  public long getBlockSize() {
    return blocksize;
  }

  /**
   * 返回文件的副本数量
   */
  public short getReplication() {
    return block_replication;
  }

  /**
   * 返回文件的修改日期
   */
  public long getModificationTime() {
    return modification_time;
  }

  /**
   * 返回文件的创建时间
   */
  public long getAccessTime() {
    return access_time;
  }

  /**
   * 取得文件的权限，如果文件系统不支持权限，则返回 777(rwxrwxrwx)
   */
  public FsPermission getPermission() {
    return permission;
  }
  
  /**
   * 返回文件的所有者，如果文件系统不支持所有者，则返回null
   */
  public String getOwner() {
    return owner;
  }
  
  /**
   * 返回文件所在的工作组，如果文件系统不支持工作组，则是未定义行为
   */
  public String getGroup() {
    return group;
  }

  /**
   * 返回文件的路径
   * @return
   */
  public Path getPath() {
    return path;
  }

  /**
   * 设置文件的路径
   * @param p
   */
  public void setPath(final Path p) {
    path = p;
  }

  /* These are provided so that these values could be loaded lazily 
   * by a filesystem (e.g. local file system).
   */
  
  /**
   * 设置文件权限
   */
  protected void setPermission(FsPermission permission) {
    this.permission = (permission == null) ? 
                      FsPermission.getDefault() : permission;
  }
  
  /**
   * 设置文件所有者，默认为""
   */  
  protected void setOwner(String owner) {
    this.owner = (owner == null) ? "" : owner;
  }
  
  /**
   * 设置文件所在工作组，默认为""
   */  
  protected void setGroup(String group) {
    this.group = (group == null) ? "" :  group;
  }

  /**
   * 如果此文件是链接，则获取该链接
   * @return The contents of the symbolic link.
   */
  public Path getSymlink() throws IOException {
    if (!isSymlink()) {
      throw new IOException("Path " + path + " is not a symbolic link");
    }
    return symlink;
  }

  /**
   * 设置文件链接
   * @param p
   */
  public void setSymlink(final Path p) {
    symlink = p;
  }
  
  //////////////////////////////////////////////////
  // Writable
  /////////////////////////////////////////////////

  /**
   * 向指定的流输出文件的基本信息
   */
  public void write(DataOutput out) throws IOException {
    Text.writeString(out, getPath().toString());
    out.writeLong(length);
    out.writeBoolean(isdir);
    out.writeShort(block_replication);
    out.writeLong(blocksize);
    out.writeLong(modification_time);
    out.writeLong(access_time);
    permission.write(out);
    Text.writeString(out, owner);
    Text.writeString(out, group);
    out.writeBoolean(isSymlink());
    if (isSymlink()) {
      Text.writeString(out, symlink.toString());
    }
  }
  /**
   * 从给定的流读取并设置该文件的属性
   */
  public void readFields(DataInput in) throws IOException {
    String strPath = Text.readString(in);
    this.path = new Path(strPath);
    this.length = in.readLong();
    this.isdir = in.readBoolean();
    this.block_replication = in.readShort();
    blocksize = in.readLong();
    modification_time = in.readLong();
    access_time = in.readLong();
    permission.readFields(in);
    owner = Text.readString(in);
    group = Text.readString(in);
    if (in.readBoolean()) {
      this.symlink = new Path(Text.readString(in));
    } else {
      this.symlink = null;
    }
  }

  /**
   * 比较函数，通过比较路径{@link org.apache.hadoop.fs.Path#compareTo{Object}，
   * 比较该文件和给定的文件
   * 如果此文件比给定的文件小，则返回一个负数
   * 相等则返回0
   * 如果此文件比给定的文件大，则返回一个正数
   */
  public int compareTo(Object o) {
    FileStatus other = (FileStatus)o;
    return this.getPath().compareTo(other.getPath());
  }
  
  /**
   * 通过判断路径是否相等来判断此文件和给定的文件是否相同
   */
  public boolean equals(Object o) {
    if (o == null) {
      return false;
    }
    if (this == o) {
      return true;
    }
    if (!(o instanceof FileStatus)) {
      return false;
    }
    FileStatus other = (FileStatus)o;
    return this.getPath().equals(other.getPath());
  }
  
  /**
   * 返回此文件的hash码，即文件的路径的hash码
   *
   */
  public int hashCode() {
    return getPath().hashCode();
  }
}
