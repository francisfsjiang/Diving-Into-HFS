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
package org.apache.hadoop.fs.local;

import java.io.IOException;
import java.io.FileNotFoundException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.DelegateToFileSystem;
import org.apache.hadoop.fs.FsConstants;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Shell;

@InterfaceAudience.Private
@InterfaceStability.Evolving /*Evolving for a release,to be changed to Stable */

/**
 * 该类继承于AbstractFileSystem分之下，对应
 * FileSystem分之下的原生本地文件系统
 */
public class RawLocalFs extends DelegateToFileSystem {

  RawLocalFs(final Configuration conf) throws IOException, URISyntaxException {
    this(FsConstants.LOCAL_FS_URI, conf);
  }
  
  RawLocalFs(final URI theUri, final Configuration conf) throws IOException,
      URISyntaxException {
    super(theUri, new RawLocalFileSystem(), conf, 
        FsConstants.LOCAL_FS_URI.getScheme(), false);
  }

  /**
   * 返回URI默认端口
   */
  @Override
  protected int getUriDefaultPort() {
    return -1;
  }

  /**
   * 返回默认服务器
   */
  @Override
  protected FsServerDefaults getServerDefaults() throws IOException {
    return LocalConfigKeys.getServerDefaults();
  }
  
  @Override
  protected boolean supportsSymlinks() {
    return true;
  }

  /**
   * 建立符号连接
   * 符号链接一般用于将一个文件或这个目录结构移动到系统中的另一个位置
   */
  @Override
  protected void createSymlink(Path target, Path link, boolean createParent) 
      throws IOException {
    final String targetScheme = target.toUri().getScheme();
    if (targetScheme != null && !"file".equals(targetScheme)) {
      throw new IOException("Unable to create symlink to non-local file "+
                            "system: "+target.toString());
    }
    if (createParent) {
      mkdir(link.getParent(), FsPermission.getDefault(), true);
    }
    try {
      Shell.execCommand(Shell.LINK_COMMAND, "-s",
                        new URI(target.toString()).getPath(),
                        new URI(link.toString()).getPath());
    } catch (URISyntaxException x) {
      throw new IOException("Invalid symlink path: "+x.getMessage());
    } catch (IOException x) {
      throw new IOException("Unable to create symlink: "+x.getMessage());
    }
  }

  /**
   * 读取建立的符号连接
   */
  private String readLink(Path p) {
    try {
      final String path = p.toUri().getPath();
      return Shell.execCommand(Shell.READ_LINK_COMMAND, path).trim(); 
    } catch (IOException x) {
      return "";
    }
  }
  
  /**
   *
   * 获取文件连接状态
   */
  @Override
  protected FileStatus getFileLinkStatus(final Path f) throws IOException {
    String target = readLink(f);
    try {
      FileStatus fs = getFileStatus(f);  
      if ("".equals(target)) {
        return fs;
      }
      return new FileStatus(fs.getLen(), 
          false,
          fs.getReplication(), 
          fs.getBlockSize(),
          fs.getModificationTime(),
          fs.getAccessTime(),
          fs.getPermission(),
          fs.getOwner(),
          fs.getGroup(),
          new Path(target),
          f);
    } catch (FileNotFoundException e) {
      if (!"".equals(target)) {
        return new FileStatus(0, false, 0, 0, 0, 0, FsPermission.getDefault(), 
            "", "", new Path(target), f);        
      }
      throw e;
    }
  }


  /**
   * 获取连接目标
   */
  @Override
  protected Path getLinkTarget(Path f) throws IOException {

    throw new AssertionError();
  }
}
