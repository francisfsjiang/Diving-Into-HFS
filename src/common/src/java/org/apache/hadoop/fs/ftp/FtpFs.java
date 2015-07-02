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

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.commons.net.ftp.FTP;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.AbstractFileSystem;
import org.apache.hadoop.fs.DelegateToFileSystem;
import org.apache.hadoop.fs.FsConstants;
import org.apache.hadoop.fs.FsServerDefaults;

/**
 * FTPFs是AbstractFileSystem以抽象的形式向Hadoop FS中具体的文件系统提供的实现接口
 * DelegateToFileSystem是代理类
 * FTPFs通过继承DelegateToFileSystem类,为旧式的文件系统提供了代理接口
 * @author benco
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving /*Evolving for a release,to be changed to Stable */
public class FtpFs extends DelegateToFileSystem {
 /**
   * 构造函数需要链接到上层抽象类AbstractFileSystem的URL和Configuration
   * {@link AbstractFileSystem#createFileSystem(URI, Configuration)}.
   *
   * @param theUri which must be that of localFs 本地文件系统
   * @param conf Configuration对象
   * @throws IOException 
   * @throws URISyntaxException AbstractFileSystem通过唯一能标识URI,抛出URI格式错误
   */
  FtpFs(final URI theUri, final Configuration conf) throws IOException
      URISyntaxException {
    super(theUri, new FTPFileSystem(), conf, FsConstants.FTP_SCHEME, true);
  }
  
/**
  * @throws IOException 
  * @return 返回FTP的默认端口
  */
  @Override
  protected int getUriDefaultPort() {
    return FTP.DEFAULT_PORT;
  }

/**
  * @throws IOException 
  * @return 返回FtpConfigKeys类的getServerDefaults()方法的返回值,返回值是一个FsServerDefaults对象.
  */
  @Override
  protected FsServerDefaults getServerDefaults() throws IOException {
    return FtpConfigKeys.getServerDefaults();
  }
}
