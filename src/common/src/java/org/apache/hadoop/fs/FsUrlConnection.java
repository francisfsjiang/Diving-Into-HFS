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
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLConnection;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;

/**
 * URL连接到InputStreams上的类
 * 具有Configuration对象conf和InputStream对象is作为实例属性
 * 继承自URLConnection的connect方法和getInputStream方法,并Override覆盖其原来方法
 * 构造函数接收两个参数,第一个参数是Configuration conf.
 * 第二个参数是URL对象url,调用其超类URLConnection构造函数super(url)
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
class FsUrlConnection extends URLConnection {

  private Configuration conf;

  private InputStream is;

  FsUrlConnection(Configuration conf, URL url) {
    super(url);
    this.conf = conf;
  }
/**
  * @throw IOException
  * connect方法是继承自URLConnection,在FsUrlConnection类中Override的方法
  * 该方法通过FileSystem.get(url.toURI(), conf)方法获得FileSystem对象fs
  * 并用fs打开url的地址获得的输入流对象赋值给FsUrlConnection类的实例变量is
  */
  @Override
  public void connect() throws IOException {
    try {
      FileSystem fs = FileSystem.get(url.toURI(), conf);
      is = fs.open(new Path(url.getPath()));
    } catch (URISyntaxException e) {
      throw new IOException(e.toString());
    }
  }
/**
  * @throw IOException
  * @return InputStream对象 
  * getInputStream()方法是继承自URLConnection,在FsUrlConnection类中Override的方法
  * 该方法首先判断FsUrlConnection类的实例变量is是否为null,若为null则调用connect方法
  */
  /* @inheritDoc */
  @Override
  public InputStream getInputStream() throws IOException {
    if (is == null) {
      connect();
    }
    return is;
  }

}
