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

import java.net.URLStreamHandlerFactory;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;

/**
 * FsUrlStreamHandlerFactory是由多个URL流输入输出处理器组成的工厂方法,
 * 实现了URLStreamHandlerFactory接口,
 * 拥有三个私有的成员变量: Configuration对象conf,
 * HashMap<String, Boolean>对象 protocols,
 * 还有private java.net.URLStreamHandler对象handler.
 * 在众多的类中只有一个处理器的工作是生成UrlConnections对象,
 * UrlConnections类依赖于FileSystem并选择合适的接口进行实现.
 * 在createURLStreamHandler方法中返回handler之前,
 * 需要FileSystem类的实现接口中明确清楚传入需要的参数
 * @author benco
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class FsUrlStreamHandlerFactory implements
    URLStreamHandlerFactory {

  // Configuration对象conf中设置了支持FS的接口类的名称
  private Configuration conf;

  // 字典对象存的是这个protocol键是否在FileSystem中,在的则值存入true,不在则值存入false
  private Map<String, Boolean> protocols = new HashMap<String, Boolean>();

  // URLStream 处理器
  private java.net.URLStreamHandler handler;
/**
  * FsUrlStreamHandlerFactory第一个构造函数
  * conf引用到一个新创建的Configuration类对象中
  * 确定了conf的值后调用FsUrlStreamHandler
  * 并将返回值赋值给handler
  */
  public FsUrlStreamHandlerFactory() {
    this.conf = new Configuration();
    // force the resolution of the configuration files
    // this is required if we want the factory to be able to handle
    // file:// URLs
    this.conf.getClass("fs.file.impl", null);
    this.handler = new FsUrlStreamHandler(this.conf);
  }
/**
  * FsUrlStreamHandlerFactory第二个构造函数
  * conf引用到一个配置好的Configuration类对象中
  * 确定了conf的值后调用FsUrlStreamHandler
  * 并将返回值赋值给handler
  */
  public FsUrlStreamHandlerFactory(Configuration conf) {
    this.conf = new Configuration(conf);
    // force the resolution of the configuration files
    this.conf.getClass("fs.file.impl", null);
    this.handler = new FsUrlStreamHandler(this.conf);
  }
/**
  * @param String protocol
  * @return java.net.URLStreamHandler 返回一个handler
  * FsUrlStreamHandlerFactory第二个构造函数
  * createURLStreamHandler接收一个String参数protocol
  * 判断protocols字典中是否存在这个protocol
  * 若存在则将handler变量返回,不然则返回null
  */
  public java.net.URLStreamHandler createURLStreamHandler(String protocol) {
    if (!protocols.containsKey(protocol)) {
      boolean known =
          (conf.getClass("fs." + protocol + ".impl", null) != null);
      protocols.put(protocol, known);
    }
    if (protocols.get(protocol)) {
      return handler;
    } else {
      // FileSystem does not know the protocol, let the VM handle this
      return null;
    }
  }

}
