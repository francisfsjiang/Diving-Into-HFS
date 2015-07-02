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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableFactory;
/**
 * 块位置类：
 * 域包含主机号、端口号、拓扑路径以及偏移量和长度信息
 * 主要方法为域的get、set方法和实现Writable接口的读写方法
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class BlockLocation implements Writable {

  static {               // register a ctor
    WritableFactories.setFactory
      (BlockLocation.class,
       new WritableFactory() {
         public Writable newInstance() { return new BlockLocation(); }
       });
  }

  private String[] hosts; //hostnames of datanodes
  private String[] names; //hostname:portNumber of datanodes
  private String[] topologyPaths; // full path name in network topology
  private long offset;  //offset of the of the block in the file
  private long length;

  /**
   * Default Constructor
   */
  public BlockLocation() {
    this(new String[0], new String[0],  0L, 0L);
  }

  /**
   * Constructor with host, name, offset and length
   */
  public BlockLocation(String[] names, String[] hosts, long offset,
                       long length) {
    if (names == null) {
      this.names = new String[0];
    } else {
      this.names = names;
    }
    if (hosts == null) {
      this.hosts = new String[0];
    } else {
      this.hosts = hosts;
    }
    this.offset = offset;
    this.length = length;
    this.topologyPaths = new String[0];
  }

  /**
   * Constructor with host, name, network topology, offset and length
   */
  public BlockLocation(String[] names, String[] hosts, String[] topologyPaths,
                       long offset, long length) {
    this(names, hosts, offset, length);
    if (topologyPaths == null) {
      this.topologyPaths = new String[0];
    } else {
      this.topologyPaths = topologyPaths;
    }
  }

  /**
   * Get the list of hosts (hostname) hosting this block
   * 获取主机名列表
   */
  public String[] getHosts() throws IOException {
    if ((hosts == null) || (hosts.length == 0)) {
      return new String[0];
    } else {
      return hosts;
    }
  }

  /**
   * Get the list of names (hostname:port) hosting this block
   * 获取端口号列表
   */
  public String[] getNames() throws IOException {
    if ((names == null) || (names.length == 0)) {
      return new String[0];
    } else {
      return this.names;
    }
  }

  /**
   * Get the list of network topology paths for each of the hosts.
   * The last component of the path is the host.
   * 获取每一个主机的网络拓扑路径的列表，路径的最后部分是主机
   */
  public String[] getTopologyPaths() throws IOException {
    if ((topologyPaths == null) || (topologyPaths.length == 0)) {
      return new String[0];
    } else {
      return this.topologyPaths;
    }
  }

  /**
   * Get the start offset of file associated with this block
   * 获取块的偏移量
   */
  public long getOffset() {
    return offset;
  }

  /**
   * Get the length of the block
   * 获取块的长度信息
   */
  public long getLength() {
    return length;
  }

  /**
   * Set the start offset of file associated with this block
   * 设置快的偏移量
   */
  public void setOffset(long offset) {
    this.offset = offset;
  }

  /**
   * Set the length of block
   * 设置块的长度信息
   */
  public void setLength(long length) {
    this.length = length;
  }

  /**
   * Set the hosts hosting this block
   * 设置当前块的主机名
   */
  public void setHosts(String[] hosts) throws IOException {
    if (hosts == null) {
      this.hosts = new String[0];
    } else {
      this.hosts = hosts;
    }
  }

  /**
   * Set the names (host:port) hosting this block
   * 设置当前块的端口号
   */
  public void setNames(String[] names) throws IOException {
    if (names == null) {
      this.names = new String[0];
    } else {
      this.names = names;
    }
  }

  /**
   * Set the network topology paths of the hosts
   * 设置主机的网络拓扑路径
   */
  public void setTopologyPaths(String[] topologyPaths) throws IOException {
    if (topologyPaths == null) {
      this.topologyPaths = new String[0];
    } else {
      this.topologyPaths = topologyPaths;
    }
  }

  /**
   * Implement write of Writable
   * 实现Writable的write方法，主要是将块位置的
   * 各参数信息写到输出缓存中
   */
  public void write(DataOutput out) throws IOException {
    out.writeLong(offset);
    out.writeLong(length);
    out.writeInt(names.length);
    for (int i=0; i < names.length; i++) {
      Text name = new Text(names[i]);
      name.write(out);
    }
    out.writeInt(hosts.length);
    for (int i=0; i < hosts.length; i++) {
      Text host = new Text(hosts[i]);
      host.write(out);
    }
    out.writeInt(topologyPaths.length);
    for (int i=0; i < topologyPaths.length; i++) {
      Text host = new Text(topologyPaths[i]);
      host.write(out);
    }
  }

  /**
   * Implement readFields of Writable
   * 实现Writable的readFields方法
   * 读入块位置的各参数信息
   */
  public void readFields(DataInput in) throws IOException {
    this.offset = in.readLong();
    this.length = in.readLong();
    int numNames = in.readInt();
    this.names = new String[numNames];
    for (int i = 0; i < numNames; i++) {
      Text name = new Text();
      name.readFields(in);
      names[i] = name.toString();
    }

    int numHosts = in.readInt();
    this.hosts = new String[numHosts];
    for (int i = 0; i < numHosts; i++) {
      Text host = new Text();
      host.readFields(in);
      hosts[i] = host.toString();
    }

    int numTops = in.readInt();
    topologyPaths = new String[numTops];
    for (int i = 0; i < numTops; i++) {
      Text path = new Text();
      path.readFields(in);
      topologyPaths[i] = path.toString();
    }
  }

  public String toString() {
    StringBuilder result = new StringBuilder();
    result.append(offset);
    result.append(',');
    result.append(length);
    for(String h: hosts) {
      result.append(',');
      result.append(h);
    }
    return result.toString();
  }
}
