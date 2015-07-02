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
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.io.WritableUtils;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.znerd.xmlenc.XMLOutputter;

/** MD5 of MD5 of CRC32.
  * MD5MD5CRC32FileChecksum继承自FileChecksum,
  * MD5加密并用CRC32校验和校验结果,
  * 定义三个私有成员变量,bytesPerCRC,crcPerBlock和md5,
  * 声明有静态成员常量LENGTH = MD5Hash.MD5_LEN + (Integer.SIZE + Long.SIZE)/Byte.SIZE
  */
@InterfaceAudience.LimitedPrivate({"HDFS"})
@InterfaceStability.Unstable
public class MD5MD5CRC32FileChecksum extends FileChecksum {
  public static final int LENGTH = MD5Hash.MD5_LEN
      + (Integer.SIZE + Long.SIZE)/Byte.SIZE;

  private int bytesPerCRC;
  private long crcPerBlock;
  private MD5Hash md5;

  /** 构造函数对三个成员变量初始化
    * bytesPerCRC = 0
    * crcPerBlock = 0
    * md5 = null
    */
  public MD5MD5CRC32FileChecksum() {
    this(0, 0, null);
  }

  /**
    * @param bytesPerCRC
    * @param crcPerBlock
    * @param md5
    * 构造函数对三个成员变量赋初值
    */
  public MD5MD5CRC32FileChecksum(int bytesPerCRC, long crcPerBlock, MD5Hash md5) {
    this.bytesPerCRC = bytesPerCRC;
    this.crcPerBlock = crcPerBlock;
    this.md5 = md5;
  }

  /**
    * @return String
    * 返回算法名称
    */
  public String getAlgorithmName() {
    return "MD5-of-" + crcPerBlock + "MD5-of-" + bytesPerCRC + "CRC32";
  }

  /**
    * @return LENGTH
    * 返回静态常量LENGTH
    */
  public int getLength() {return LENGTH;}

  /**
    * @return byte[]
    * 调用WritableUtils返回一个byte数组
    */
  public byte[] getBytes() {
    return WritableUtils.toByteArray(this);
  }

  /**
    * @throw IOException
    * @param DataInput对象 in
    * 通过从数据输入流中读入bytesPerCRC,crcPerBlock,md5的值
    */
  public void readFields(DataInput in) throws IOException {
    bytesPerCRC = in.readInt();
    crcPerBlock = in.readLong();
    md5 = MD5Hash.read(in);
  }

  /**
    * @throw IOException
    * @param DataOutput对象 out
    * 将bytesPerCRC,crcPerBlock,md5的值写入到数据输出流中
    */
  public void write(DataOutput out) throws IOException {
    out.writeInt(bytesPerCRC);
    out.writeLong(crcPerBlock);
    md5.write(out);
  }

  /**
    * @throw IOException
    * @param XMLOutputter对象 xml
    * @param MD5MD5CRC32FileChecksum对象 that
    * 将MD5MD5CRC32FileChecksum对象that的bytesPerCRC,crcPerBlock,md5的值写入到XML文件中
    */
  public static void write(XMLOutputter xml, MD5MD5CRC32FileChecksum that
      ) throws IOException {
    xml.startTag(MD5MD5CRC32FileChecksum.class.getName());
    if (that != null) {
      xml.attribute("bytesPerCRC", "" + that.bytesPerCRC);
      xml.attribute("crcPerBlock", "" + that.crcPerBlock);
      xml.attribute("md5", "" + that.md5);
    }
    xml.endTag();
  }

  /**
    * @throw SAXException
    * @param Attributes对象 attrs
    * @return MD5MD5CRC32FileChecksum
    * 静态方法valueOf,通过外部实体attrs获得bytesPerCRC,crcPerBlock和md5的值
    * 当外部实体未被找到时候抛出异常,并返回MD5MD5CRC32FileChecksum对象
    */
  public static MD5MD5CRC32FileChecksum valueOf(Attributes attrs
      ) throws SAXException {
    final String bytesPerCRC = attrs.getValue("bytesPerCRC");
    final String crcPerBlock = attrs.getValue("crcPerBlock");
    final String md5 = attrs.getValue("md5");
    if (bytesPerCRC == null || crcPerBlock == null || md5 == null) {
      return null;
    }

    try {
      return new MD5MD5CRC32FileChecksum(Integer.valueOf(bytesPerCRC),
          Integer.valueOf(crcPerBlock), new MD5Hash(md5));
    } catch(Exception e) {
      throw new SAXException("Invalid attributes: bytesPerCRC=" + bytesPerCRC
          + ", crcPerBlock=" + crcPerBlock + ", md5=" + md5, e);
    }
  }

  /**
    * @return String
    * 返回算法名称+ ":" + md5
    */
  public String toString() {
    return getAlgorithmName() + ":" + md5;
  }
}
