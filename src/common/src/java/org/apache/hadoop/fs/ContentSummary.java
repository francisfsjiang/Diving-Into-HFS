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
import org.apache.hadoop.io.Writable;

/** 储存内容的摘要(a directory or a file). */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class ContentSummary implements Writable{
  private long length;
  private long fileCount;
  private long directoryCount;
  private long quota;
  private long spaceConsumed;
  private long spaceQuota;
  

  /** Constructor */
  public ContentSummary() {}
  
  /** Constructor */
  public ContentSummary(long length, long fileCount, long directoryCount) {
    this(length, fileCount, directoryCount, -1L, length, -1L);
  }

  /** Constructor */
  public ContentSummary(
      long length, long fileCount, long directoryCount, long quota,
      long spaceConsumed, long spaceQuota) {
    this.length = length;
    this.fileCount = fileCount;
    this.directoryCount = directoryCount;
    this.quota = quota;
    this.spaceConsumed = spaceConsumed;
    this.spaceQuota = spaceQuota;
  }

  /** @return the length */
  public long getLength() {return length;}

  /** @return the directory count */
  public long getDirectoryCount() {return directoryCount;}

  /** @return the file count */
  public long getFileCount() {return fileCount;}
  
  /** 返回限定的目录a */
  public long getQuota() {return quota;}
  
  /** 返回消耗的空间 */ 
  public long getSpaceConsumed() {return spaceConsumed;}

  /** 返回存储空间的限额 */
  public long getSpaceQuota() {return spaceQuota;}
  
  /** {@inheritDoc} */
  @InterfaceAudience.Private
  public void write(DataOutput out) throws IOException {
    out.writeLong(length);
    out.writeLong(fileCount);
    out.writeLong(directoryCount);
    out.writeLong(quota);
    out.writeLong(spaceConsumed);
    out.writeLong(spaceQuota);
  }

  /** {@inheritDoc} */
  @InterfaceAudience.Private
  public void readFields(DataInput in) throws IOException {
    this.length = in.readLong();
    this.fileCount = in.readLong();
    this.directoryCount = in.readLong();
    this.quota = in.readLong();
    this.spaceConsumed = in.readLong();
    this.spaceQuota = in.readLong();
  }
  
  /** 
   * 输出格式:
   * <----12----> <----12----> <-------18------->
   *    DIR_COUNT   FILE_COUNT       CONTENT_SIZE FILE_NAME    
   */
  private static final String STRING_FORMAT = "%12d %12d %18d ";
  /** 
   * 输出格式:
   * <----12----> <----15----> <----15----> <----15----> <----12----> <----12----> <-------18------->
   *    QUOTA   REMAINING_QUATA SPACE_QUOTA SPACE_QUOTA_REM DIR_COUNT   FILE_COUNT   CONTENT_SIZE     FILE_NAME    
   */
  private static final String QUOTA_STRING_FORMAT = "%12s %15s ";
  private static final String SPACE_QUOTA_STRING_FORMAT = "%15s %15s ";
  
  /** 数据头字符串*/
  private static final String HEADER = String.format(
      STRING_FORMAT.replace('d', 's'), "directories", "files", "bytes");

  private static final String QUOTA_HEADER = String.format(
      QUOTA_STRING_FORMAT + SPACE_QUOTA_STRING_FORMAT, 
      "quota", "remaining quota", "space quota", "reamaining quota") +
      HEADER;
  
  /** 返回输出的数据头
   * 如果 qOption 为false, 输出目录数，,文件数,内容大小. 
   * 如果 qOption 为true, 输出空间限额以及剩余的空间.
   * 
   * @param qOption 用来监听是否需要输出限额的哨兵
   * @return 输出的数据头
   */
  public static String getHeader(boolean qOption) {
    return qOption ? QUOTA_HEADER : HEADER;
  }
  
  /** {@inheritDoc} */
  public String toString() {
    return toString(true);
  }

  /** 按格式返回对象的字符串表示
   * 如果 qOption 为false, 输出目录数，,文件数,内容大小. 
   * 如果 qOption 为true, 输出空间限额以及剩余的空间.
   * @param qOption 用来监听是否需要输出限额的哨兵
   * @return 对象的字符串表示
   */
  public String toString(boolean qOption) {
    String prefix = "";
    if (qOption) {
      String quotaStr = "none";
      String quotaRem = "inf";
      String spaceQuotaStr = "none";
      String spaceQuotaRem = "inf";
      
      if (quota>0) {
        quotaStr = Long.toString(quota);
        quotaRem = Long.toString(quota-(directoryCount+fileCount));
      }
      if (spaceQuota>0) {
        spaceQuotaStr = Long.toString(spaceQuota);
        spaceQuotaRem = Long.toString(spaceQuota - spaceConsumed);        
      }
      
      prefix = String.format(QUOTA_STRING_FORMAT + SPACE_QUOTA_STRING_FORMAT, 
                             quotaStr, quotaRem, spaceQuotaStr, spaceQuotaRem);
    }
    
    return prefix + String.format(STRING_FORMAT, directoryCount, 
                                  fileCount, length);
  }
}
