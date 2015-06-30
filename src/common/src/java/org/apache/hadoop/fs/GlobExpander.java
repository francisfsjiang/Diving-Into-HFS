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
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

@InterfaceAudience.Private
@InterfaceStability.Unstable
class GlobExpander {
  //内部类StringWithOffset能够存储GlobExpander的string和offset成员变量
  static class StringWithOffset {
    String string;
    int offset;
    public StringWithOffset(String string, int offset) {
      super();
      this.string = string;
      this.offset = offset;
    }
  }
  
  /**
   * 静态方法expand()匹配指定模式的文件名或目录名称filePattern
   * 并将其结果存入List<String>对象fullyExpanded中返回
   * @param filePattern
   * @return List<String>对象
   * @throws IOException 
   */
  public static List<String> expand(String filePattern) throws IOException {
    List<String> fullyExpanded = new ArrayList<String>();
    List<StringWithOffset> toExpand = new ArrayList<StringWithOffset>();
    toExpand.add(new StringWithOffset(filePattern, 0));
    while (!toExpand.isEmpty()) {
      StringWithOffset path = toExpand.remove(0);
      List<StringWithOffset> expanded = expandLeftmost(path);
      if (expanded == null) {
        fullyExpanded.add(path.string);
      } else {
        toExpand.addAll(0, expanded);
      }
    }
    return fullyExpanded;
  }
  
  /**
   * 静态方法expandLeftmost()最左匹配带有偏移量模式的文件名或目录名称filePatternWithOffset
   * 从内部类StringWithOffset对象filePatternWithOffset中获得filePattern
   * 并调用leftmostOuterCurlyContainingSlash(filePattern,ilePatternWithOffset.offset)方法
   * 获得leftmost值,若为-1则返回null
   * 通过给定的leftmost获得filePattern的子串赋值为prefix
   * 以leftmost整数值作为起点遍历filePattern串中的字符
   * 并以'//','{','}',','分情况处理
   * @param filePattern
   * @return List<StringWithOffset>对象 exp
   * @throws IOException 
   */
  private static List<StringWithOffset> expandLeftmost(StringWithOffset
      filePatternWithOffset) throws IOException {
    
    String filePattern = filePatternWithOffset.string;
    int leftmost = leftmostOuterCurlyContainingSlash(filePattern,
        filePatternWithOffset.offset);
    if (leftmost == -1) {
      return null;
    }
    int curlyOpen = 0;
    StringBuilder prefix = new StringBuilder(filePattern.substring(0, leftmost));
    StringBuilder suffix = new StringBuilder();
    List<String> alts = new ArrayList<String>();
    StringBuilder alt = new StringBuilder();
    StringBuilder cur = prefix;
    for (int i = leftmost; i < filePattern.length(); i++) {
      char c = filePattern.charAt(i);
      if (cur == suffix) {
        cur.append(c);
      } else if (c == '\\') {
        i++;
        if (i >= filePattern.length()) {
          throw new IOException("Illegal file pattern: "
              + "An escaped character does not present for glob "
              + filePattern + " at " + i);
        }
        c = filePattern.charAt(i);
        cur.append(c);
      } else if (c == '{') {
        if (curlyOpen++ == 0) {
          alt.setLength(0);
          cur = alt;
        } else {
          cur.append(c);
        }

      } else if (c == '}' && curlyOpen > 0) {
        if (--curlyOpen == 0) {
          alts.add(alt.toString());
          alt.setLength(0);
          cur = suffix;
        } else {
          cur.append(c);
        }
      } else if (c == ',') {
        if (curlyOpen == 1) {
          alts.add(alt.toString());
          alt.setLength(0);
        } else {
          cur.append(c);
        }
      } else {
        cur.append(c);
      }
    }
    List<StringWithOffset> exp = new ArrayList<StringWithOffset>();
    for (String string : alts) {
      exp.add(new StringWithOffset(prefix + string + suffix, prefix.length()));
    }
    return exp;
  }
  
  /**
   * 静态方法leftmostOuterCurlyContainingSlash以offset整数值作为起点遍历filePattern串中的字符
   * 并以'//','{','}','/'分情况处理,找到能在'{}'字符之间找到'/'的最大下标值并返回
   * 找不到结果则返回-1
   * @param filePattern
   * @param offset
   * @return int数据类型
   * @throws IOException 
   */
  private static int leftmostOuterCurlyContainingSlash(String filePattern,
      int offset) throws IOException {
    int curlyOpen = 0;
    int leftmost = -1;
    boolean seenSlash = false;
    for (int i = offset; i < filePattern.length(); i++) {
      char c = filePattern.charAt(i);
      if (c == '\\') {
        i++;
        if (i >= filePattern.length()) {
          throw new IOException("Illegal file pattern: "
              + "An escaped character does not present for glob "
              + filePattern + " at " + i);
        }
      } else if (c == '{') {
        if (curlyOpen++ == 0) {
          leftmost = i;
        }
      } else if (c == '}' && curlyOpen > 0) {
        if (--curlyOpen == 0 && leftmost != -1 && seenSlash) {
          return leftmost;
        }
      } else if (c == '/' && curlyOpen > 0) {
        seenSlash = true;
      }
    }
    return -1;
  }

}
