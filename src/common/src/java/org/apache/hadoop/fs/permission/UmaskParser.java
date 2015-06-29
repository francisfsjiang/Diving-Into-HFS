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
package org.apache.hadoop.fs.permission;

import java.util.regex.Pattern;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * 解析String中提供的八进制或符号形式的umask模式, 并返回short型整数(以八进制形式).
 * Umask形式与标准形式略有不同, 它们不能指定"sticky bit"(粘滞位, 用于限制用户在公共目录中修改他人文件),
 * 也不能指定"special execute"(X, 拥有该标记的目录下的所有文件被无条件赋予可执行权限).
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
class UmaskParser extends PermissionParser {
  private static Pattern chmodOctalPattern =
    Pattern.compile("^\\s*[+]?()([0-7]{3})\\s*$"); // no leading 1 for sticky bit
  private static Pattern umaskSymbolicPattern =    /* not allow X or t */
    Pattern.compile("\\G\\s*([ugoa]*)([+=-]+)([rwx]*)([,\\s]*)\\s*");
  final short umaskMode;

  public UmaskParser(String modeStr) throws IllegalArgumentException {
    super(modeStr, umaskSymbolicPattern, chmodOctalPattern);

    umaskMode = (short)combineModes(0, false);
  }

  /**
   * 仅用于创建文件/目录. 符号形式的umask被用于描述"相对权限模式", 使用'+'或'-'来对
   * 原有的权限模式进行复位或置位.
   * 对八进制形式的umask而言, 指定的模式位通过创建时的模式给出.
   * For octal umask, the specified bits are set in the file mode creation mask.
   *
   * @return umask
   */
  public short getUMask() {
    if (symbolic) {
      // Return the complement of octal equivalent of umask that was computed
      return (short) (~umaskMode & 0777);
    }
    return umaskMode;
  }
}
