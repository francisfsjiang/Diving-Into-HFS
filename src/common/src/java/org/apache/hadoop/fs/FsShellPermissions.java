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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FsShell.CmdHandler;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.ChmodParser;


/**
 * FsShellPermissions类是将文件操作与命令行关联起来的类
 * 因为FsShell类越来越大,所以将FsShellPermissions类单独分离出来
 * FsShellPermissions类中有三个静态内部类,分别是ChmodHandler,ChownHandler和ChgrpHandler
 * 还有一个静态方法changePermissions,将三个静态内部类的情况与命令行关联起来使用
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
class FsShellPermissions {
  
  /*========== chmod ==========*/

  /*
   * chmod shell command模式几乎是被允许使用的最灵活的模式.
   * 最主要的限制是只认识使用rwxXt.
   * 为了减少错误并同时加强八进制使用模式规范
   * 使用没有sticky的三位有效数字的位设置或者有sticky的四位有效数字的位设置
   */

  static String CHMOD_USAGE = "-chmod [-R] <MODE[,MODE]... | OCTALMODE> PATH...";

  private static  ChmodParser pp;
  
  private static class ChmodHandler extends CmdHandler {
    /**
      * @param FileSystem对象 fs
      * @param String对象 modeStr
      * @throw IOException
      * ChmodHandler继承自CmdHandler,ChmodHandler构造函数调用父类的构造函数并传入"chmod"和fs
      * 同时传入modeStr生成ChmodParser对象赋值给FsShellPermissions类的静态变量ChmodParser pp
      */
    ChmodHandler(FileSystem fs, String modeStr) throws IOException {
      super("chmod", fs);
      try {
        pp = new ChmodParser(modeStr);
      } catch(IllegalArgumentException iea) {
        patternError(iea.getMessage());
      }
    }
    /**
      * @param String对象 mode
      * @throw IOException
      * 当模式发生错误匹配不上时候抛出带有特定信息异常
      */
    private void patternError(String mode) throws IOException {
     throw new IOException("chmod : mode '" + mode + 
         "' does not match the expected pattern.");      
    }
    /**
      * @param FileStatus对象 file
      * @param FileSystem对象 srcFs
      * @throw IOException
      * Override父类run方法从ChmodParser对象pp获得权限(写入权限,读取权限,操作权限)
      */
    @Override
    public void run(FileStatus file, FileSystem srcFs) throws IOException {
      int newperms = pp.applyNewPermission(file);

      if (file.getPermission().toShort() != newperms) {
        try {
          srcFs.setPermission(file.getPath(), 
                                new FsPermission((short)newperms));
        } catch (IOException e) {
          System.err.println(getName() + ": changing permissions of '" + 
                             file.getPath() + "':" + e.getMessage());
        }
      }
    }
  }

  /*========== chown ==========*/
  
  static private String allowedChars = "[-_./@a-zA-Z0-9]";
  //只有allowedChars匹配到user或者group名称时候才允许使用
  static private Pattern chownPattern = 
         Pattern.compile("^\\s*(" + allowedChars + "+)?" +
                          "([:](" + allowedChars + "*))?\\s*$");
  static private Pattern chgrpPattern = 
         Pattern.compile("^\\s*(" + allowedChars + "+)\\s*$");
  
  static String CHOWN_USAGE = "-chown [-R] [OWNER][:[GROUP]] PATH...";
  static String CHGRP_USAGE = "-chgrp [-R] GROUP PATH...";  

  private static class ChownHandler extends CmdHandler {
    protected String owner = null;
    protected String group = null;
    /**
      * @param FileSystem对象 fs
      * @param String对象 cmd
      * @throw IOException
      * ChownHandler继承自CmdHandler,protected继承ChownHandler构造函数调用父类的构造函数并传入cmd和fs
      */
    protected ChownHandler(String cmd, FileSystem fs) { //for chgrp
      super(cmd, fs);
    }
    /**
      * @param FileSystem对象 fs
      * @param String对象 ownerStr
      * @throw IOException
      * ChownHandler继承自CmdHandler,ChownHandler构造函数调用父类的构造函数并传入"chown"和fs
      * chownPattern使用正则表达式匹配ownerStr并赋值给matcher,mathcer内部存有若干组匹配上的结果
      * 将第二组matcher.group(1)结果赋值给这个类的owner,将第四组结果matcher.group(3)赋值给这个类的group
      */
    ChownHandler(FileSystem fs, String ownerStr) throws IOException {
      super("chown", fs);
      Matcher matcher = chownPattern.matcher(ownerStr);
      if (!matcher.matches()) {
        throw new IOException("'" + ownerStr + "' does not match " +
                              "expected pattern for [owner][:group].");
      }
      owner = matcher.group(1);
      group = matcher.group(3);
      if (group != null && group.length() == 0) {
        group = null;
      }
      if (owner == null && group == null) {
        throw new IOException("'" + ownerStr + "' does not specify " +
                              " onwer or group.");
      }
    }
    /**
      * @param FileStatus对象 file
      * @param FileSystem对象 srcFs
      * @throw IOException
      * 通过从file.getOwner()和file.getGroup()中取值分别与owner和group比较
      * 并将结果分别赋值给newOwner和newGroup
      * 当newOwner和newGroup其中一个为空,则从file.getOwner()和file.getGroup()取值
      * 在srcFs中调用setOwner(file.getPath(), newOwner, newGroup)方法
      */
    @Override
    public void run(FileStatus file, FileSystem srcFs) throws IOException {
      String newOwner = (owner == null || owner.equals(file.getOwner())) ?
                        null : owner;
      String newGroup = (group == null || group.equals(file.getGroup())) ?
                        null : group;

      if (newOwner != null || newGroup != null) {
        try {
          srcFs.setOwner(file.getPath(), newOwner, newGroup);
        } catch (IOException e) {
          System.err.println(getName() + ": changing ownership of '" + 
                             file.getPath() + "':" + e.getMessage());

        }
      }
    }
  }

  /*========== chgrp ==========*/    
  
  private static class ChgrpHandler extends ChownHandler {
    /**
      * @param FileSystem对象 fs
      * @param String对象 groupStr
      * @throw IOException
      * ChgrpHandler继承自ChownHandler,ChgrpHandler构造函数调用父类的构造函数并传入chgrp和fs
      * chgrpPattern使用正则表达式匹配groupStr并赋值给matcher,mathcer内部存有若干组匹配上的结果
      * 将第二组结果matcher.group(1)赋值给这个类的group
      */
    ChgrpHandler(FileSystem fs, String groupStr) throws IOException {
      super("chgrp", fs);

      Matcher matcher = chgrpPattern.matcher(groupStr);
      if (!matcher.matches()) {
        throw new IOException("'" + groupStr + "' does not match " +
        "expected pattern for group");
      }
      group = matcher.group(1);
    }
  }
  /**
    * @param FileSystem对象 fs
    * @param String对象 cmd
    * @param String[]对象 argv[]
    * @param int类型 startIndex
    * @param FsShell对象 shell
    * @return int类型 shell.runCmdHandler(handler, argv, startIndex, recursive)
    * 静态方法changePermissions,将三个静态内部类的情况与命令行关联起来使用
    * 当cmd匹配上-chmod,调用ChmodHandler(fs, argv[startIndex++])构造函数并将对象赋值给handler
    * 当cmd匹配上-chown,调用ChownHandler(fs, argv[startIndex++])构造函数并将对象赋值给handler
    * 当cmd匹配上-chgrp,调用ChgrpHandler(fs, argv[startIndex++])构造函数并将对象赋值给handler
    */
  static int changePermissions(FileSystem fs, String cmd, 
                                String argv[], int startIndex, FsShell shell)
                                throws IOException {
    CmdHandler handler = null;
    boolean recursive = false;

    // handle common arguments, currently only "-R" 
    for (; startIndex < argv.length && argv[startIndex].equals("-R"); 
    startIndex++) {
      recursive = true;
    }

    if ( startIndex >= argv.length ) {
      throw new IOException("Not enough arguments for the command");
    }

    if (cmd.equals("-chmod")) {
      handler = new ChmodHandler(fs, argv[startIndex++]);
    } else if (cmd.equals("-chown")) {
      handler = new ChownHandler(fs, argv[startIndex++]);
    } else if (cmd.equals("-chgrp")) {
      handler = new ChgrpHandler(fs, argv[startIndex++]);
    }

    return shell.runCmdHandler(handler, argv, startIndex, recursive);
  } 
}
