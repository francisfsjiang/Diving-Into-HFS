package org.apache.hadoop.fs;

import java.io.*;
import java.util.Enumeration;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.Shell.ShellCommandExecutor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * 该类通过静态方法提供了大量的文件处理函数
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class FileUtil {

  private static final Log LOG = LogFactory.getLog(FileUtil.class);

  /**
   * 将一个FileStatus数组转化为Path数组，通过调用每个FileStatus
   * 的getPath()方法实现
   */
  public static Path[] stat2Paths(FileStatus[] stats) {
    if (stats == null)
      return null;
    Path[] ret = new Path[stats.length];
    for (int i = 0; i < stats.length; ++i) {
      ret[i] = stats[i].getPath();
    }
    return ret;
  }

  /**
   * 将一个FileStatus数组转化为Path数组，如果stats为null，则返回path
   */
  public static Path[] stat2Paths(FileStatus[] stats, Path path) {
    if (stats == null)
      return new Path[]{path};
    else
      return stat2Paths(stats);
  }
  
  /**
   * 删除dir指定的目录下所有内容，如果返回false，则可能造成部分删除
   */
  public static boolean fullyDelete(File dir) throws IOException {
    if (!fullyDeleteContents(dir)) {
      return false;
    }
    return dir.delete();
  }

  /**
   * 删除dir指定的目录下所有内容，而不删除目录本身如果返回false，则可能造成部分删除
   */
  public static boolean fullyDeleteContents(File dir) throws IOException {
    boolean deletionSucceeded = true;
    File contents[] = dir.listFiles();
    if (contents != null) {
      for (int i = 0; i < contents.length; i++) {
        if (contents[i].isFile()) {
          if (!contents[i].delete()) {
            deletionSucceeded = false;
            continue; 
          }
        } else {
          boolean b = false;
          b = contents[i].delete();
          if (b){
            continue;
          }
          if (!fullyDelete(contents[i])) {
            deletionSucceeded = false;
            continue; 
          }
        }
      }
    }
    return deletionSucceeded;
  }

  /**
   * 递归地删除fs中的一个目录，通过调用fs的delete方法，此方法
   * 将被放弃
   */
  @Deprecated
  public static void fullyDelete(FileSystem fs, Path dir) 
  throws IOException {
    fs.delete(dir, true);
  }

  private static void checkDependencies(FileSystem srcFS, 
                                        Path src, 
                                        FileSystem dstFS, 
                                        Path dst)
                                        throws IOException {
    if (srcFS == dstFS) {
      String srcq = src.makeQualified(srcFS).toString() + Path.SEPARATOR;
      String dstq = dst.makeQualified(dstFS).toString() + Path.SEPARATOR;
      if (dstq.startsWith(srcq)) {
        if (srcq.length() == dstq.length()) {
          throw new IOException("Cannot copy " + src + " to itself.");
        } else {
          throw new IOException("Cannot copy " + src + " to its subdirectory " +
                                dst);
        }
      }
    }
  }

  /**
   * 在文件系统间拷贝文件
   */
  public static boolean copy(FileSystem srcFS, Path src, 
                             FileSystem dstFS, Path dst, 
                             boolean deleteSource,
                             Configuration conf) throws IOException {
    return copy(srcFS, src, dstFS, dst, deleteSource, true, conf);
  }
  /**
   * 在文件系统间拷贝文件，可以指定是否删除源文件，是否复写已存在的文件
   */
  public static boolean copy(FileSystem srcFS, Path[] srcs, 
                             FileSystem dstFS, Path dst,
                             boolean deleteSource, 
                             boolean overwrite, Configuration conf)
                             throws IOException {
    boolean gotException = false;
    boolean returnVal = true;
    StringBuilder exceptions = new StringBuilder();

    if (srcs.length == 1)
      return copy(srcFS, srcs[0], dstFS, dst, deleteSource, overwrite, conf);

    // Check if dest is directory
    if (!dstFS.exists(dst)) {
      throw new IOException("`" + dst +"': specified destination directory " +
                            "doest not exist");
    } else {
      FileStatus sdst = dstFS.getFileStatus(dst);
      if (!sdst.isDirectory()) 
        throw new IOException("copying multiple files, but last argument `" +
                              dst + "' is not a directory");
    }

    for (Path src : srcs) {
      try {
        if (!copy(srcFS, src, dstFS, dst, deleteSource, overwrite, conf))
          returnVal = false;
      } catch (IOException e) {
        gotException = true;
        exceptions.append(e.getMessage());
        exceptions.append("\n");
      }
    }
    if (gotException) {
      throw new IOException(exceptions.toString());
    }
    return returnVal;
  }

  /**
   * 在文件系统间拷贝文件，可以指定是否删除源文件，是否复写已存在的文件
   */
  public static boolean copy(FileSystem srcFS, Path src, 
                             FileSystem dstFS, Path dst, 
                             boolean deleteSource,
                             boolean overwrite,
                             Configuration conf) throws IOException {
    FileStatus fileStatus = srcFS.getFileStatus(src);
    return copy(srcFS, fileStatus, dstFS, dst, deleteSource, overwrite, conf);
  }

  /**
   * 在文件系统间拷贝文件，可以指定是否删除源文件，是否复写已存在的文件
   */
  private static boolean copy(FileSystem srcFS, FileStatus srcStatus,
                              FileSystem dstFS, Path dst,
                              boolean deleteSource,
                              boolean overwrite,
                              Configuration conf) throws IOException {
    Path src = srcStatus.getPath();
    dst = checkDest(src.getName(), dstFS, dst, overwrite);
    if (srcStatus.isDirectory()) {
      checkDependencies(srcFS, src, dstFS, dst);
      if (!dstFS.mkdirs(dst)) {
        return false;
      }
      FileStatus contents[] = srcFS.listStatus(src);
      for (int i = 0; i < contents.length; i++) {
        copy(srcFS, contents[i], dstFS,
             new Path(dst, contents[i].getPath().getName()),
             deleteSource, overwrite, conf);
      }
    } else {
      InputStream in=null;
      OutputStream out = null;
      try {
        in = srcFS.open(src);
        out = dstFS.create(dst, overwrite);
        IOUtils.copyBytes(in, out, conf, true);
      } catch (IOException e) {
        IOUtils.closeStream(out);
        IOUtils.closeStream(in);
        throw e;
      }
    }
    if (deleteSource) {
      return srcFS.delete(src, true);
    } else {
      return true;
    }
  
  }

  /**
   * 拷贝指定目录下的所有文件，合并到同一个文件中，可以指定是否删除源文
   * 件，是否复写已存在的文件
   */
  public static boolean copyMerge(FileSystem srcFS, Path srcDir, 
                                  FileSystem dstFS, Path dstFile, 
                                  boolean deleteSource,
                                  Configuration conf, String addString) throws IOException {
    dstFile = checkDest(srcDir.getName(), dstFS, dstFile, false);

    if (srcFS.getFileStatus(srcDir).isDirectory())
      return false;
   
    OutputStream out = dstFS.create(dstFile);
    
    try {
      FileStatus contents[] = srcFS.listStatus(srcDir);
      for (int i = 0; i < contents.length; i++) {
        if (contents[i].isFile()) {
          InputStream in = srcFS.open(contents[i].getPath());
          try {
            IOUtils.copyBytes(in, out, conf, false);
            if (addString!=null)
              out.write(addString.getBytes("UTF-8"));
                
          } finally {
            in.close();
          } 
        }
      }
    } finally {
      out.close();
    }
    

    if (deleteSource) {
      return srcFS.delete(srcDir, true);
    } else {
      return true;
    }
  }  
  
  /**
   * 把本地文件拷贝到指定文件系统中
   */
  public static boolean copy(File src,
                             FileSystem dstFS, Path dst,
                             boolean deleteSource,
                             Configuration conf) throws IOException {
    dst = checkDest(src.getName(), dstFS, dst, false);

    if (src.isDirectory()) {
      if (!dstFS.mkdirs(dst)) {
        return false;
      }
      File contents[] = src.listFiles();
      for (int i = 0; i < contents.length; i++) {
        copy(contents[i], dstFS, new Path(dst, contents[i].getName()),
             deleteSource, conf);
      }
    } else if (src.isFile()) {
      InputStream in = null;
      OutputStream out =null;
      try {
        in = new FileInputStream(src);
        out = dstFS.create(dst);
        IOUtils.copyBytes(in, out, conf);
      } catch (IOException e) {
        IOUtils.closeStream( out );
        IOUtils.closeStream( in );
        throw e;
      }
    } else {
      throw new IOException(src.toString() + 
                            ": No such file or directory");
    }
    if (deleteSource) {
      return FileUtil.fullyDelete(src);
    } else {
      return true;
    }
  }

  /**
   * 将文件系统中的文件拷贝到本地
   */
  public static boolean copy(FileSystem srcFS, Path src, 
                             File dst, boolean deleteSource,
                             Configuration conf) throws IOException {
    FileStatus filestatus = srcFS.getFileStatus(src);
    return copy(srcFS, filestatus, dst, deleteSource, conf);
  }

  /**
   * 将文件系统中的文件拷贝到本地
   */
  private static boolean copy(FileSystem srcFS, FileStatus srcStatus,
                              File dst, boolean deleteSource,
                              Configuration conf) throws IOException {
    Path src = srcStatus.getPath();
    if (srcStatus.isDirectory()) {
      if (!dst.mkdirs()) {
        return false;
      }
      FileStatus contents[] = srcFS.listStatus(src);
      for (int i = 0; i < contents.length; i++) {
        copy(srcFS, contents[i],
             new File(dst, contents[i].getPath().getName()),
             deleteSource, conf);
      }
    } else {
      InputStream in = srcFS.open(src);
      IOUtils.copyBytes(in, new FileOutputStream(dst), conf);
    }
    if (deleteSource) {
      return srcFS.delete(src, true);
    } else {
      return true;
    }
  }
  /**
   * 用于检查目标路径是否为目录或者是已经存在，返回目标路径
   */
  private static Path checkDest(String srcName, FileSystem dstFS, Path dst,
      boolean overwrite) throws IOException {
    if (dstFS.exists(dst)) {
      FileStatus sdst = dstFS.getFileStatus(dst);
      if (sdst.isDirectory()) {
        if (null == srcName) {
          throw new IOException("Target " + dst + " is a directory");
        }
        return checkDest(null, dstFS, new Path(dst, srcName), overwrite);
      } else if (!overwrite) {
        throw new IOException("Target " + dst + " already exists");
      }
    }
    return dst;
  }

  /**
   * 这个类只用来在windows下调用cygpath命令
   */
  private static class CygPathCommand extends Shell {
    String[] command;
    String result;
    CygPathCommand(String path) throws IOException {
      command = new String[]{"cygpath", "-u", path};
      run();
    }
    String getResult() throws IOException {
      return result;
    }
    protected String[] getExecString() {
      return command;
    }
    protected void parseExecResult(BufferedReader lines) throws IOException {
      String line = lines.readLine();
      if (line == null) {
        throw new IOException("Can't convert '" + command[2] + 
                              " to a cygwin path");
      }
      result = line;
    }
  }

  /**
   * 转换一个系统相关的文件命名到可以再shell下工作的路径
   */
  public static String makeShellPath(String filename) throws IOException {
    if (Path.WINDOWS) {
      return new CygPathCommand(filename).getResult();
    } else {
      return filename;
    }    
  }

  /**
   * 转换一个系统相关的文件命名到可以再shell下工作的路径
   */
  public static String makeShellPath(File file) throws IOException {
    return makeShellPath(file, false);
  }

  /**
   * 转换一个系统相关的文件命名到可以再shell下工作的路径
   */
  public static String makeShellPath(File file, boolean makeCanonicalPath) 
  throws IOException {
    if (makeCanonicalPath) {
      return makeShellPath(file.getCanonicalPath());
    } else {
      return makeShellPath(file.toString());
    }
  }

  /**
   * 获取指定目录在磁盘上的大小
   */
  public static long getDU(File dir) {
    long size = 0;
    if (!dir.exists())
      return 0;
    if (!dir.isDirectory()) {
      return dir.length();
    } else {
      size = dir.length();
      File[] allFiles = dir.listFiles();
      for (int i = 0; i < allFiles.length; i++) {
        size = size + getDU(allFiles[i]);
      }
      return size;
    }
  }
    
  /**
   * 解压一个ZIP压缩文件，到第二个参数指定的位置
   */
  public static void unZip(File inFile, File unzipDir) throws IOException {
    Enumeration<? extends ZipEntry> entries;
    ZipFile zipFile = new ZipFile(inFile);

    try {
      entries = zipFile.entries();
      while (entries.hasMoreElements()) {
        ZipEntry entry = entries.nextElement();
        if (!entry.isDirectory()) {
          InputStream in = zipFile.getInputStream(entry);
          try {
            File file = new File(unzipDir, entry.getName());
            if (!file.getParentFile().mkdirs()) {           
              if (!file.getParentFile().isDirectory()) {
                throw new IOException("Mkdirs failed to create " + 
                                      file.getParentFile().toString());
              }
            }
            OutputStream out = new FileOutputStream(file);
            try {
              byte[] buffer = new byte[8192];
              int i;
              while ((i = in.read(buffer)) != -1) {
                out.write(buffer, 0, i);
              }
            } finally {
              out.close();
            }
          } finally {
            in.close();
          }
        }
      }
    } finally {
      zipFile.close();
    }
  }

  /**
   * 解压一个Tar压缩文件，到第二个参数指定的位置
   */
  public static void unTar(File inFile, File untarDir) throws IOException {
    if (!untarDir.mkdirs()) {           
      if (!untarDir.isDirectory()) {
        throw new IOException("Mkdirs failed to create " + untarDir);
      }
    }

    StringBuilder untarCommand = new StringBuilder();
    boolean gzipped = inFile.toString().endsWith("gz");
    if (gzipped) {
      untarCommand.append(" gzip -dc '");
      untarCommand.append(FileUtil.makeShellPath(inFile));
      untarCommand.append("' | (");
    } 
    untarCommand.append("cd '");
    untarCommand.append(FileUtil.makeShellPath(untarDir)); 
    untarCommand.append("' ; ");
    untarCommand.append("tar -xf ");
    
    if (gzipped) {
      untarCommand.append(" -)");
    } else {
      untarCommand.append(FileUtil.makeShellPath(inFile));
    }
    String[] shellCmd = { "bash", "-c", untarCommand.toString() };
    ShellCommandExecutor shexec = new ShellCommandExecutor(shellCmd);
    shexec.execute();
    int exitcode = shexec.getExitCode();
    if (exitcode != 0) {
      throw new IOException("Error untarring file " + inFile + 
                  ". Tar process exited with exit code " + exitcode);
    }
  }

  /**
   * 该类抽象了硬链接
   * 支持unix，cygwin，windxp
   */
  public static class HardLink { 
    enum OSType {
      OS_TYPE_UNIX, 
      OS_TYPE_WINXP,
      OS_TYPE_SOLARIS,
      OS_TYPE_MAC; 
    }
  
    private static String[] hardLinkCommand;
    private static String[] getLinkCountCommand;
    private static OSType osType;
    
    static {
      osType = getOSType();
      switch(osType) {
      case OS_TYPE_WINXP:
        hardLinkCommand = new String[] {"fsutil","hardlink","create", null, null};
        getLinkCountCommand = new String[] {"stat","-c%h"};
        break;
      case OS_TYPE_SOLARIS:
        hardLinkCommand = new String[] {"ln", null, null};
        getLinkCountCommand = new String[] {"ls","-l"};
        break;
      case OS_TYPE_MAC:
        hardLinkCommand = new String[] {"ln", null, null};
        getLinkCountCommand = new String[] {"stat","-f%l"};
        break;
      case OS_TYPE_UNIX:
      default:
        hardLinkCommand = new String[] {"ln", null, null};
        getLinkCountCommand = new String[] {"stat","-c%h"};
      }
    }

    /**
     * 获取系统类型
     */
    static private OSType getOSType() {
      String osName = System.getProperty("os.name");
      if (osName.indexOf("Windows") >= 0 && 
          (osName.indexOf("XP") >= 0 || osName.indexOf("2003") >= 0 || osName.indexOf("Vista") >= 0))
        return OSType.OS_TYPE_WINXP;
      else if (osName.indexOf("SunOS") >= 0)
         return OSType.OS_TYPE_SOLARIS;
      else if (osName.indexOf("Mac") >= 0)
         return OSType.OS_TYPE_MAC;
      else
        return OSType.OS_TYPE_UNIX;
    }
    
    /**
     * 创建一个硬链接
     */
    public static void createHardLink(File target, 
                                      File linkName) throws IOException {
      int len = hardLinkCommand.length;
      if (osType == OSType.OS_TYPE_WINXP) {
       hardLinkCommand[len-1] = target.getCanonicalPath();
       hardLinkCommand[len-2] = linkName.getCanonicalPath();
      } else {
       hardLinkCommand[len-2] = makeShellPath(target, true);
       hardLinkCommand[len-1] = makeShellPath(linkName, true);
      }
      // execute shell command
      Process process = Runtime.getRuntime().exec(hardLinkCommand);
      try {
        if (process.waitFor() != 0) {
          String errMsg = new BufferedReader(new InputStreamReader(
                                                                   process.getInputStream())).readLine();
          if (errMsg == null)  errMsg = "";
          String inpMsg = new BufferedReader(new InputStreamReader(
                                                                   process.getErrorStream())).readLine();
          if (inpMsg == null)  inpMsg = "";
          throw new IOException(errMsg + inpMsg);
        }
      } catch (InterruptedException e) {
        throw new IOException(StringUtils.stringifyException(e));
      } finally {
        process.destroy();
      }
    }

    /**
     * 获取指定文件的链接数量
     */
    public static int getLinkCount(File fileName) throws IOException {
      if (!fileName.exists()) {
        throw new FileNotFoundException(fileName + " not found.");
      }

      int len = getLinkCountCommand.length;
      String[] cmd = new String[len + 1];
      for (int i = 0; i < len; i++) {
        cmd[i] = getLinkCountCommand[i];
      }
      cmd[len] = fileName.toString();
      String inpMsg = null;
      String errMsg = null;
      int exitValue = -1;
      BufferedReader in = null;
      BufferedReader err = null;

      Process process = Runtime.getRuntime().exec(cmd);
      try {
        exitValue = process.waitFor();
        in = new BufferedReader(new InputStreamReader(
                                    process.getInputStream()));
        inpMsg = in.readLine();
        err = new BufferedReader(new InputStreamReader(
                                     process.getErrorStream()));
        errMsg = err.readLine();
        if (inpMsg == null || exitValue != 0) {
          throw createIOException(fileName, inpMsg, errMsg, exitValue, null);
        }
        if (getOSType() == OSType.OS_TYPE_SOLARIS) {
          String[] result = inpMsg.split("\\s+");
          return Integer.parseInt(result[1]);
        } else {
          return Integer.parseInt(inpMsg);
        }
      } catch (NumberFormatException e) {
        throw createIOException(fileName, inpMsg, errMsg, exitValue, e);
      } catch (InterruptedException e) {
        throw createIOException(fileName, inpMsg, errMsg, exitValue, e);
      } finally {
        process.destroy();
        if (in != null) in.close();
        if (err != null) err.close();
      }
    }
  }

  /**
   * 计算链接数量失败时抛出的错误
   */
  static private IOException createIOException(File f, String message,
      String error, int exitvalue, Exception cause) {
    final String s = "Failed to get link count on file " + f
        + ": message=" + message
        + "; error=" + error
        + "; exit value=" + exitvalue;
    return cause == null? new IOException(s): new IOException(s, cause);
  }

  /**
   * 创建一个软连接
   * 只在local磁盘上工作，HDFS不支持此操作
   */
  public static int symLink(String target, String linkname) throws IOException{
    String cmd = "ln -s " + target + " " + linkname;
    Process p = Runtime.getRuntime().exec(cmd, null);
    int returnVal = -1;
    try{
      returnVal = p.waitFor();
    } catch(InterruptedException e){
    }
    return returnVal;
  }
  
  /**
   * 更改文件的权限
   */
  public static int chmod(String filename, String perm
                          ) throws IOException, InterruptedException {
    return chmod(filename, perm, false);
  }

  /**
   * 更改文件的权限，可以设置是否递归更改
   */
  public static int chmod(String filename, String perm, boolean recursive)
                            throws IOException, InterruptedException {
    StringBuilder cmdBuf = new StringBuilder();
    cmdBuf.append("chmod ");
    if (recursive) {
      cmdBuf.append("-R ");
    }
    cmdBuf.append(perm).append(" ");
    cmdBuf.append(filename);
    String[] shellCmd = {"bash", "-c" ,cmdBuf.toString()};
    ShellCommandExecutor shExec = new ShellCommandExecutor(shellCmd);
    try {
      shExec.execute();
    }catch(Exception e) {
      if(LOG.isDebugEnabled()) {
        LOG.debug("Error while changing permission : " + filename 
            +" Exception: " + StringUtils.stringifyException(e));
      }
    }
    return shExec.getExitCode();
  }
  
  /**
   * 创建一个文件的临时文件
   */
  public static final File createLocalTempFile(final File basefile,
                                               final String prefix,
                                               final boolean isDeleteOnExit)
    throws IOException {
    File tmp = File.createTempFile(prefix + basefile.getName(),
                                   "", basefile.getParentFile());
    if (isDeleteOnExit) {
      tmp.deleteOnExit();
    }
    return tmp;
  }

  /**
   * 移动文件
   */
  public static void replaceFile(File src, File target) throws IOException {
    if (!src.renameTo(target)) {
      int retries = 5;
      while (target.exists() && !target.delete() && retries-- >= 0) {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          throw new IOException("replaceFile interrupted.");
        }
      }
      if (!src.renameTo(target)) {
        throw new IOException("Unable to rename " + src +
                              " to " + target);
      }
    }
  }
}
