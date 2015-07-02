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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.StringUtils;

/** 
 * Trash类继承自Configured类,是为文件系统中的文件提供一个"垃圾"标签,
 * 方便将文件移除到回收站中
 * 这种设计保证了垃圾回收管理不需要详细查询垃圾文件的content
 * 不需要文件系统的日期记录支持和时钟同步,保证了线程安全的垃圾回收功能
 * @author benco
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class Trash extends Configured {
  private static final Log LOG =
    LogFactory.getLog(Trash.class);

  private static final Path CURRENT = new Path("Current");
  private static final Path TRASH = new Path(".Trash/");
  private static final Path HOMES = new Path("/user/");

  private static final FsPermission PERMISSION =
    new FsPermission(FsAction.ALL, FsAction.NONE, FsAction.NONE);

  private static final DateFormat CHECKPOINT = new SimpleDateFormat("yyMMddHHmm");
  private static final int MSECS_PER_MINUTE = 60*1000;

  private final FileSystem fs;
  private final Path trash;
  private final Path current;
  private final long interval;

  /** Construct a trash can accessor.
   * Trash类的构造函数,通过给入Configuration对象conf进行相关设置
   * @param conf a Configuration
   */
  public Trash(Configuration conf) throws IOException {
    this(FileSystem.get(conf), conf);
  }

  /**
   *  Trash类的构造函数,通过给入FileSystem对象fs和Configuration对象conf
   *  对Trash类中的静态变量进行赋值
   */
  public Trash(FileSystem fs, Configuration conf) throws IOException {
    super(conf);
    this.fs = fs;
    this.trash = new Path(fs.getHomeDirectory(), TRASH);
    this.current = new Path(trash, CURRENT);
    this.interval = conf.getLong("fs.trash.interval", 60) * MSECS_PER_MINUTE;
  }
  /**
   *  Trash类的私有构造函数,通过给入Path对象home和Configuration对象
   *  对Trash类中的静态变量进行赋值
   */
  private Trash(Path home, Configuration conf) throws IOException {
    super(conf);
    this.fs = home.getFileSystem(conf);
    this.trash = new Path(home, TRASH);
    this.current = new Path(trash, CURRENT);
    this.interval = conf.getLong("fs.trash.interval", 60) * MSECS_PER_MINUTE;
  }
  /**
   * 返回要被删除文件目录与垃圾回收站的源目录组合的地址
   * 将被删除的文件移除到垃圾回收站
   */
  private Path makeTrashRelativePath(Path basePath, Path rmFilePath) {
    return new Path(basePath + rmFilePath.toUri().getPath());
  }

  /**
   * 将当前要被删除的目录和文件移到当前的垃圾回收站中
   */ 
  public boolean moveToTrash(Path path) throws IOException {
    if (interval == 0)
      return false;

    if (!path.isAbsolute())                       
      path = new Path(fs.getWorkingDirectory(), path);

    if (!fs.exists(path))                        
      throw new FileNotFoundException(path.toString());

    String qpath = path.makeQualified(fs).toString();

    if (qpath.startsWith(trash.toString())) {
      return false;                              
    }

    if (trash.getParent().toString().startsWith(qpath)) {
      throw new IOException("Cannot move \"" + path +
                            "\" to the trash, as it contains the trash");
    }

    Path trashPath = makeTrashRelativePath(current, path);
    Path baseTrashPath = makeTrashRelativePath(current, path.getParent());
    
    IOException cause = null;

    for (int i = 0; i < 2; i++) {
      try {
        if (!fs.mkdirs(baseTrashPath, PERMISSION)) {    
          LOG.warn("Can't create(mkdir) trash directory: "+baseTrashPath);
          return false;
        }
      } catch (IOException e) {
        LOG.warn("Can't create trash directory: "+baseTrashPath);
        cause = e;
        break;
      }
      try {
        String orig = trashPath.toString();
        
        while(fs.exists(trashPath)) {
          trashPath = new Path(orig + System.currentTimeMillis());
        }
        
        if (fs.rename(path, trashPath))           // move to current trash
          return true;
      } catch (IOException e) {
        cause = e;
      }
    }
    throw (IOException)
      new IOException("Failed to move to trash: "+path).initCause(cause);
  }

  /** 创建一个垃圾检查点. */
  public void checkpoint() throws IOException {
    if (!fs.exists(current))                      // no trash, no checkpoint
      return;

    Path checkpoint;
    synchronized (CHECKPOINT) {
      checkpoint = new Path(trash, CHECKPOINT.format(new Date()));
    }

    if (fs.rename(current, checkpoint)) {
      LOG.info("Created trash checkpoint: "+checkpoint.toUri().getPath());
    } else {
      throw new IOException("Failed to checkpoint trash: "+checkpoint);
    }
  }

  /** 删除过期的垃圾检查点. */
  public void expunge() throws IOException {
    FileStatus[] dirs = null;
    
    try {
      dirs = fs.listStatus(trash);            // scan trash sub-directories
    } catch (FileNotFoundException fnfe) {
      return;
    }

    long now = System.currentTimeMillis();
    for (int i = 0; i < dirs.length; i++) {
      Path path = dirs[i].getPath();
      String dir = path.toUri().getPath();
      String name = path.getName();
      if (name.equals(CURRENT.getName()))         // skip current
        continue;

      long time;
      try {
        synchronized (CHECKPOINT) {
          time = CHECKPOINT.parse(name).getTime();
        }
      } catch (ParseException e) {
        LOG.warn("Unexpected item in trash: "+dir+". Ignoring.");
        continue;
      }

      if ((now - interval) > time) {
        if (fs.delete(path, true)) {
          LOG.info("Deleted trash checkpoint: "+dir);
        } else {
          LOG.warn("Couldn't delete checkpoint: "+dir+" Ignoring.");
        }
      }
    }
  }

  /**
   * 获得当前Trash的工作目录  
   */
  Path getCurrentTrashDir() {
    return current;
  }

  /** 
   * 返回周期性清空所有用户垃圾文件的内部类Emptier的对象
   * 该对象只能被超级用户运行,并且运行时只保持一个检查点
   */
  public Runnable getEmptier() throws IOException {
    return new Emptier(getConf());
  }
 /** 
   * 内部类Emptier实现Runnable接口
   * 返回周期性清空所有用户垃圾文件的Emptier对象
   * 该对象只能被超级用户运行调用run()方法,并且运行时只保持一个检查点
   */
  private class Emptier implements Runnable {

    private Configuration conf;
    private long interval;

    Emptier(Configuration conf) throws IOException {
      this.conf = conf;
      this.interval = conf.getLong("fs.trash.interval", 0) * MSECS_PER_MINUTE;
    }

    public void run() {
      if (interval == 0)
        return;                                   // trash disabled

      long now = System.currentTimeMillis();
      long end;
      while (true) {
        end = ceiling(now, interval);
        try {                                     // sleep for interval
          Thread.sleep(end - now);
        } catch (InterruptedException e) {
          break;                                  // exit on interrupt
        }
          
        try {
          now = System.currentTimeMillis();
          if (now >= end) {

            FileStatus[] homes = null;
            try {
              homes = fs.listStatus(HOMES);         // list all home dirs
            } catch (IOException e) {
              LOG.warn("Trash can't list homes: "+e+" Sleeping.");
              continue;
            }

            for (FileStatus home : homes) {         // dump each trash
              if (!home.isDirectory())
                continue;
              try {
                Trash trash = new Trash(home.getPath(), conf);
                trash.expunge();
                trash.checkpoint();
              } catch (IOException e) {
                LOG.warn("Trash caught: "+e+". Skipping "+home.getPath()+".");
              } 
            }
          }
        } catch (Exception e) {
          LOG.warn("RuntimeException during Trash.Emptier.run() " + 
                   StringUtils.stringifyException(e));
        }
      }
      try {
        fs.close();
      } catch(IOException e) {
        LOG.warn("Trash cannot close FileSystem. " +
            StringUtils.stringifyException(e));
      }
    }
    /**
      * 将时间间隔的值向上取整
      */
    private long ceiling(long time, long interval) {
      return floor(time, interval) + interval;
    }

    /**
      * 将时间间隔的值向下取整
      */
    private long floor(long time, long interval) {
      return (time / interval) * interval;
    }

  }

  /** 获取Emptier对象并调用其run方法运行*/
  public static void main(String[] args) throws Exception {
    new Trash(new Configuration()).getEmptier().run();
  }

}
