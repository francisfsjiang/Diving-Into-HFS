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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

/**
 * Options类是由final修饰的,无法被继承的类
 * 包含一些连接到文件系统操作的option
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public final class Options {
  /**
   * 静态内部类CreateOpts支持create方法中实现可变参数
   */
  public static class CreateOpts {
    private CreateOpts() { };
    public static BlockSize blockSize(long bs) { 
      return new BlockSize(bs);
    }
    public static BufferSize bufferSize(int bs) { 
      return new BufferSize(bs);
    }
    public static ReplicationFactor repFac(short rf) { 
      return new ReplicationFactor(rf);
    }
    public static BytesPerChecksum bytesPerChecksum(short crc) {
      return new BytesPerChecksum(crc);
    }
    public static Perms perms(FsPermission perm) {
      return new Perms(perm);
    }
    public static CreateParent createParent() {
      return new CreateParent(true);
    }
    public static CreateParent donotCreateParent() {
      return new CreateParent(false);
    }
    /**
      * BlockSize内部类继承自CreateOpts类
      * 内部含有blockSize常量
      */
    public static class BlockSize extends CreateOpts {
      private final long blockSize;
      protected BlockSize(long bs) {
        if (bs <= 0) {
          throw new IllegalArgumentException(
                        "Block size must be greater than 0");
        }
        blockSize = bs; 
      }
      public long getValue() { return blockSize; }
    }
    /**
      * ReplicationFactor内部类继承自CreateOpts类
      * 内部含有replication常量
      */
    public static class ReplicationFactor extends CreateOpts {
      private final short replication;
      protected ReplicationFactor(short rf) { 
        if (rf <= 0) {
          throw new IllegalArgumentException(
                      "Replication must be greater than 0");
        }
        replication = rf;
      }
      public short getValue() { return replication; }
    }
    /**
      * BufferSize内部类继承自CreateOpts类
      * 内部含有bufferSize常量
      */
    public static class BufferSize extends CreateOpts {
      private final int bufferSize;
      protected BufferSize(int bs) {
        if (bs <= 0) {
          throw new IllegalArgumentException(
                        "Buffer size must be greater than 0");
        }
        bufferSize = bs; 
      }
      public int getValue() { return bufferSize; }
    }
     /**
      * BytesPerChecksum内部类继承自CreateOpts类
      * 内部含有bytesPerChecksum常量
      */   
    public static class BytesPerChecksum extends CreateOpts {
      private final int bytesPerChecksum;
      protected BytesPerChecksum(short bpc) { 
        if (bpc <= 0) {
          throw new IllegalArgumentException(
                        "Bytes per checksum must be greater than 0");
        }
        bytesPerChecksum = bpc; 
      }
      public int getValue() { return bytesPerChecksum; }
    }
     /**
      * Perms内部类继承自CreateOpts类
      * 内部含有permissions常量
      */   
    public static class Perms extends CreateOpts {
      private final FsPermission permissions;
      protected Perms(FsPermission perm) { 
        if(perm == null) {
          throw new IllegalArgumentException("Permissions must not be null");
        }
        permissions = perm; 
      }
      public FsPermission getValue() { return permissions; }
    }
     /**
      * Progress内部类继承自CreateOpts类
      * 内部含有progress常量
      */  
    public static class Progress extends CreateOpts {
      private final Progressable progress;
      protected Progress(Progressable prog) { 
        if(prog == null) {
          throw new IllegalArgumentException("Progress must not be null");
        }
        progress = prog;
      }
      public Progressable getValue() { return progress; }
    }
     /**
      * CreateParent内部类继承自CreateOpts类
      * 内部含有createParent常量
      */  
    public static class CreateParent extends CreateOpts {
      private final boolean createParent;
      protected CreateParent(boolean createPar) {
        createParent = createPar;}
      public boolean getValue() { return createParent; }
    }

    
    /**
     * 从想要的数据类型中获得option
     * 当option为null则抛出IllegalArgumentException("Null opt")异常
     */
    protected static CreateOpts getOpt(Class<? extends CreateOpts> theClass,  CreateOpts ...opts) {
      if (opts == null) {
        throw new IllegalArgumentException("Null opt");
      }
      CreateOpts result = null;
      for (int i = 0; i < opts.length; ++i) {
        if (opts[i].getClass() == theClass) {
          if (result != null) 
            throw new IllegalArgumentException("multiple blocksize varargs");
          result = opts[i];
        }
      }
      return result;
    }
    /**
     * 对指定的option设置或更新一个新的值newValue
     */
    protected static <T extends CreateOpts> CreateOpts[] setOpt(T newValue,
        CreateOpts ...opts) {
      boolean alreadyInOpts = false;
      if (opts != null) {
        for (int i = 0; i < opts.length; ++i) {
          if (opts[i].getClass() == newValue.getClass()) {
            if (alreadyInOpts) 
              throw new IllegalArgumentException("multiple opts varargs");
            alreadyInOpts = true;
            opts[i] = newValue;
          }
        }
      }
      CreateOpts[] resultOpt = opts;
      if (!alreadyInOpts) { // no newValue in opt
        CreateOpts[] newOpts = new CreateOpts[opts.length + 1];
        System.arraycopy(opts, 0, newOpts, 0, opts.length);
        newOpts[opts.length] = newValue;
        resultOpt = newOpts;
      }
      return resultOpt;
    }
  }

  /**
   * 对支持可变参数的rename()方法的options进行枚举
   */
  public static enum Rename {
    NONE((byte) 0), // No options
    OVERWRITE((byte) 1); // Overwrite the rename destination

    private final byte code;
    
    private Rename(byte code) {
      this.code = code;
    }

    public static Rename valueOf(byte code) {
      return code < 0 || code >= values().length ? null : values()[code];
    }

    public byte value() {
      return code;
    }
  }
}
