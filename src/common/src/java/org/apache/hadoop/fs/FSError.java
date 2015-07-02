package org.apache.hadoop.fs;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/** 
 * 此异常会在文件系统异常时抛出，例如本地文件系统上发生磁盘错误时。
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class FSError extends Error {
  private static final long serialVersionUID = 1L;

  FSError(Throwable cause) {
    super(cause);
  }
}
