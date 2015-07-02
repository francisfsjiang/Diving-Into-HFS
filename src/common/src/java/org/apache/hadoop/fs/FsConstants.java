package org.apache.hadoop.fs;

import java.net.URI;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * 此类保存了{@link FileSystem}需要用到的常量
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public interface FsConstants {
  public static final URI LOCAL_FS_URI = URI.create("file:///");

  public static final String FTP_SCHEME = "ftp";
}
