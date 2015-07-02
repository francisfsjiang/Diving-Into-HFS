package org.apache.hadoop.fs;

import java.io.*;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/** 
 * 一个工具类，继承了@link DataOutputStream，包装一个@link PositionCache，
 * 具体的方法调用都通过被包装的对象实现，通过这种方法实现了写入统计功能。
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class FSDataOutputStream extends DataOutputStream implements Syncable {
  private OutputStream wrappedStream;

  /**
   * 此类包装了一个@link FileSystem.Statistics对象，在每次write
   * 时进行统计。
   */
  private static class PositionCache extends FilterOutputStream {
    private FileSystem.Statistics statistics;
    long position;

    public PositionCache(OutputStream out, 
                         FileSystem.Statistics stats,
                         long pos) throws IOException {
      super(out);
      statistics = stats;
      position = pos;
    }

    public void write(int b) throws IOException {
      out.write(b);
      position++;
      if (statistics != null) {
        statistics.incrementBytesWritten(1);
      }
    }
    
    public void write(byte b[], int off, int len) throws IOException {
      out.write(b, off, len);
      position += len;                            // update position
      if (statistics != null) {
        statistics.incrementBytesWritten(len);
      }
    }
      
    public long getPos() throws IOException {
      return position;                            // return cached position
    }
    
    public void close() throws IOException {
      out.close();
    }
  }

  @Deprecated
  public FSDataOutputStream(OutputStream out) throws IOException {
    this(out, null);
  }

  public FSDataOutputStream(OutputStream out, FileSystem.Statistics stats)
    throws IOException {
    this(out, stats, 0);
  }

  public FSDataOutputStream(OutputStream out, FileSystem.Statistics stats,
                            long startPosition) throws IOException {
    super(new PositionCache(out, stats, startPosition));
    wrappedStream = out;
  }
  
  public long getPos() throws IOException {
    return ((PositionCache)out).getPos();
  }

  public void close() throws IOException {
    out.close();        
  }

  public OutputStream getWrappedStream() {
    return wrappedStream;
  }

  @Override  
  @Deprecated
  public void sync() throws IOException {
    if (wrappedStream instanceof Syncable) {
      ((Syncable)wrappedStream).sync();
    }
  }
  
  @Override  
  public void hflush() throws IOException {
    if (wrappedStream instanceof Syncable) {
      ((Syncable)wrappedStream).hflush();
    } else {
      wrappedStream.flush();
    }
  }
  
  @Override  
  public void hsync() throws IOException {
    if (wrappedStream instanceof Syncable) {
      ((Syncable)wrappedStream).hsync();
    } else {
      wrappedStream.flush();
    }
  }
}
