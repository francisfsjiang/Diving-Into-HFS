package org.apache.hadoop.fs;

import java.io.*;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * 一个包装类，类似于{@link DataInputStream}， 里面包装了一个
 * 实现了{@link Seekable}和{@link PositionReadbale}接口的
 * {@link java.io.InputStream}，通常是传入 {@link FSInputStream},
 * 此类的方法都是通过调用被包装对象的对应方法来实现的。
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class FSDataInputStream extends DataInputStream
    implements Seekable, PositionedReadable {

  public FSDataInputStream(InputStream in)
    throws IOException {
    super(in);
    if( !(in instanceof Seekable) || !(in instanceof PositionedReadable) ) {
      throw new IllegalArgumentException(
          "In is not an instance of Seekable or PositionedReadable");
    }
  }

  public synchronized void seek(long desired) throws IOException {
    ((Seekable)in).seek(desired);
  }

  public long getPos() throws IOException {
    return ((Seekable)in).getPos();
  }

  public int read(long position, byte[] buffer, int offset, int length)
    throws IOException {
    return ((PositionedReadable)in).read(position, buffer, offset, length);
  }

  public void readFully(long position, byte[] buffer, int offset, int length)
    throws IOException {
    ((PositionedReadable)in).readFully(position, buffer, offset, length);
  }

  public void readFully(long position, byte[] buffer)
    throws IOException {
    ((PositionedReadable)in).readFully(position, buffer, 0, buffer.length);
  }

  public boolean seekToNewSource(long targetPos) throws IOException {
    return ((Seekable)in).seekToNewSource(targetPos);
  }
}
