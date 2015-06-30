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

import java.io.Closeable;
import java.io.IOException;

import org.apache.avro.file.SeekableInput;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/** {@link FSDataInputStream} 到 Avro的SeekableInput接口的适配器. */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class AvroFSInput implements Closeable, SeekableInput {
  private final FSDataInputStream stream;
  private final long len;

  /** 由{@link FSDataInputStream}及其长度构建{@link AvroFSInput}实例 */
  public AvroFSInput(final FSDataInputStream in, final long len) {
    this.stream = in;
    this.len = len;
  }

  /** 以{@link FileContext}和{@link Path}构建{@link AvroFSInput}实例 */
  public AvroFSInput(final FileContext fc, final Path p) throws IOException {
    FileStatus status = fc.getFileStatus(p);
    this.len = status.getLen();
    this.stream = fc.open(p);
  }

  public long length() {
    return len;
  }

  public int read(byte[] b, int off, int len) throws IOException {
    return stream.read(b, off, len);
  }

  public void seek(long p) throws IOException {
    stream.seek(p);
  }

  public long tell() throws IOException {
    return stream.getPos();
  }

  public void close() throws IOException {
    stream.close();
  }
}
