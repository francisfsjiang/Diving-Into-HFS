package org.apache.hadoop.fs;

import java.net.*;
import java.io.*;
import org.apache.avro.reflect.Stringable;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;

@Stringable
@InterfaceAudience.Public
@InterfaceStability.Stable

/**
 * Hadoop中路径分为两类：\\
 * 1.绝对路径：
 * \begin{XeItem}
 *     \item 完全限定的URI, scheme://authority/path
 *     \item 以斜杠开头的路径, /path 表示相对于默认的文件系统, 即相当于default-scheme://default-authority/path
 * \end{XeItem}
 * 2.相对路径：path相对于工作目录
 */
public class Path implements Comparable {

  /** The directory separator, a slash. */
  public static final String SEPARATOR = "/";
  public static final char SEPARATOR_CHAR = '/';

  public static final String CUR_DIR = ".";

  static final boolean WINDOWS
    = System.getProperty("os.name").startsWith("Windows");

  private URI uri;

  public Path(String parent, String child) {
    this(new Path(parent), new Path(child));
  }

  public Path(Path parent, String child) {
    this(parent, new Path(child));
  }

  public Path(String parent, Path child) {
    this(new Path(parent), child);
  }

  public Path(Path parent, Path child) {
    URI parentUri = parent.uri;
    String parentPath = parentUri.getPath();
    if (!(parentPath.equals("/") || parentPath.equals("")))
      try {
        parentUri = new URI(parentUri.getScheme(), parentUri.getAuthority(),
                      parentUri.getPath()+"/", null, parentUri.getFragment());
      } catch (URISyntaxException e) {
        throw new IllegalArgumentException(e);
      }
    URI resolved = parentUri.resolve(child.uri);
    initialize(resolved.getScheme(), resolved.getAuthority(),
               normalizePath(resolved.getPath()), resolved.getFragment());
  }

  /**
   * 检查路径参数
   */
  private void checkPathArg( String path ) {
    if ( path == null ) {
      throw new IllegalArgumentException(
          "Can not create a Path from a null string");
    }
    if( path.length() == 0 ) {
       throw new IllegalArgumentException(
           "Can not create a Path from an empty string");
    }
  }

  public Path(String pathString) {
    checkPathArg( pathString );


    if (hasWindowsDrive(pathString, false))
      pathString = "/"+pathString;

    String scheme = null;
    String authority = null;

    int start = 0;

    int colon = pathString.indexOf(':');
    int slash = pathString.indexOf('/');
    if ((colon != -1) &&
        ((slash == -1) || (colon < slash))) {
      scheme = pathString.substring(0, colon);
      start = colon+1;
    }

    if (pathString.startsWith("//", start) &&
        (pathString.length()-start > 2)) {       // has authority
      int nextSlash = pathString.indexOf('/', start+2);
      int authEnd = nextSlash > 0 ? nextSlash : pathString.length();
      authority = pathString.substring(start+2, authEnd);
      start = authEnd;
    }

    String path = pathString.substring(start, pathString.length());

    initialize(scheme, authority, path, null);
  }

  public Path(URI aUri) {
    uri = aUri;
  }

  public Path(String scheme, String authority, String path) {
    checkPathArg( path );
    initialize(scheme, authority, path, null);
  }

  private void initialize(String scheme, String authority, String path,
      String fragment) {
    try {
      this.uri = new URI(scheme, authority, normalizePath(path), null, fragment)
        .normalize();
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(e);
    }
  }

  private String normalizePath(String path) {
    path = path.replace("//", "/");
    path = path.replace("\\", "/");

    int minLength = hasWindowsDrive(path, true) ? 4 : 1;
    if (path.length() > minLength && path.endsWith("/")) {
      path = path.substring(0, path.length()-1);
    }

    return path;
  }

  private boolean hasWindowsDrive(String path, boolean slashed) {
    if (!WINDOWS) return false;
    int start = slashed ? 1 : 0;
    return
      path.length() >= start+2 &&
      (slashed ? path.charAt(0) == '/' : true) &&
      path.charAt(start+1) == ':' &&
      ((path.charAt(start) >= 'A' && path.charAt(start) <= 'Z') ||
       (path.charAt(start) >= 'a' && path.charAt(start) <= 'z'));
  }


  public URI toUri() { return uri; }

  public FileSystem getFileSystem(Configuration conf) throws IOException {
    return FileSystem.get(this.toUri(), conf);
  }

  public boolean isUriPathAbsolute() {
    int start = hasWindowsDrive(uri.getPath(), true) ? 3 : 0;
    return uri.getPath().startsWith(SEPARATOR, start);
   }

  public boolean isAbsolute() {
     return isUriPathAbsolute();
  }

  public String getName() {
    String path = uri.getPath();
    int slash = path.lastIndexOf(SEPARATOR);
    return path.substring(slash+1);
  }

  public Path getParent() {
    String path = uri.getPath();
    int lastSlash = path.lastIndexOf('/');
    int start = hasWindowsDrive(path, true) ? 3 : 0;
    if ((path.length() == start) ||
        (lastSlash == start && path.length() == start+1)) {
      return null;
    }
    String parent;
    if (lastSlash==-1) {
      parent = CUR_DIR;
    } else {
      int end = hasWindowsDrive(path, true) ? 3 : 0;
      parent = path.substring(0, lastSlash==end?end+1:lastSlash);
    }
    return new Path(uri.getScheme(), uri.getAuthority(), parent);
  }

  public Path suffix(String suffix) {
    return new Path(getParent(), getName()+suffix);
  }

  public String toString() {
    StringBuilder buffer = new StringBuilder();
    if (uri.getScheme() != null) {
      buffer.append(uri.getScheme());
      buffer.append(":");
    }
    if (uri.getAuthority() != null) {
      buffer.append("//");
      buffer.append(uri.getAuthority());
    }
    if (uri.getPath() != null) {
      String path = uri.getPath();
      if (path.indexOf('/')==0 &&
          hasWindowsDrive(path, true) &&
          uri.getScheme() == null &&
          uri.getAuthority() == null)
        path = path.substring(1);
      buffer.append(path);
    }
    if (uri.getFragment() != null) {
      buffer.append("#");
      buffer.append(uri.getFragment());
    }
    return buffer.toString();
  }

  public boolean equals(Object o) {
    if (!(o instanceof Path)) {
      return false;
    }
    Path that = (Path)o;
    return this.uri.equals(that.uri);
  }

  public int hashCode() {
    return uri.hashCode();
  }

  public int compareTo(Object o) {
    Path that = (Path)o;
    return this.uri.compareTo(that.uri);
  }


  public int depth() {
    String path = uri.getPath();
    int depth = 0;
    int slash = path.length()==1 && path.charAt(0)=='/' ? -1 : 0;
    while (slash != -1) {
      depth++;
      slash = path.indexOf(SEPARATOR, slash+1);
    }
    return depth;
  }


  /**
   *  返回一个限定路径对象
   *  {@link #makeQualified(URI, Path)}
   */

  @Deprecated
  public Path makeQualified(FileSystem fs) {
    return makeQualified(fs.getUri(), fs.getWorkingDirectory());
  }


  @InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
  public Path makeQualified(URI defaultUri, Path workingDir ) {
    Path path = this;
    if (!isAbsolute()) {
      path = new Path(workingDir, this);
    }

    URI pathUri = path.toUri();

    String scheme = pathUri.getScheme();
    String authority = pathUri.getAuthority();
    String fragment = pathUri.getFragment();

    if (scheme != null &&
        (authority != null || defaultUri.getAuthority() == null))
      return path;

    if (scheme == null) {
      scheme = defaultUri.getScheme();
    }

    if (authority == null) {
      authority = defaultUri.getAuthority();
      if (authority == null) {
        authority = "";
      }
    }

    URI newUri = null;
    try {
      newUri = new URI(scheme, authority ,
        normalizePath(pathUri.getPath()), null, fragment);
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(e);
    }
    return new Path(newUri);
  }
}
