package org.apache.hadoop.fs;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.EnumSet;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileSystem.Statistics;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.util.Progressable;

/**
 * 一个抽象类，包装了一个{@link AbstractFileSystem}对象，该类所有的方法都是通过调用
 * 被包装对象的对应方法实现的，继承此类的类通过重载其方法来添加更多地功能
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public abstract class FilterFs extends AbstractFileSystem {
  private final AbstractFileSystem myFs;

  protected AbstractFileSystem getMyFs() {
    return myFs;
  }

  protected FilterFs(AbstractFileSystem fs) throws IOException,
      URISyntaxException {
    super(fs.getUri(), fs.getUri().getScheme(),
        fs.getUri().getAuthority() != null, fs.getUriDefaultPort());
    myFs = fs;
  }

  @Override
  protected Statistics getStatistics() {
    return myFs.getStatistics();
  }

  @Override
  protected Path getInitialWorkingDirectory() {
    return myFs.getInitialWorkingDirectory();
  }

  @Override
  protected Path getHomeDirectory() {
    return myFs.getHomeDirectory();
  }

  @Override
  protected FSDataOutputStream createInternal(Path f,
    EnumSet<CreateFlag> flag, FsPermission absolutePermission, int bufferSize,
    short replication, long blockSize, Progressable progress,
    int bytesPerChecksum, boolean createParent)
      throws IOException, UnresolvedLinkException {
    checkPath(f);
    return myFs.createInternal(f, flag, absolutePermission, bufferSize,
        replication, blockSize, progress, bytesPerChecksum, createParent);
  }

  @Override
  protected boolean delete(Path f, boolean recursive)
      throws IOException, UnresolvedLinkException {
    checkPath(f);
    return myFs.delete(f, recursive);
  }

  @Override
  protected BlockLocation[] getFileBlockLocations(Path f, long start, long len)
      throws IOException, UnresolvedLinkException {
    checkPath(f);
    return myFs.getFileBlockLocations(f, start, len);
  }

  @Override
  protected FileChecksum getFileChecksum(Path f)
      throws IOException, UnresolvedLinkException {
    checkPath(f);
    return myFs.getFileChecksum(f);
  }

  @Override
  protected FileStatus getFileStatus(Path f)
      throws IOException, UnresolvedLinkException {
    checkPath(f);
    return myFs.getFileStatus(f);
  }

  @Override
  protected FileStatus getFileLinkStatus(final Path f)
    throws IOException, UnresolvedLinkException {
    checkPath(f);
    return myFs.getFileLinkStatus(f);
  }

  @Override
  protected FsStatus getFsStatus(final Path f) throws AccessControlException,
    FileNotFoundException, UnresolvedLinkException, IOException {
    return myFs.getFsStatus(f);
  }

  @Override
  protected FsStatus getFsStatus() throws IOException {
    return myFs.getFsStatus();
  }

  @Override
  protected FsServerDefaults getServerDefaults() throws IOException {
    return myFs.getServerDefaults();
  }

  @Override
  protected int getUriDefaultPort() {
    return myFs.getUriDefaultPort();
  }

  @Override
  protected URI getUri() {
    return myFs.getUri();
  }

  @Override
  protected void checkPath(Path path) {
    myFs.checkPath(path);
  }

  @Override
  protected String getUriPath(final Path p) {
    return myFs.getUriPath(p);
  }

  @Override
  protected FileStatus[] listStatus(Path f)
      throws IOException, UnresolvedLinkException {
    checkPath(f);
    return myFs.listStatus(f);
  }

  @Override
  protected void mkdir(Path dir, FsPermission permission, boolean createParent)
    throws IOException, UnresolvedLinkException {
    checkPath(dir);
    myFs.mkdir(dir, permission, createParent);

  }

  @Override
  protected FSDataInputStream open(final Path f) throws AccessControlException,
    FileNotFoundException, UnresolvedLinkException, IOException {
    checkPath(f);
    return myFs.open(f);
  }

  @Override
  protected FSDataInputStream open(Path f, int bufferSize)
    throws IOException, UnresolvedLinkException {
    checkPath(f);
    return myFs.open(f, bufferSize);
  }

  @Override
  protected void renameInternal(Path src, Path dst)
    throws IOException, UnresolvedLinkException {
    checkPath(src);
    checkPath(dst);
    myFs.rename(src, dst, Options.Rename.NONE);
  }

  @Override
  protected void renameInternal(final Path src, final Path dst,
      boolean overwrite) throws AccessControlException,
      FileAlreadyExistsException, FileNotFoundException,
      ParentNotDirectoryException, UnresolvedLinkException, IOException {
    myFs.renameInternal(src, dst, overwrite);
  }

  @Override
  protected void setOwner(Path f, String username, String groupname)
    throws IOException, UnresolvedLinkException {
    checkPath(f);
    myFs.setOwner(f, username, groupname);

  }

  @Override
  protected void setPermission(Path f, FsPermission permission)
    throws IOException, UnresolvedLinkException {
    checkPath(f);
    myFs.setPermission(f, permission);
  }

  @Override
  protected boolean setReplication(Path f, short replication)
    throws IOException, UnresolvedLinkException {
    checkPath(f);
    return myFs.setReplication(f, replication);
  }

  @Override
  protected void setTimes(Path f, long mtime, long atime)
      throws IOException, UnresolvedLinkException {
    checkPath(f);
    myFs.setTimes(f, mtime, atime);
  }

  @Override
  protected void setVerifyChecksum(boolean verifyChecksum)
      throws IOException, UnresolvedLinkException {
    myFs.setVerifyChecksum(verifyChecksum);
  }

  @Override
  protected boolean supportsSymlinks() {
    return myFs.supportsSymlinks();
  }

  @Override
  protected void createSymlink(Path target, Path link, boolean createParent)
    throws IOException, UnresolvedLinkException {
    myFs.createSymlink(target, link, createParent);
  }

  @Override
  protected Path getLinkTarget(final Path f) throws IOException {
    return myFs.getLinkTarget(f);
  }
}
