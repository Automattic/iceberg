/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.spark;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.spark.sql.connector.catalog.Identifier;

/**
 * Verifies, before a table is dropped, that the current user has the filesystem permissions
 * required to delete the table's files.
 *
 * <p>With a Hive catalog, {@code DROP TABLE} removes the metastore entry without deleting data, and
 * {@code DROP TABLE PURGE} removes the metastore entry <em>before</em> deleting files client-side.
 * In both cases a user without delete permission on the table location ends up with orphaned data
 * that is no longer reachable through any catalog. Failing the drop up front keeps the catalog
 * entry and the data consistent.
 *
 * <p>The check calls {@link FileSystem#access(Path, FsAction)}, which on HDFS is a {@code
 * checkAccess} RPC evaluated by the NameNode against the caller's UGI (including groups and ACLs).
 * Deleting a directory tree requires WRITE and EXECUTE on the directory itself and on its parent,
 * so both are checked. Sticky-bit protection is evaluated the way the NameNode's {@code
 * FSPermissionChecker} does on delete: when a directory has the sticky bit set, an entry can only
 * be removed by the entry's owner or the directory's owner. That rule is applied to the parent
 * directory (unlinking the table directory) and to the table directory's immediate children
 * (purging its contents). HDFS superusers bypass sticky-bit checks server-side but cannot be
 * detected here, so they may be blocked spuriously.
 *
 * <p>Only {@code hdfs} locations are enforced: other filesystems fall back to a client-side
 * mode-bit guess that is not authoritative. The check inspects only the top level, not the full
 * tree; Iceberg-written trees have uniform ownership in practice.
 *
 * <p>Locations that are null, empty, or missing pass through so that dangling metastore entries can
 * still be cleaned up. Service accounts (usernames starting with {@code bot-}) are exempt from the
 * check entirely.
 */
final class DropTablePermissionValidator {

  private static final Set<String> ENFORCED_SCHEMES = ImmutableSet.of("hdfs");
  private static final String EXEMPT_USER_PREFIX = "bot-";

  private DropTablePermissionValidator() {}

  static void validate(Identifier ident, String location, Configuration conf) {
    if (location == null || location.isEmpty()) {
      return;
    }

    Path path = new Path(location);
    FileSystem fs;
    try {
      fs = path.getFileSystem(conf);
    } catch (IOException e) {
      throw new UncheckedIOException(
          String.format("Cannot drop table %s: failed to access filesystem for %s", ident, path),
          e);
    }

    validate(ident, path, fs);
  }

  static void validate(Identifier ident, Path path, FileSystem fs) {
    if (!ENFORCED_SCHEMES.contains(fs.getUri().getScheme())) {
      return;
    }

    if (currentUser().startsWith(EXEMPT_USER_PREFIX)) {
      return;
    }

    try {
      FileStatus tableDir;
      try {
        tableDir = fs.getFileStatus(path);
      } catch (FileNotFoundException e) {
        return;
      }

      fs.access(path, FsAction.WRITE_EXECUTE);

      Path parent = path.getParent();
      if (parent != null) {
        fs.access(parent, FsAction.WRITE_EXECUTE);
        checkStickyBit(ident, fs.getFileStatus(parent), tableDir);
      }

      checkStickyBitOnChildren(ident, fs, tableDir);
    } catch (FileNotFoundException e) {
      // the location disappeared concurrently; nothing left to protect
    } catch (AccessControlException e) {
      throw new ValidationException(
          e,
          "Cannot drop table %s: user %s has no permission to delete data at %s",
          ident,
          currentUser(),
          path);
    } catch (IOException e) {
      throw new UncheckedIOException(
          String.format(
              "Cannot drop table %s: failed to check delete permission on %s", ident, path),
          e);
    }
  }

  /**
   * Mirrors the NameNode's sticky-bit rule for deleting {@code entry} from {@code dir}: allowed
   * only for the owner of the entry or the owner of the directory.
   */
  private static void checkStickyBit(Identifier ident, FileStatus dir, FileStatus entry) {
    if (!dir.getPermission().getStickyBit()) {
      return;
    }

    String user = currentUser();
    if (user.equals(dir.getOwner()) || user.equals(entry.getOwner())) {
      return;
    }

    throw new ValidationException(
        "Cannot drop table %s: sticky bit on %s prevents user %s from deleting %s (owned by %s)",
        ident, dir.getPath(), user, entry.getPath(), entry.getOwner());
  }

  private static void checkStickyBitOnChildren(Identifier ident, FileSystem fs, FileStatus tableDir)
      throws IOException {
    if (!tableDir.getPermission().getStickyBit()) {
      return;
    }

    String user = currentUser();
    if (user.equals(tableDir.getOwner())) {
      return;
    }

    for (FileStatus child : fs.listStatus(tableDir.getPath())) {
      if (!user.equals(child.getOwner())) {
        throw new ValidationException(
            "Cannot drop table %s: sticky bit on %s prevents user %s from deleting %s (owned by %s)",
            ident, tableDir.getPath(), user, child.getPath(), child.getOwner());
      }
    }
  }

  private static String currentUser() {
    try {
      return UserGroupInformation.getCurrentUser().getShortUserName();
    } catch (IOException e) {
      return "<unknown>";
    }
  }
}
