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

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.security.AccessControlException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.junit.jupiter.api.Test;

public class TestDropTablePermissionValidator {

  private static final Identifier MYDB_MYTABLE = Identifier.of(new String[] {"mydb"}, "mytable");
  private static final Path TABLE_PATH = new Path("hdfs://ns1/warehouse/mydb.db/mytable");
  private static final Path PARENT_PATH = TABLE_PATH.getParent();

  private FileSystem mockFs(String scheme) {
    FileSystem fs = mock(FileSystem.class);
    when(fs.getUri()).thenReturn(URI.create(scheme + "://ns1"));
    return fs;
  }

  @Test
  public void passesWhenTableAndParentAreWritable() throws IOException {
    FileSystem fs = mockFs("hdfs");
    when(fs.exists(TABLE_PATH)).thenReturn(true);

    assertThatCode(() -> DropTablePermissionValidator.validate(MYDB_MYTABLE, TABLE_PATH, fs))
        .doesNotThrowAnyException();

    verify(fs).access(TABLE_PATH, FsAction.WRITE_EXECUTE);
    verify(fs).access(PARENT_PATH, FsAction.WRITE_EXECUTE);
  }

  @Test
  public void rejectsWhenTableDirIsNotWritable() throws IOException {
    FileSystem fs = mockFs("hdfs");
    when(fs.exists(TABLE_PATH)).thenReturn(true);
    doThrow(new AccessControlException("Permission denied"))
        .when(fs)
        .access(TABLE_PATH, FsAction.WRITE_EXECUTE);

    assertThatThrownBy(() -> DropTablePermissionValidator.validate(MYDB_MYTABLE, TABLE_PATH, fs))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("Cannot drop table mydb.mytable")
        .hasMessageContaining("no permission to delete data")
        .hasMessageContaining(TABLE_PATH.toString());
  }

  @Test
  public void rejectsWhenParentDirIsNotWritable() throws IOException {
    FileSystem fs = mockFs("hdfs");
    when(fs.exists(TABLE_PATH)).thenReturn(true);
    doThrow(new AccessControlException("Permission denied"))
        .when(fs)
        .access(PARENT_PATH, FsAction.WRITE_EXECUTE);

    assertThatThrownBy(() -> DropTablePermissionValidator.validate(MYDB_MYTABLE, TABLE_PATH, fs))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("no permission to delete data");
  }

  @Test
  public void skipsNonHdfsFilesystems() throws IOException {
    FileSystem fs = mockFs("s3a");

    assertThatCode(() -> DropTablePermissionValidator.validate(MYDB_MYTABLE, TABLE_PATH, fs))
        .doesNotThrowAnyException();

    verify(fs, never()).exists(any(Path.class));
    verify(fs, never()).access(any(Path.class), any(FsAction.class));
  }

  @Test
  public void passesWhenLocationDoesNotExist() throws IOException {
    FileSystem fs = mockFs("hdfs");
    when(fs.exists(TABLE_PATH)).thenReturn(false);

    assertThatCode(() -> DropTablePermissionValidator.validate(MYDB_MYTABLE, TABLE_PATH, fs))
        .doesNotThrowAnyException();

    verify(fs, never()).access(any(Path.class), any(FsAction.class));
  }

  @Test
  public void passesWhenLocationDisappearsConcurrently() throws IOException {
    FileSystem fs = mockFs("hdfs");
    when(fs.exists(TABLE_PATH)).thenReturn(true);
    doThrow(new FileNotFoundException("gone")).when(fs).access(TABLE_PATH, FsAction.WRITE_EXECUTE);

    assertThatCode(() -> DropTablePermissionValidator.validate(MYDB_MYTABLE, TABLE_PATH, fs))
        .doesNotThrowAnyException();
  }

  @Test
  public void failsOnFilesystemErrors() throws IOException {
    FileSystem fs = mockFs("hdfs");
    when(fs.exists(TABLE_PATH)).thenThrow(new IOException("RPC failed"));

    assertThatThrownBy(() -> DropTablePermissionValidator.validate(MYDB_MYTABLE, TABLE_PATH, fs))
        .isInstanceOf(UncheckedIOException.class)
        .hasMessageContaining("failed to check delete permission");
  }

  @Test
  public void passesNullAndEmptyLocationsThrough() {
    Configuration conf = new Configuration(false);

    assertThatCode(() -> DropTablePermissionValidator.validate(MYDB_MYTABLE, (String) null, conf))
        .doesNotThrowAnyException();
    assertThatCode(() -> DropTablePermissionValidator.validate(MYDB_MYTABLE, "", conf))
        .doesNotThrowAnyException();
  }
}
