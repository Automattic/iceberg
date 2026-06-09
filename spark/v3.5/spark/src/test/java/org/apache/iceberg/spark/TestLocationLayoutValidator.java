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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.spark.sql.connector.catalog.Identifier;
import org.junit.jupiter.api.Test;

public class TestLocationLayoutValidator {

  private static final Identifier MYDB_MYTABLE = Identifier.of(new String[] {"mydb"}, "mytable");
  private static final Identifier MYDB_MYTABLE_NEW =
      Identifier.of(new String[] {"mydb"}, "mytable_new");

  @Test
  public void allowsLocationEndingInExpectedSuffix() {
    String location = "s3://bucket/warehouse/mydb.db/mytable";

    assertThat(LocationLayoutValidator.validateAndReturn(MYDB_MYTABLE, location))
        .isEqualTo(location);
  }

  @Test
  public void allowsTrailingSlashAndReturnsOriginal() {
    String location = "s3://bucket/warehouse/mydb.db/mytable/";

    assertThat(LocationLayoutValidator.validateAndReturn(MYDB_MYTABLE, location))
        .isSameAs(location);
  }

  @Test
  public void allowsRebuildSuffixWithUnderscoreNew() {
    String location = "s3://bucket/warehouse/mydb.db/mytable_new";

    assertThat(LocationLayoutValidator.validateAndReturn(MYDB_MYTABLE, location))
        .isEqualTo(location);
  }

  @Test
  public void allowsRebuildSuffixWithTrailingSlash() {
    String location = "s3://bucket/warehouse/mydb.db/mytable_new/";

    assertThat(LocationLayoutValidator.validateAndReturn(MYDB_MYTABLE, location))
        .isSameAs(location);
  }

  @Test
  public void rejectsRebuildSuffixForDifferentTable() {
    assertThatThrownBy(
            () ->
                LocationLayoutValidator.validateAndReturn(
                    MYDB_MYTABLE, "s3://bucket/warehouse/mydb.db/othertable_new"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("mydb.db/mytable_new");
  }

  @Test
  public void rejectsLocationOutsideDbDirectory() {
    assertThatThrownBy(
            () ->
                LocationLayoutValidator.validateAndReturn(
                    MYDB_MYTABLE, "s3://bucket/random/mytable"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("mydb.db/mytable")
        .hasMessageContaining("s3://bucket/random/mytable");
  }

  @Test
  public void rejectsWhenTableNameMismatchesIdentifier() {
    assertThatThrownBy(
            () ->
                LocationLayoutValidator.validateAndReturn(
                    MYDB_MYTABLE, "s3://bucket/warehouse/mydb.db/somethingelse"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("must end with");
  }

  @Test
  public void rejectsWhenDbNameMismatchesIdentifier() {
    assertThatThrownBy(
            () ->
                LocationLayoutValidator.validateAndReturn(
                    MYDB_MYTABLE, "s3://bucket/warehouse/otherdb.db/mytable"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("must end with");
  }

  @Test
  public void rejectsPrefixCollisionWithoutSlashBoundary() {
    // "xmydb.db/mytable" must not be accepted just because it ends in "mydb.db/mytable"
    assertThatThrownBy(
            () ->
                LocationLayoutValidator.validateAndReturn(
                    MYDB_MYTABLE, "s3://bucket/warehouse/xmydb.db/mytable"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("must end with");
  }

  @Test
  public void allowsExactSuffixWithoutPrefix() {
    // edge case: location is exactly the required suffix with no scheme/host
    String location = "mydb.db/mytable";

    assertThat(LocationLayoutValidator.validateAndReturn(MYDB_MYTABLE, location))
        .isEqualTo(location);
  }

  @Test
  public void passesNullThrough() {
    assertThat(LocationLayoutValidator.validateAndReturn(MYDB_MYTABLE, null)).isNull();
  }

  @Test
  public void passesEmptyThrough() {
    assertThat(LocationLayoutValidator.validateAndReturn(MYDB_MYTABLE, "")).isEqualTo("");
  }

  @Test
  public void usesLastNamespaceSegmentForMultiLevelNamespace() {
    Identifier ident = Identifier.of(new String[] {"cat", "parent", "mydb"}, "mytable");

    String allowed = "s3://bucket/parent/mydb.db/mytable";
    assertThat(LocationLayoutValidator.validateAndReturn(ident, allowed)).isEqualTo(allowed);

    assertThatThrownBy(
            () -> LocationLayoutValidator.validateAndReturn(ident, "s3://bucket/parent.db/mytable"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("must end with");
  }

  @Test
  public void allowsCanonicalLocationForNewSuffixedIdentifier() {
    // swap-via-rename: stage `mytable_new` at the canonical `mytable` location, then rename
    String location = "s3://bucket/warehouse/mydb.db/mytable";

    assertThat(LocationLayoutValidator.validateAndReturn(MYDB_MYTABLE_NEW, location))
        .isEqualTo(location);
  }

  @Test
  public void allowsNewSuffixLocationForNewSuffixedIdentifier() {
    String location = "s3://bucket/warehouse/mydb.db/mytable_new";

    assertThat(LocationLayoutValidator.validateAndReturn(MYDB_MYTABLE_NEW, location))
        .isEqualTo(location);
  }

  @Test
  public void rejectsDoubleNewSuffixLocationForNewSuffixedIdentifier() {
    assertThatThrownBy(
            () ->
                LocationLayoutValidator.validateAndReturn(
                    MYDB_MYTABLE_NEW, "s3://bucket/warehouse/mydb.db/mytable_new_new"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("mydb.db/mytable")
        .hasMessageContaining("mydb.db/mytable_new");
  }

  @Test
  public void passesWhenNamespaceIsEmpty() {
    Identifier ident = Identifier.of(new String[0], "mytable");

    String location = "s3://bucket/anywhere/mytable";
    assertThat(LocationLayoutValidator.validateAndReturn(ident, location)).isEqualTo(location);
  }
}
