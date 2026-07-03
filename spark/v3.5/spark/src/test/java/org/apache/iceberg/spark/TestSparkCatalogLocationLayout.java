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

import java.io.File;
import java.nio.file.Path;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestSparkCatalogLocationLayout extends TestBase {

  private static final String CATALOG = "layout_check";
  private static final String DB = "default";

  @TempDir private static Path warehouseRoot;

  // The validator only inspects user-supplied LOCATION values. HadoopCatalog rejects custom
  // LOCATION outright, so the matching-suffix accept path is covered by the unit tests in
  // TestLocationLayoutValidator. These integration tests cover:
  //   - the validator fires from each create/alter entry point on a mismatched location
  //   - null pass-through (CREATE without LOCATION) still works through Spark + HadoopCatalog

  @BeforeAll
  public static void registerCatalog() {
    String warehouse = "file:" + new File(warehouseRoot.toFile(), "warehouse").getAbsolutePath();
    String prefix = "spark.sql.catalog." + CATALOG;
    spark.conf().set(prefix, SparkCatalog.class.getName());
    spark.conf().set(prefix + ".type", "hadoop");
    spark.conf().set(prefix + ".warehouse", warehouse);
    spark.conf().set(prefix + ".cache-enabled", "false");
  }

  @AfterAll
  public static void unregisterCatalog() {
    String prefix = "spark.sql.catalog." + CATALOG;
    spark.conf().unset(prefix);
    spark.conf().unset(prefix + ".type");
    spark.conf().unset(prefix + ".warehouse");
    spark.conf().unset(prefix + ".cache-enabled");
  }

  @AfterEach
  public void dropTables() {
    sql("DROP TABLE IF EXISTS %s.%s.t", CATALOG, DB);
    sql("DROP TABLE IF EXISTS %s.%s.t_src", CATALOG, DB);
    sql("DROP TABLE IF EXISTS %s.%s.t_new", CATALOG, DB);
  }

  @Test
  public void createTableWithMismatchedLocationIsRejected() {
    assertThatThrownBy(
            () ->
                sql(
                    "CREATE TABLE %s.%s.t (id bigint) USING iceberg LOCATION 'file:/forbidden/x'",
                    CATALOG, DB))
        .hasMessageContaining("default.db/t");
  }

  @Test
  public void createTableWithoutLocationSucceeds() {
    sql("CREATE TABLE %s.%s.t (id bigint) USING iceberg", CATALOG, DB);

    assertThat(sql("SHOW TABLES IN %s.%s", CATALOG, DB)).isNotEmpty();
  }

  @Test
  public void ctasWithMismatchedLocationIsRejected() {
    sql("CREATE TABLE %s.%s.t_src (id bigint) USING iceberg", CATALOG, DB);

    assertThatThrownBy(
            () ->
                sql(
                    "CREATE TABLE %s.%s.t USING iceberg LOCATION 'file:/forbidden/x' AS SELECT * FROM %s.%s.t_src",
                    CATALOG, DB, CATALOG, DB))
        .hasMessageContaining("default.db/t");
  }

  @Test
  public void rtasWithMismatchedLocationIsRejected() {
    sql("CREATE TABLE %s.%s.t_src (id bigint) USING iceberg", CATALOG, DB);
    sql("CREATE TABLE %s.%s.t (id bigint) USING iceberg", CATALOG, DB);

    assertThatThrownBy(
            () ->
                sql(
                    "REPLACE TABLE %s.%s.t USING iceberg LOCATION 'file:/forbidden/x' AS SELECT * FROM %s.%s.t_src",
                    CATALOG, DB, CATALOG, DB))
        .hasMessageContaining("default.db/t");
  }

  @Test
  public void createNewSuffixedTableWithMismatchedLocationListsBothAllowedSuffixes() {
    // Swap-via-rename pattern stages `<t>_new` at the canonical `<t>` location. Verify the
    // validator wires through SparkCatalog for `_new` identifiers and surfaces both the
    // canonical and rebuild suffixes in the error.
    assertThatThrownBy(
            () ->
                sql(
                    "CREATE TABLE %s.%s.t_new (id bigint) USING iceberg LOCATION 'file:/forbidden/x'",
                    CATALOG, DB))
        .hasMessageContaining("default.db/t'")
        .hasMessageContaining("default.db/t_new'");
  }

  @Test
  public void alterTableSetLocationMismatchedIsRejected() {
    sql("CREATE TABLE %s.%s.t (id bigint) USING iceberg", CATALOG, DB);

    assertThatThrownBy(
            () -> sql("ALTER TABLE %s.%s.t SET LOCATION 'file:/forbidden/x'", CATALOG, DB))
        .hasMessageContaining("default.db/t");
  }
}
