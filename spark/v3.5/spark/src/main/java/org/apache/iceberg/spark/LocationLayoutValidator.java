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

import org.apache.iceberg.util.LocationUtil;
import org.apache.spark.sql.connector.catalog.Identifier;

/**
 * Enforces that user-supplied table locations follow the Hive-style layout: a table
 * {@code <db>.<table>} must reside at a path ending in {@code <db>.db/<table>} or
 * {@code <db>.db/<table>_new} (the latter supports rebuild/swap workflows).
 *
 * <p>Null and empty locations pass through unchanged so the underlying catalog can compute the
 * default location from the database's metadata.
 */
final class LocationLayoutValidator {

  private LocationLayoutValidator() {}

  static String validateAndReturn(Identifier ident, String location) {
    if (location == null || location.isEmpty()) {
      return location;
    }

    String[] namespace = ident.namespace();
    if (namespace.length == 0) {
      return location;
    }

    String db = namespace[namespace.length - 1];
    String table = ident.name();
    String requiredSuffix = db + ".db/" + table;
    String rebuildSuffix = requiredSuffix + "_new";

    String normalized = LocationUtil.stripTrailingSlash(location);
    if (!matchesSuffix(normalized, requiredSuffix) && !matchesSuffix(normalized, rebuildSuffix)) {
      throw new IllegalArgumentException(
          String.format(
              "Location %s for table %s.%s must end with '%s' or '%s'",
              location, db, table, requiredSuffix, rebuildSuffix));
    }

    return location;
  }

  private static boolean matchesSuffix(String normalized, String suffix) {
    return normalized.equals(suffix) || normalized.endsWith("/" + suffix);
  }
}
