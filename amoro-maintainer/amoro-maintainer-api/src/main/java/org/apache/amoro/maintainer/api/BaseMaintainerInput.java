/*
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

package org.apache.amoro.maintainer.api;

import org.apache.amoro.TableFormat;
import org.apache.amoro.shade.guava32.com.google.common.base.MoreObjects;

import java.util.Map;

/**
 * Base input class for table maintenance operations. Provides common configuration parameters for
 * all maintenance operations across different data lake implementations.
 *
 * <p>This class includes universal optimization parameters such as: - Snapshot retention
 * configuration - Orphan file cleanup settings - Execution parallelism and timeout settings - Data
 * lake specific configurations
 */
public class BaseMaintainerInput implements TableMaintainerOptimizing.MaintainerInput {

  private final String database;
  private final String table;
  private final String catalog;
  private final TableFormat tableFormat;
  private final Map<String, String> options;

  /**
   * Construct base maintainer input with essential table identification
   *
   * @param catalog catalog name
   * @param database database name
   * @param table table name
   */
  public BaseMaintainerInput(
      String catalog,
      String database,
      String table,
      TableFormat tableFormat,
      Map<String, String> options) {
    this.catalog = catalog;
    this.database = database;
    this.table = table;
    this.tableFormat = tableFormat;
    this.options = options;
  }

  public String getDatabase() {
    return database;
  }

  public String getTable() {
    return table;
  }

  public String getCatalog() {
    return catalog;
  }

  public TableFormat getTableFormat() {
    return tableFormat;
  }

  @Override
  public void option(String name, String value) {
    options.put(name, value);
  }

  @Override
  public void options(Map<String, String> options) {
    this.options.putAll(options);
  }

  @Override
  public Map<String, String> getOptions() {
    return options;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("catalog", catalog)
        .add("database", database)
        .add("table", table)
        .add("tableFormat", tableFormat.name())
        .add("options", options)
        .toString();
  }
}
