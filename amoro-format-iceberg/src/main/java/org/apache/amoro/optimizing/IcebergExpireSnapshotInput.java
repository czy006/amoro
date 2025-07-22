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

package org.apache.amoro.optimizing;

import org.apache.amoro.TableFormat;
import org.apache.amoro.api.CatalogMeta;
import org.apache.amoro.maintainer.input.ExpireSnapshotInput;
import org.apache.iceberg.Table;

import java.util.Map;
import java.util.Set;

public class IcebergExpireSnapshotInput extends ExpireSnapshotInput {

  private final Table icebergTable;
  private final Integer maxConcurrentDeletes;

  public IcebergExpireSnapshotInput(
      String catalog,
      String database,
      String table,
      TableFormat tableFormat,
      Map<String, String> options,
      CatalogMeta catalogMeta,
      Table icebergTable,
      Long mustOlderThan,
      Integer minCount,
      Set<String> expireSnapshotNeedToExcludeFiles,
      Integer maxConcurrentDeletes) {
    super(
        catalog,
        database,
        table,
        tableFormat,
        options,
        catalogMeta,
        mustOlderThan,
        minCount,
        expireSnapshotNeedToExcludeFiles);
    this.icebergTable = icebergTable;
    this.maxConcurrentDeletes = maxConcurrentDeletes;
  }

  public Table getIcebergTable() {
    return icebergTable;
  }

  public Integer getMaxConcurrentDeletes() {
    return maxConcurrentDeletes;
  }
}
