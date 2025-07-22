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

import org.apache.amoro.AmsClient;
import org.apache.amoro.Constants;
import org.apache.amoro.PooledAmsClient;
import org.apache.amoro.api.CatalogMeta;
import org.apache.amoro.api.ExecutorTaskResult;
import org.apache.amoro.client.AmsThriftUrl;
import org.apache.amoro.shade.thrift.org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TableMaintainerExecutor {

  private static final Logger LOG = LoggerFactory.getLogger(TableMaintainerExecutor.class);
  private final AbstractTableMaintainerOperator abstractTableMaintainerOperator;
  private final TableMaintainerConfig config;

  public TableMaintainerExecutor(TableMaintainerConfig config) {
    this.config = config;
    this.abstractTableMaintainerOperator = new AbstractTableMaintainerOperator(config);
  }

  public void sendTaskResultToAms(ExecutorTaskResult taskResult) throws Exception {
    abstractTableMaintainerOperator.callAms(
        client -> {
          client.completeTask(taskResult);
          return null;
        });
  }

  /**
   * Load catalog meta from metastore. thrift://ams-host:port/catalog_name
   *
   * @param catalogUrl - catalog url
   * @return catalog meta
   */
  public CatalogMeta loadMeta(String catalogUrl) {
    AmsThriftUrl url = AmsThriftUrl.parse(catalogUrl, Constants.THRIFT_TABLE_SERVICE_NAME);
    if (url.catalogName() == null || url.catalogName().contains("/")) {
      throw new IllegalArgumentException("invalid catalog name " + url.catalogName());
    }
    AmsClient client = new PooledAmsClient(catalogUrl);
    try {
      return client.getCatalog(url.catalogName());
    } catch (TException e) {
      throw new IllegalStateException("failed when load catalog " + url.catalogName(), e);
    }
  }
}
