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

package org.apache.amoro.maintainer;

import org.apache.amoro.ServerTableIdentifier;
import org.apache.amoro.TableFormat;
import org.apache.amoro.api.CatalogMeta;
import org.apache.amoro.config.TableConfiguration;
import org.apache.amoro.maintainer.api.MaintainerExecutor;
import org.apache.amoro.maintainer.api.TableMaintainerConfig;
import org.apache.amoro.maintainer.api.TableMaintainerExecutor;
import org.apache.amoro.maintainer.factorys.SparkIcebergFactory;
import org.apache.amoro.maintainer.iceberg.ConfigUtils;
import org.apache.amoro.maintainer.iceberg.SparkExpireSnapshotsFactory;
import org.apache.amoro.optimizing.IcebergExpireSnapshotInput;
import org.apache.amoro.optimizing.IcebergExpireSnapshotsOutput;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.actions.SparkActions;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;

public class SparkTableMaintainer {

  private static final Logger LOG = LoggerFactory.getLogger(SparkTableMaintainer.class);

  public static void main(String[] args) throws Exception {
    // 需要知道不同的端口 OPTIMER URL 和 maintainer 不一样
    TableMaintainerConfig config = new TableMaintainerConfig(args);
    TableFormat tableFormat = TableFormat.valueOf(config.getType());
    ServerTableIdentifier serverTableIdentifier =
        ServerTableIdentifier.of(
            config.getId(),
            config.getCatalog(),
            config.getDatabase(),
            config.getTable(),
            tableFormat);
    LOG.info("Starting Spark Table Maintainer,Server Table Identifier: {}", serverTableIdentifier);
    // 获取信息 构建CatalogMeta，根据不同的CatalogMeta加载不同的东西
    TableMaintainerExecutor executor = new TableMaintainerExecutor(config);
    // use ams optimizer port
    CatalogMeta catalogMeta = executor.loadMeta(config.getAmsUrl() + "/" + config.getCatalog());

    // 根据工厂类创建不同的SparkSession，我们可以分为PaimonFactory、IcebergFactory、MixedIcebergFactory
    String[] formats = catalogMeta.getCatalogProperties().get("table-formats").split(",");
    if (Arrays.stream(formats).noneMatch(x -> x.equals(config.getType()))) {
      throw new RuntimeException("Not support table format: " + config.getType());
    }

    SparkSession spark =
        SparkIcebergFactory.getSparkSessionInstance(serverTableIdentifier, catalogMeta);

    // Get catalogMeta to Set Config with Diff Session Catalog
    String catalogName = catalogMeta.getCatalogName();
    JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
    if (!jsc.getConf().getBoolean("spark.dynamicAllocation.enabled", false)) {
      LOG.warn(
          "To better utilize computing resources, it is recommended to enable 'spark.dynamicAllocation.enabled' "
              + "and set 'spark.dynamicAllocation.maxExecutors' equal to 'OPTIMIZER_EXECUTION_PARALLEL'");
    }
    if (tableFormat.in(TableFormat.ICEBERG)) {
      spark.catalog().setCurrentCatalog("torrent_iceberg");
    } else {
      throw new RuntimeException("Not support table format: " + tableFormat);
    }
    Catalog sparkCatalog = Spark3Util.loadIcebergCatalog(spark, catalogName);
    Table originalTable =
        sparkCatalog.loadTable(TableIdentifier.of(config.getDatabase(), config.getTable()));
    TableConfiguration configuration = ConfigUtils.parseTableConfig(originalTable.properties());
    SparkActions sparkActions = SparkActions.get(spark);
    SparkExpireSnapshotsFactory expireSnapshotsFactory =
        new SparkExpireSnapshotsFactory(sparkActions, spark);
    MaintainerExecutor<IcebergExpireSnapshotsOutput> snapshotsFactoryExecutor =
        expireSnapshotsFactory.createExecutor(
            new IcebergExpireSnapshotInput(
                config.getCatalog(),
                config.getDatabase(),
                config.getTable(),
                tableFormat,
                new HashMap<>(),
                catalogMeta,
                originalTable,
                30L,
                10,
                new HashSet<>(),
                4));
    IcebergExpireSnapshotsOutput expireSnapshotsOutput = snapshotsFactoryExecutor.execute();
    System.out.println(expireSnapshotsOutput);
    jsc.stop();
  }
}
