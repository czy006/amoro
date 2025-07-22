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

package org.apache.amoro.maintainer.factorys;

import org.apache.amoro.ServerTableIdentifier;
import org.apache.amoro.TableFormat;
import org.apache.amoro.api.CatalogMeta;
import org.apache.spark.sql.SparkSession;

import java.util.Base64;
import java.util.Map;

public class SparkIcebergFactory {

  public static SparkSession getSparkSessionInstance(
      ServerTableIdentifier tableIdentifier, CatalogMeta catalogMeta) {
    SparkSession.Builder builder =
        SparkSession.builder()
            .appName(
                String.format(
                    "amoro-spark-table-maintainer-%s-%s",
                    tableIdentifier.toString(), System.currentTimeMillis()));
    if (tableIdentifier.getFormat().equals(TableFormat.ICEBERG)) {
      builder.config(
          "spark.sql.extensions",
          "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions");
      builder.config(
          String.format("spark.sql.catalog.%s", tableIdentifier.getCatalog()),
          "org.apache.iceberg.spark" + ".SparkCatalog");
    } else if (tableIdentifier.getFormat().equals(TableFormat.PAIMON)) {
      builder.config(
          "spark.sql.extensions",
          "org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions");
      builder.config(
          String.format("spark.sql.catalog.%s", tableIdentifier.getCatalog()),
          "org.apache.paimon.spark" + ".SparkCatalog");
    }
    builder.config("spark.sql.catalog.spark_catalog.type", "hive");
    builder.config(
        "spark.sql.catalog.spark_catalog.uri",
        "thrift://txy-hn1-bigdata-hdp11-mhd-prd-11.myhll.cn:9083,thrift://txy-hn1-bigdata-hdp11-mhd-prd-02.myhll"
            + ".cn:9083,thrift://txy-hn1-bigdata-hdp11-mhd-prd-03.myhll.cn:9083,thrift://txy-hn1-bigdata-hdp11-mhd-prd-04.myhll.cn:9083,thrift://txy-hn1-bigdata-hdp11-mhd-prd-05.myhll.cn:9083,thrift://txy-hn1-bigdata-hdp11-mhd-prd-06.myhll.cn:9083,thrift://txy-hn1-bigdata-hdp11-mhd-prd-07.myhll.cn:9083,thrift://txy-hn1-bigdata-hdp11-mhd-prd-08.myhll.cn:9083,thrift://txy-hn1-bigdata-hdp11-mhd-prd-09.myhll.cn:9083,thrift://txy-hn1-bigdata-hdp11-mhd-prd-10.myhll.cn:9083,thrift://txy-hn1-bigdata-hdp11-mhd-prd-01.myhll.cn:9083");
    return builder.getOrCreate();
  }

  // 解析Hive Conf
  private static void parseHiveConf(CatalogMeta catalogMeta) {
    Map<String, String> storageConfigs = catalogMeta.getStorageConfigs();
    String catalogType = catalogMeta.getCatalogType();
    if (catalogType.equals("hive")) {
      if (storageConfigs.containsKey("hive.site")) {
        byte[] decode = Base64.getUrlDecoder().decode(storageConfigs.get("hive.site"));
        System.out.println(decode);
      }
    }
  }
}
