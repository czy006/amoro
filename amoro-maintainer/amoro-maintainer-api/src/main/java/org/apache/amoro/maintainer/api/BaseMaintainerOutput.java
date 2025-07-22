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

import java.util.HashMap;
import java.util.Map;

public class BaseMaintainerOutput implements TableMaintainerOptimizing.MaintainerOutput {

  private String catalog;
  private String database;
  private String table;
  private MaintainerType type;
  private Long startTime;
  private Long lastTime;
  private Long endTime;
  private Long executionTimeMs;
  private Boolean success;
  private String errorMessage;
  private Map<String, String> summary;

  public BaseMaintainerOutput(
      String catalog,
      String database,
      String table,
      MaintainerType type,
      Long startTime,
      Long lastTime,
      Long endTime,
      Long executionTimeMs,
      Boolean success,
      String errorMessage,
      Map<String, String> summary) {
    this.catalog = catalog;
    this.database = database;
    this.table = table;
    this.type = type;
    this.startTime = startTime;
    this.lastTime = lastTime;
    this.endTime = endTime;
    this.executionTimeMs = executionTimeMs;
    this.success = success;
    this.errorMessage = errorMessage;
    this.summary = summary;
  }

  public String getCatalog() {
    return catalog;
  }

  public void setCatalog(String catalog) {
    this.catalog = catalog;
  }

  public String getDatabase() {
    return database;
  }

  public void setDatabase(String database) {
    this.database = database;
  }

  public String getTable() {
    return table;
  }

  public void setTable(String table) {
    this.table = table;
  }

  public MaintainerType getType() {
    return type;
  }

  public void setType(MaintainerType type) {
    this.type = type;
  }

  public Long getStartTime() {
    return startTime;
  }

  public void setStartTime(Long startTime) {
    this.startTime = startTime;
  }

  public Long getLastTime() {
    return lastTime;
  }

  public void setLastTime(Long lastTime) {
    this.lastTime = lastTime;
  }

  public Long getEndTime() {
    return endTime;
  }

  public void setEndTime(Long endTime) {
    this.endTime = endTime;
  }

  public Long getExecutionTimeMs() {
    return executionTimeMs;
  }

  public void setExecutionTimeMs(Long executionTimeMs) {
    this.executionTimeMs = executionTimeMs;
  }

  public Boolean getSuccess() {
    return success;
  }

  public void setSuccess(Boolean success) {
    this.success = success;
  }

  public String getErrorMessage() {
    return errorMessage;
  }

  public void setErrorMessage(String errorMessage) {
    this.errorMessage = errorMessage;
  }

  public Map<String, String> getSummary() {
    return summary;
  }

  public void setSummary(Map<String, String> summary) {
    this.summary = summary;
  }

  @Override
  public Map<String, String> summary() {
    HashMap<String, String> summary = new HashMap<>(8);
    summary.put("catalog", catalog);
    summary.put("database", database);
    summary.put("table", table);
    summary.put("type", type.name());
    return summary;
  }
}
