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

package org.apache.amoro.server;

import org.apache.amoro.Action;
import org.apache.amoro.IcebergActions;
import org.apache.amoro.ServerTableIdentifier;
import org.apache.amoro.TableFormat;
import org.apache.amoro.api.AmoroException;
import org.apache.amoro.api.ExecutorTaskResult;
import org.apache.amoro.api.MaintainerService;
import org.apache.amoro.api.TableIdentifier;
import org.apache.amoro.config.Configurations;
import org.apache.amoro.process.ProcessStatus;
import org.apache.amoro.process.TableProcessState;
import org.apache.amoro.server.catalog.CatalogManager;
import org.apache.amoro.server.persistence.PersistentBase;
import org.apache.amoro.server.persistence.mapper.ProcessStateMapper;
import org.apache.amoro.shade.thrift.org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultTableMaintainerService extends PersistentBase
    implements MaintainerService.Iface {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultTableMaintainerService.class);

  private final CatalogManager catalogManager;
  private final Configurations serviceConfig;

  public DefaultTableMaintainerService(
      Configurations serviceConfig, CatalogManager catalogManager) {
    this.serviceConfig = serviceConfig;
    this.catalogManager = catalogManager;
  }

  @Override
  public void ping() throws TException {}

  @Override
  public void ackTask(ExecutorTaskResult taskResult) throws AmoroException, TException {
    TableIdentifier tableIdentifier = taskResult.getTableIdentifier();
    ServerTableIdentifier serverTableIdentifier =
        ServerTableIdentifier.of(
            taskResult.getTableId(),
            tableIdentifier.getCatalog(),
            tableIdentifier.getDatabase(),
            tableIdentifier.getTableName(),
            TableFormat.ICEBERG);
    // 获取当前的PROCESS_ID 以PROCESS ID 作为主键进行更新
    long processId = taskResult.getProcessId();
    int status = taskResult.getStatus();
    Action action = IcebergActions.actions.get(taskResult.getAction());
    TableProcessState tableProcessState =
        new TableProcessState(processId, action, serverTableIdentifier);
    tableProcessState.setStatus(ProcessStatus.valueOf("RUNNING"));
    doAs(ProcessStateMapper.class, mapper -> mapper.updateProcessRunning(tableProcessState));
  }

  /**
   * 完成Task的一些情况
   *
   * @param taskResult
   * @throws AmoroException
   * @throws TException
   */
  @Override
  public void completeTask(ExecutorTaskResult taskResult) throws AmoroException, TException {
    TableIdentifier tableIdentifier = taskResult.getTableIdentifier();
    ServerTableIdentifier serverTableIdentifier =
        ServerTableIdentifier.of(
            taskResult.getTableId(),
            tableIdentifier.getCatalog(),
            tableIdentifier.getDatabase(),
            tableIdentifier.getTableName(),
            TableFormat.ICEBERG);
    Action action = IcebergActions.actions.get(taskResult.getAction());
    TableProcessState tableProcessState =
        new TableProcessState(taskResult.getProcessId(), action, serverTableIdentifier);
    if (taskResult.getErrorMessage() != null) {
      tableProcessState.setCompleted(taskResult.getErrorMessage());
    } else {
      tableProcessState.setCompleted();
    }
    doAs(ProcessStateMapper.class, mapper -> mapper.updateProcessRunning(tableProcessState));
  }
}
