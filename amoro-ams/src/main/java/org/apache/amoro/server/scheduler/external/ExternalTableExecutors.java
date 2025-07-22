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

package org.apache.amoro.server.scheduler.external;

import static org.apache.amoro.IcebergActions.DELETE_ORPHANS;

import org.apache.amoro.config.Configurations;
import org.apache.amoro.resource.ResourceManager;
import org.apache.amoro.server.manager.StandaloneTableMaintainerExternalResource;
import org.apache.amoro.server.table.TableService;

/** ExternalTableExecutors Submit Jobs to Clusters */
public class ExternalTableExecutors {

  private static final ExternalTableExecutors instance = new ExternalTableExecutors();
  private JavaProcessExecutor javaProcessExecutor;

  public static ExternalTableExecutors getInstance() {
    return instance;
  }

  public void setup(
      ResourceManager resourceManager, TableService tableService, Configurations conf) {
    StandaloneTableMaintainerExternalResource standaloneTableMaintainerExternalResource =
        new StandaloneTableMaintainerExternalResource();
    this.javaProcessExecutor =
        new JavaProcessExecutor(
            resourceManager,
            standaloneTableMaintainerExternalResource,
            DELETE_ORPHANS,
            tableService,
            2);
  }

  public JavaProcessExecutor getJavaProcessExecutor() {
    return javaProcessExecutor;
  }
}
