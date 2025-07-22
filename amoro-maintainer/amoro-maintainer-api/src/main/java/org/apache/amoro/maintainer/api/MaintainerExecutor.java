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

import java.io.Serializable;

/**
 * Executor interface for table maintenance operations. Provides the core execution contract for all
 * maintenance operations across different data lake implementations.
 *
 * <p>This interface defines the standard pattern for executing maintenance operations such as
 * snapshot expiration, orphan file cleanup, and data expiration. Implementations are expected to
 * handle data lake specific execution logic while providing consistent input/output behavior.
 *
 * <p>Features: - Multi-lake support through pluggable implementations - Comprehensive error
 * handling and status reporting - Performance metrics collection - Dry run support for testing -
 * Configurable execution parameters
 */
public interface MaintainerExecutor<O extends TableMaintainerOptimizing.MaintainerOutput>
    extends Serializable {

  /**
   * Execute the maintenance operation with the configured input
   *
   * @return operation results containing comprehensive statistics and status
   * @throws Exception if operation fails
   */
  O execute();

  /**
   * Get the type of maintenance operation this executor performs
   *
   * @return maintainer type (e.g., EXPIRE_SNAPSHOTS, CLEAN_ORPHAN_FILES)
   */
  MaintainerType type();
}
