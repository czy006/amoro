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

/**
 * Core interface for table maintenance operations in data lakes. Defines the common behavior for
 * table maintainers across different data lake implementations (Iceberg, Paimon, etc.).
 *
 * <p>This interface provides abstraction for snapshot expiration, orphan file cleanup, and other
 * maintenance operations while allowing each data lake implementation to have its own specific
 * optimizations.
 */
public interface TableMaintainer {

  /** Status of table maintenance operations */
  enum Status {
    RUNNING, // Maintenance operation is currently running
    IDLE, // Maintainer is idle, ready for new operations
    PENDING, // Maintenance operation is queued and waiting to start
    FAILED, // Maintenance operation failed
    SUCCESS, // Maintenance operation completed successfully
    CANCELED // Maintenance operation was canceled (if parent process failed, all tasks except
    // SUCCESS tasks are CANCELED)
  }

  /**
   * Get the current status of the table maintainer
   *
   * @return current status
   */
  Status getStatus();

  /**
   * Get the table format this maintainer supports
   *
   * @return table format (e.g., ICEBERG, PAIMON, MIXED_ICEBERG)
   */
  String getTableFormat();

  /**
   * Check if this maintainer supports a specific maintenance type
   *
   * @param type the maintenance type to check
   * @return true if supported, false otherwise
   */
  boolean supports(MaintainerType type);
}
