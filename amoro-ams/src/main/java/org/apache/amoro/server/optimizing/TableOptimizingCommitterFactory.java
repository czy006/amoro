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

package org.apache.amoro.server.optimizing;

import org.apache.amoro.ServerTableIdentifier;
import org.apache.amoro.optimizing.RewriteStageTask;
import org.apache.amoro.optimizing.TableOptimizingCommitter;

import java.util.Collection;
import java.util.Map;

/**
 * Factory for creating format-specific committers. Decouples OptimizingTableProcess from
 * format-specific commit implementations like UnKeyedTableCommit and KeyedTableCommit.
 */
@FunctionalInterface
public interface TableOptimizingCommitterFactory {
  TableOptimizingCommitter createCommitter(
      ServerTableIdentifier tableIdentifier,
      long targetSnapshotId,
      long targetChangeSnapshotId,
      Collection<TaskRuntime<RewriteStageTask>> tasks,
      Map<String, Long> fromSequence,
      Map<String, Long> toSequence);
}
