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

package org.apache.amoro.formats.paimon.optimizing;

import org.apache.amoro.optimizing.OptimizingExecutor;

/**
 * Compaction executor for Paimon format tables. Executes a compaction task that merges sorted runs
 * according to Paimon's LSM-tree compaction strategy (UniversalCompaction).
 */
public class PaimonCompactionExecutor implements OptimizingExecutor<PaimonCompactionOutput> {

  private static final long serialVersionUID = 1L;

  private final PaimonCompactionInput input;

  public PaimonCompactionExecutor(PaimonCompactionInput input) {
    this.input = input;
  }

  @Override
  public PaimonCompactionOutput execute() {
    throw new UnsupportedOperationException(
        "Paimon compaction executor is not yet implemented. "
            + "This is a placeholder for future Paimon compaction support in Amoro optimizer.");
  }
}
