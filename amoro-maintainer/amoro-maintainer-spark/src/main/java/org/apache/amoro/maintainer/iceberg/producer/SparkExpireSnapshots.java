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

package org.apache.amoro.maintainer.iceberg.producer;

import static org.apache.amoro.shade.guava32.com.google.common.primitives.Longs.min;

import org.apache.amoro.api.CommitMetaProducer;
import org.apache.amoro.maintainer.api.MaintainerExecutor;
import org.apache.amoro.maintainer.api.MaintainerType;
import org.apache.amoro.optimizing.IcebergExpireSnapshotInput;
import org.apache.amoro.optimizing.IcebergExpireSnapshotsOutput;
import org.apache.amoro.optimizing.maintainer.IcebergTableUtil;
import org.apache.amoro.shade.guava32.com.google.common.collect.Sets;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.ExpireSnapshots;
import org.apache.iceberg.relocated.com.google.common.util.concurrent.MoreExecutors;
import org.apache.iceberg.relocated.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.iceberg.spark.actions.ExpireSnapshotsSparkAction;
import org.apache.iceberg.spark.actions.SparkActions;

import java.util.HashMap;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

public class SparkExpireSnapshots implements MaintainerExecutor<IcebergExpireSnapshotsOutput> {

  private final SparkActions action;
  private final Table table;
  private final Integer retainLastNum;
  private final Long olderThanMillis;
  private final Integer maxConcurrentDeletes;

  public static final Set<String> AMORO_MAINTAIN_COMMITS =
      Sets.newHashSet(
          CommitMetaProducer.OPTIMIZE.name(),
          CommitMetaProducer.DATA_EXPIRATION.name(),
          CommitMetaProducer.CLEAN_DANGLING_DELETE.name());

  // same as org.apache.iceberg.flink.sink.IcebergFilesCommitter#MAX_COMMITTED_CHECKPOINT_ID
  public static final String FLINK_MAX_COMMITTED_CHECKPOINT_ID =
      "flink.max-committed-checkpoint-id";
  private final String catalogName;
  private final String database;

  public SparkExpireSnapshots(IcebergExpireSnapshotInput input, SparkActions sparkActions) {
    this.action = sparkActions;
    this.catalogName = input.getCatalogMeta().getCatalogName();
    this.database = input.getDatabase();
    this.table = input.getIcebergTable();
    this.olderThanMillis =
        mustOlderThan(table, input.getSnapshotTTLMinutes(), System.currentTimeMillis());
    this.retainLastNum = input.getMinCount();
    this.maxConcurrentDeletes = input.getMaxConcurrentDeletes();
  }

  @Override
  public IcebergExpireSnapshotsOutput execute() {
    ExpireSnapshots.Result result =
        action
            .expireSnapshots(table)
            .expireOlderThan(olderThanMillis)
            .retainLast(retainLastNum)
            .option(ExpireSnapshotsSparkAction.STREAM_RESULTS, Boolean.toString(true))
            .executeDeleteWith(
                MoreExecutors.getExitingExecutorService(
                    (ThreadPoolExecutor)
                        Executors.newFixedThreadPool(
                            maxConcurrentDeletes,
                            new ThreadFactoryBuilder()
                                .setDaemon(true)
                                .setNameFormat("expire-snapshots" + "-%d")
                                .build())))
            .execute();
    return new IcebergExpireSnapshotsOutput(
        catalogName,
        database,
        table.name(),
        MaintainerType.EXPIRE_SNAPSHOTS,
        System.currentTimeMillis(),
        System.currentTimeMillis(),
        System.currentTimeMillis(),
        1000L,
        true,
        null,
        new HashMap<>(),
        3L,
        result);
  }

  protected static long mustOlderThan(Table table, Long snapshotTTLMinutes, long now) {
    return min(
        // The snapshots keep time
        now - snapshotTTLMinutes,
        //  TODO The snapshot optimizing plan based should not be expired for committing
        // fetchOptimizingPlanSnapshotTime(table, tableConfiguration),
        // The latest non-optimized snapshot should not be expired for data expiring
        fetchLatestNonOptimizedSnapshotTime(table),
        // The latest flink committed snapshot should not be expired for recovering flink job
        fetchLatestFlinkCommittedSnapshotTime(table));
  }

  public static long fetchLatestNonOptimizedSnapshotTime(Table table) {
    Optional<Snapshot> snapshot =
        IcebergTableUtil.findFirstMatchSnapshot(
            table, s -> s.summary().values().stream().noneMatch(AMORO_MAINTAIN_COMMITS::contains));
    return snapshot.map(Snapshot::timestampMillis).orElse(Long.MAX_VALUE);
  }

  /**
   * When committing a snapshot, Flink will write a checkpoint id into the snapshot summary, which
   * will be used when Flink job recovers from the checkpoint.
   *
   * @param table table
   * @return commit time of snapshot with the latest flink checkpointId in summary, return
   *     Long.MAX_VALUE if not exist
   */
  public static long fetchLatestFlinkCommittedSnapshotTime(Table table) {
    Snapshot snapshot = findLatestSnapshotContainsKey(table, FLINK_MAX_COMMITTED_CHECKPOINT_ID);
    return snapshot == null ? Long.MAX_VALUE : snapshot.timestampMillis();
  }

  public static Snapshot findLatestSnapshotContainsKey(Table table, String summaryKey) {
    Snapshot latestSnapshot = null;
    for (Snapshot snapshot : table.snapshots()) {
      if (snapshot.summary().containsKey(summaryKey)) {
        latestSnapshot = snapshot;
      }
    }
    return latestSnapshot;
  }

  @Override
  public MaintainerType type() {
    return MaintainerType.EXPIRE_SNAPSHOTS;
  }
}
