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

package org.apache.amoro.formats.paimon.utils;

import org.apache.amoro.table.descriptor.OperationType;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.paimon.Snapshot;
import org.apache.paimon.utils.SnapshotManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;

public class SnapShotsScanUtils {

  private static final Logger LOG = LoggerFactory.getLogger(SnapShotsScanUtils.class);

  /**
   * Get snapshots with cursor-based pagination to avoid full scan. Uses smart scanning to only scan
   * the necessary ranges for the requested page. Supports cursor-based pagination with
   * lastSnapshotId.
   *
   * @param snapshotManager the snapshot manager
   * @param commitKind the commit kind to filter (null for all kinds)
   * @param limit the maximum number of snapshots to return
   * @param offset the number of snapshots to skip
   * @param lastSnapshotId the last snapshot ID from previous page (null for first page)
   * @return Pair of (snapshot list, total count matching the filter)
   */
  public static Pair<List<Snapshot>, Integer> getSnapshotsWithPagination(
      SnapshotManager snapshotManager,
      Snapshot.CommitKind commitKind,
      int limit,
      int offset,
      Long lastSnapshotId) {

    Long earliestSnapshotId = snapshotManager.earliestSnapshot().id();
    Long latestSnapshotId = snapshotManager.latestSnapshotId();

    if (earliestSnapshotId == null || latestSnapshotId == null) {
      return Pair.of(Collections.emptyList(), 0);
    }

    // Determine the starting point based on cursor or offset
    long currentStart;
    if (lastSnapshotId != null) {
      // Cursor-based pagination: start scanning from snapshots older than lastSnapshotId
      // For cursor-based pagination, we want to start from lastSnapshotId and go backwards
      currentStart = lastSnapshotId;
    } else {
      // Traditional offset-based pagination: start from the latest
      currentStart = latestSnapshotId;
    }

    // For cursor-based pagination, we can be more precise about the range to scan
    int targetRange;
    if (lastSnapshotId != null) {
      // When using cursor, we only need to scan 'limit + small buffer' snapshots
      targetRange = limit + Math.min(offset, 5); // Very small buffer for cursor-based
    } else {
      // For first page, be more conservative to avoid over-scanning
      targetRange = limit + Math.min(offset, limit);
    }

    LOG.info(
        "Starting pagination - lastSnapshotId:{}, currentStart:{}, targetRange:{}, limit:{}, offset:{}",
        lastSnapshotId,
        currentStart,
        targetRange,
        limit,
        offset);

    // Start smart scanning from the determined starting point
    return scanForTargetPage(
        snapshotManager,
        commitKind,
        limit,
        offset,
        currentStart,
        earliestSnapshotId,
        targetRange,
        new ArrayList<>(),
        0,
        0,
        true,
        lastSnapshotId != null); // Indicate if this is cursor-based pagination
  }

  /**
   * Get snapshots with cursor-based pagination for general snapshot queries. This method handles
   * different operation types and uses smart scanning to avoid full table scans.
   *
   * @param snapshotManager the snapshot manager
   * @param commitKind the commit kind to filter (null for all kinds)
   * @param operationType the operation type to filter
   * @param limit the maximum number of snapshots to return
   * @param offset the number of snapshots to skip
   * @param lastSnapshotId the last snapshot ID from previous page (null for first page)
   * @return Pair of (snapshot list, total count matching the filter)
   */
  public static Pair<List<Snapshot>, Integer> getSnapshotsWithPaginationForGeneral(
      SnapshotManager snapshotManager,
      Snapshot.CommitKind commitKind,
      OperationType operationType,
      int limit,
      int offset,
      Long lastSnapshotId) {

    Long earliestSnapshotId = snapshotManager.earliestSnapshot().id();
    Long latestSnapshotId = snapshotManager.latestSnapshotId();

    if (earliestSnapshotId == null || latestSnapshotId == null) {
      return Pair.of(Collections.emptyList(), 0);
    }

    // Determine the starting point based on cursor or offset
    long currentStart;
    if (lastSnapshotId != null) {
      // Cursor-based pagination: start scanning from snapshots older than lastSnapshotId
      currentStart = lastSnapshotId;
    } else {
      // Traditional offset-based pagination: start from the latest
      currentStart = latestSnapshotId;
    }

    // For cursor-based pagination, we can be more precise about the range to scan
    int targetRange;
    if (lastSnapshotId != null) {
      // When using cursor, we only need to scan 'limit + small buffer' snapshots
      targetRange = limit + Math.min(offset, 5); // Very small buffer for cursor-based
    } else {
      // For first page, be more conservative to avoid over-scanning
      targetRange = limit + Math.min(offset, limit);
    }

    LOG.info(
        "Starting general pagination - lastSnapshotId:{}, currentStart:{}, targetRange:{}, limit:{}, offset:{}",
        lastSnapshotId,
        currentStart,
        targetRange,
        limit,
        offset);

    // Start smart scanning from the determined starting point
    return scanForTargetPageForGeneral(
        snapshotManager,
        commitKind,
        operationType,
        limit,
        offset,
        currentStart,
        earliestSnapshotId,
        targetRange,
        new ArrayList<>(),
        0,
        0,
        true,
        lastSnapshotId != null);
  }

  /**
   * Smart scan for target page for general snapshot queries. Handles different operation types and
   * uses smart scanning to minimize unnecessary I/O operations.
   *
   * @param snapshotManager the snapshot manager
   * @param commitKind the commit kind to filter
   * @param operationType the operation type to filter
   * @param limit the maximum number of snapshots to return
   * @param remainingOffset remaining snapshots to skip
   * @param currentStart current starting snapshot ID
   * @param earliestSnapshotId the earliest snapshot ID
   * @param scanRange size of the current range to scan
   * @param accumulatedSnapshots accumulated result snapshots
   * @param accumulatedCount accumulated total count of matching snapshots
   * @param skippedCount number of snapshots skipped so far
   * @param needCounting whether we still need to count total snapshots
   * @param isCursorBased whether this is cursor-based pagination
   * @return Pair of (snapshot list, total count matching the filter)
   */
  public static Pair<List<Snapshot>, Integer> scanForTargetPageForGeneral(
      SnapshotManager snapshotManager,
      Snapshot.CommitKind commitKind,
      OperationType operationType,
      int limit,
      int remainingOffset,
      long currentStart,
      long earliestSnapshotId,
      int scanRange,
      List<Snapshot> accumulatedSnapshots,
      int accumulatedCount,
      int skippedCount,
      boolean needCounting,
      boolean isCursorBased) {

    // Base case: reached the beginning or have enough results
    if (currentStart < earliestSnapshotId
        || (accumulatedSnapshots.size() >= limit && !needCounting)) {
      return Pair.of(accumulatedSnapshots, accumulatedCount);
    }

    // Calculate smart scan range - only scan what we need
    long rangeEnd = Math.max(earliestSnapshotId, currentStart - scanRange + 1);

    // Ensure we don't exceed the valid snapshot ID range
    long maxSnapshotId = Math.min(currentStart, snapshotManager.latestSnapshotId());
    long minSnapshotId = Math.max(rangeEnd, snapshotManager.earliestSnapshot().id());

    // Safety check: ensure maxSnapshotId >= minSnapshotId
    if (maxSnapshotId < minSnapshotId) {
      LOG.info(
          "Safety check failed - maxSnapshotId:{} < minSnapshotId:{}",
          maxSnapshotId,
          minSnapshotId);
      return Pair.of(accumulatedSnapshots, accumulatedCount);
    }

    LOG.info(
        "Smart scanning for general page - mode:{}, minSnapshotId:{}, maxSnapshotId:{}, needCounting:{}",
        isCursorBased ? "CURSOR" : "OFFSET",
        minSnapshotId,
        maxSnapshotId,
        needCounting);

    // Get snapshots in current range
    Iterator<Snapshot> rangeSnapshots =
        snapshotManager.snapshotsWithinRange(
            Optional.of(maxSnapshotId), Optional.of(minSnapshotId));

    LOG.info(
        "Querying snapshotsWithinRange - maxSnapshotId:{}, minSnapshotId:{}",
        maxSnapshotId,
        minSnapshotId);

    // Process snapshots in this range
    List<Snapshot> snapshotsInRange = new ArrayList<>();
    while (rangeSnapshots.hasNext()) {
      snapshotsInRange.add(rangeSnapshots.next());
    }

    // Sort in descending order (newest first)
    if (!snapshotsInRange.isEmpty()) {
      snapshotsInRange.sort((s1, s2) -> Long.compare(s2.id(), s1.id()));
    }

    int rangeMatchingCount = 0;
    List<Snapshot> rangeResultSnapshots = new ArrayList<>();
    int rangeSkippedCount = 0;

    // Create predicate based on operation type
    Predicate<Snapshot> predicate =
        operationType == OperationType.ALL
            ? s -> true
            : operationType == OperationType.OPTIMIZING
                ? s -> s.commitKind() == Snapshot.CommitKind.COMPACT
                : s -> s.commitKind() != Snapshot.CommitKind.COMPACT;

    // Process each snapshot in the range
    for (Snapshot snapshot : snapshotsInRange) {
      // Apply operation type filter (commitKind is null, so we rely on operationType)
      if (predicate.test(snapshot)) {
        rangeMatchingCount++;

        if (remainingOffset > 0) {
          // Skip this snapshot (count towards offset)
          rangeSkippedCount++;
        } else if (accumulatedSnapshots.size() < limit) {
          // Add to result
          rangeResultSnapshots.add(snapshot);
        }
      }
    }

    LOG.info(
        "Processed general range - snapshotsInRange:{}, rangeMatchingCount:{}, rangeResultSnapshots:{}, rangeSkippedCount:{}",
        snapshotsInRange.size(),
        rangeMatchingCount,
        rangeResultSnapshots.size(),
        rangeSkippedCount);

    // Update accumulated results
    accumulatedSnapshots.addAll(rangeResultSnapshots);
    accumulatedCount += rangeMatchingCount;
    skippedCount += rangeSkippedCount;

    // Smart decision: do we need to continue?
    boolean hasEnoughResults = accumulatedSnapshots.size() >= limit;

    // Enhanced early termination: stop immediately if we have enough results
    // For first page queries, we can stop as soon as we have enough results
    boolean shouldContinueCounting =
        needCounting
            && !(hasEnoughResults
                && (isCursorBased
                    || // For cursor-based, stop immediately when we have enough
                    rangeEnd <= earliestSnapshotId
                    || // For offset-based, stop if we've reached the end
                    (remainingOffset == 0
                        && accumulatedSnapshots.size()
                            >= limit) // For first page, stop as soon as we have enough
                ));

    // If we have enough results and don't need to continue counting, stop immediately
    if (hasEnoughResults && !shouldContinueCounting) {
      LOG.info(
          "Early termination - found enough results: {}, stopped at rangeEnd: {}",
          accumulatedSnapshots.size(),
          rangeEnd);
      return Pair.of(accumulatedSnapshots, accumulatedCount);
    }

    // If we don't have enough results, continue with optimized range
    int nextScanRange = scanRange;
    if (!hasEnoughResults && !rangeResultSnapshots.isEmpty()) {
      // We found some results but not enough, scan a bit more
      if (isCursorBased) {
        // For cursor-based pagination, be much more conservative with range expansion
        nextScanRange = Math.min(scanRange * 2, limit + 10); // Much less aggressive expansion
      } else {
        // For offset-based pagination, be more conservative
        nextScanRange = Math.min(scanRange * 2, limit * 2);
      }
    } else if (rangeResultSnapshots.isEmpty()) {
      // No results in this range, skip ahead more aggressively
      nextScanRange = Math.min(scanRange * 2, limit * 3);
    }

    // Continue scanning with the next range
    return scanForTargetPageForGeneral(
        snapshotManager,
        commitKind,
        operationType,
        limit,
        Math.max(0, remainingOffset - rangeSkippedCount),
        rangeEnd - 1,
        earliestSnapshotId,
        nextScanRange,
        accumulatedSnapshots,
        accumulatedCount,
        skippedCount,
        shouldContinueCounting,
        isCursorBased);
  }

  /**
   * Smart scan for target page to minimize unnecessary scanning. Scans from newest to oldest, stops
   * when enough results are found. Supports both offset-based and cursor-based pagination.
   *
   * @param snapshotManager the snapshot manager
   * @param commitKind the commit kind to filter
   * @param limit the maximum number of snapshots to return
   * @param remainingOffset remaining snapshots to skip
   * @param currentStart current starting snapshot ID
   * @param earliestSnapshotId the earliest snapshot ID
   * @param scanRange size of the current range to scan
   * @param accumulatedSnapshots accumulated result snapshots
   * @param accumulatedCount accumulated total count of matching snapshots
   * @param skippedCount number of snapshots skipped so far
   * @param needCounting whether we still need to count total snapshots
   * @param isCursorBased whether this is cursor-based pagination
   * @return Pair of (snapshot list, total count matching the filter)
   */
  public static Pair<List<Snapshot>, Integer> scanForTargetPage(
      SnapshotManager snapshotManager,
      Snapshot.CommitKind commitKind,
      int limit,
      int remainingOffset,
      long currentStart,
      long earliestSnapshotId,
      int scanRange,
      List<Snapshot> accumulatedSnapshots,
      int accumulatedCount,
      int skippedCount,
      boolean needCounting,
      boolean isCursorBased) {

    // Base case: reached the beginning or have enough results
    if (currentStart < earliestSnapshotId
        || (accumulatedSnapshots.size() >= limit && !needCounting)) {
      return Pair.of(accumulatedSnapshots, accumulatedCount);
    }

    // Calculate smart scan range - only scan what we need
    long rangeEnd;
    if (isCursorBased) {
      // For cursor-based pagination, scan older snapshots from currentStart
      rangeEnd = Math.max(earliestSnapshotId, currentStart - scanRange + 1);
    } else {
      // For offset-based pagination, scan from currentStart backwards
      rangeEnd = Math.max(earliestSnapshotId, currentStart - scanRange + 1);
    }

    // Ensure we don't exceed the valid snapshot ID range
    long maxSnapshotId = Math.min(currentStart, snapshotManager.latestSnapshotId());
    long minSnapshotId = Math.max(rangeEnd, snapshotManager.earliestSnapshot().id());

    // Safety check: ensure maxSnapshotId >= minSnapshotId
    if (maxSnapshotId < minSnapshotId) {
      LOG.info(
          "Safety check failed - maxSnapshotId:{} < minSnapshotId:{}",
          maxSnapshotId,
          minSnapshotId);
      return Pair.of(accumulatedSnapshots, accumulatedCount);
    }

    LOG.info(
        "Smart scanning for page - mode:{}, minSnapshotId:{}, maxSnapshotId:{}, needCounting:{}",
        isCursorBased ? "CURSOR" : "OFFSET",
        minSnapshotId,
        maxSnapshotId,
        needCounting);

    // Get snapshots in current range
    // Note: snapshotsWithinRange takes (maxSnapshotId, minSnapshotId) as parameters
    Iterator<Snapshot> rangeSnapshots =
        snapshotManager.snapshotsWithinRange(
            Optional.of(maxSnapshotId), Optional.of(minSnapshotId));

    LOG.info(
        "Querying snapshotsWithinRange - maxSnapshotId:{}, minSnapshotId:{}",
        maxSnapshotId,
        minSnapshotId);

    // Process snapshots in this range
    List<Snapshot> snapshotsInRange = new ArrayList<>();
    while (rangeSnapshots.hasNext()) {
      snapshotsInRange.add(rangeSnapshots.next());
    }

    // Sort in descending order (newest first)
    if (!snapshotsInRange.isEmpty()) {
      snapshotsInRange.sort((s1, s2) -> Long.compare(s2.id(), s1.id()));
    }

    int rangeMatchingCount = 0;
    List<Snapshot> rangeResultSnapshots = new ArrayList<>();
    int rangeSkippedCount = 0;

    // Process each snapshot in the range
    for (Snapshot snapshot : snapshotsInRange) {
      // Note: We can't access lastSnapshotId here, so we'll handle this in the range calculation
      // instead

      if (commitKind == null || snapshot.commitKind() == commitKind) {
        rangeMatchingCount++;

        if (remainingOffset > 0) {
          // Skip this snapshot (count towards offset)
          rangeSkippedCount++;
        } else if (accumulatedSnapshots.size() < limit) {
          // Add to result
          rangeResultSnapshots.add(snapshot);
        }
      }
    }

    LOG.info(
        "Processed range - snapshotsInRange:{}, rangeMatchingCount:{}, rangeResultSnapshots:{}, rangeSkippedCount:{}",
        snapshotsInRange.size(),
        rangeMatchingCount,
        rangeResultSnapshots.size(),
        rangeSkippedCount);

    // Update accumulated results
    accumulatedSnapshots.addAll(rangeResultSnapshots);
    accumulatedCount += rangeMatchingCount;
    skippedCount += rangeSkippedCount;

    // Smart decision: do we need to continue?
    boolean hasEnoughResults = accumulatedSnapshots.size() >= limit;

    // Enhanced early termination: stop immediately if we have enough results
    // For first page queries, we can stop as soon as we have enough results
    boolean shouldContinueCounting =
        needCounting
            && !(hasEnoughResults
                && (isCursorBased
                    || // For cursor-based, stop immediately when we have enough
                    rangeEnd <= earliestSnapshotId
                    || // For offset-based, stop if we've reached the end
                    (remainingOffset == 0
                        && accumulatedSnapshots.size()
                            >= limit) // For first page, stop as soon as we have enough
                ));

    // If we have enough results and don't need to continue counting, stop immediately
    if (hasEnoughResults && !shouldContinueCounting) {
      LOG.info(
          "Early termination - found enough results: {}, stopped at rangeEnd: {}",
          accumulatedSnapshots.size(),
          rangeEnd);
      return Pair.of(accumulatedSnapshots, accumulatedCount);
    }

    // If we don't have enough results, continue with optimized range
    int nextScanRange = scanRange;
    if (!hasEnoughResults && !rangeResultSnapshots.isEmpty()) {
      // We found some results but not enough, scan a bit more
      if (isCursorBased) {
        // For cursor-based pagination, be much more conservative with range expansion
        nextScanRange = Math.min(scanRange * 2, limit + 10); // Much less aggressive expansion
      } else {
        // For offset-based pagination, be more conservative
        nextScanRange = Math.min(scanRange * 2, limit * 2);
      }
    } else if (rangeResultSnapshots.isEmpty()) {
      // No results in this range, skip ahead more aggressively
      nextScanRange = Math.min(scanRange * 2, limit * 3);
    }

    // Recursively scan the next range
    return scanForTargetPage(
        snapshotManager,
        commitKind,
        limit,
        Math.max(0, remainingOffset - rangeSkippedCount),
        rangeEnd - 1,
        earliestSnapshotId,
        nextScanRange,
        accumulatedSnapshots,
        accumulatedCount,
        skippedCount,
        shouldContinueCounting,
        isCursorBased);
  }
}
