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

package org.apache.amoro.optimizing.maintainer;

import org.apache.amoro.api.CatalogMeta;
import org.apache.amoro.api.CommitMetaProducer;
import org.apache.amoro.config.DataExpirationConfig;
import org.apache.amoro.iceberg.Constants;
import org.apache.amoro.maintainer.api.MaintainerExecutor;
import org.apache.amoro.maintainer.api.MaintainerType;
import org.apache.amoro.optimizing.IcebergExpireDataInput;
import org.apache.amoro.optimizing.IcebergExpireDataOutput;
import org.apache.amoro.shade.guava32.com.google.common.annotations.VisibleForTesting;
import org.apache.amoro.shade.guava32.com.google.common.collect.Iterables;
import org.apache.amoro.shade.guava32.com.google.common.collect.Maps;
import org.apache.amoro.shade.guava32.com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.ContentScanTask;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.DeleteFiles;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RewriteFiles;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.SerializableFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.LinkedTransferQueue;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class ExpireDataExecutor implements MaintainerExecutor<IcebergExpireDataOutput> {

  private static final Logger LOG = LoggerFactory.getLogger(ExpireDataExecutor.class);
  private final DataExpirationConfig expirationConfig;
  private final Table table;

  public static final String EXPIRE_TIMESTAMP_MS = "TIMESTAMP_MS";
  public static final String EXPIRE_TIMESTAMP_S = "TIMESTAMP_S";

  public static final Set<String> AMORO_MAINTAIN_COMMITS =
      Sets.newHashSet(
          CommitMetaProducer.OPTIMIZE.name(),
          CommitMetaProducer.DATA_EXPIRATION.name(),
          CommitMetaProducer.CLEAN_DANGLING_DELETE.name());
  private final CatalogMeta catalogMeta;
  private final String database;

  public ExpireDataExecutor(IcebergExpireDataInput icebergExpireDataInput) {
    this.expirationConfig = icebergExpireDataInput.getExpirationConfig();
    this.catalogMeta = icebergExpireDataInput.getCatalogMeta();
    this.database = icebergExpireDataInput.getDatabase();
    this.table = icebergExpireDataInput.getTable();
  }

  @Override
  public IcebergExpireDataOutput execute() {
    try {
      Types.NestedField field = table.schema().findField(expirationConfig.getExpirationField());
      if (!isValidDataExpirationField(expirationConfig, field, table.name())) {
        return null;
      }
      return expireDataFrom(expirationConfig, expireBaseOnRule(expirationConfig, field));
    } catch (Throwable t) {
      LOG.error("Unexpected purge error for table {} ", table.name(), t);
    }
    return null;
  }

  @Override
  public MaintainerType type() {
    return MaintainerType.EXPIRE_DATA;
  }

  protected Instant expireBaseOnRule(
      DataExpirationConfig expirationConfig, Types.NestedField field) {
    switch (expirationConfig.getBaseOnRule()) {
      case CURRENT_TIME:
        return Instant.now();
      case LAST_COMMIT_TIME:
        long lastCommitTimestamp = fetchLatestNonOptimizedSnapshotTime(table);
        // if the table does not exist any non-optimized snapshots, should skip the expiration
        if (lastCommitTimestamp != Long.MAX_VALUE) {
          // snapshot timestamp should be UTC
          return Instant.ofEpochMilli(lastCommitTimestamp);
        } else {
          return Instant.MIN;
        }
      default:
        throw new IllegalArgumentException(
            "Cannot expire data base on " + expirationConfig.getBaseOnRule().name());
    }
  }

  /**
   * When expiring historic data and `data-expire.base-on-rule` is `LAST_COMMIT_TIME`, the latest
   * snapshot should not be produced by Amoro optimizing.
   *
   * @param table iceberg table
   * @return the latest non-optimized snapshot timestamp
   */
  public static long fetchLatestNonOptimizedSnapshotTime(Table table) {
    Optional<Snapshot> snapshot =
        IcebergTableUtil.findFirstMatchSnapshot(
            table, s -> s.summary().values().stream().noneMatch(AMORO_MAINTAIN_COMMITS::contains));
    return snapshot.map(Snapshot::timestampMillis).orElse(Long.MAX_VALUE);
  }

  /**
   * Purge data older than the specified UTC timestamp
   *
   * @param expirationConfig expiration configs
   * @param instant timestamp/timestampz/long field type uses UTC, others will use the local time
   *     zone
   */
  @VisibleForTesting
  public IcebergExpireDataOutput expireDataFrom(
      DataExpirationConfig expirationConfig, Instant instant) {
    if (instant.equals(Instant.MIN)) {
      return null;
    }

    long expireTimestamp = instant.minusMillis(expirationConfig.getRetentionTime()).toEpochMilli();
    LOG.info(
        "Expiring data older than {} in table {} ",
        Instant.ofEpochMilli(expireTimestamp)
            .atZone(
                getDefaultZoneId(table.schema().findField(expirationConfig.getExpirationField())))
            .toLocalDateTime(),
        table.name());

    Expression dataFilter = getDataExpression(table.schema(), expirationConfig, expireTimestamp);

    ExpireFiles expiredFiles = expiredFileScan(expirationConfig, dataFilter, expireTimestamp);
    return expireFiles(expiredFiles, expireTimestamp);
  }

  protected ExpireFiles expiredFileScan(
      DataExpirationConfig expirationConfig, Expression dataFilter, long expireTimestamp) {
    Map<StructLike, DataFileFreshness> partitionFreshness = Maps.newConcurrentMap();
    ExpireFiles expiredFiles = new ExpireFiles();
    try (CloseableIterable<FileEntry> entries =
        fileScan(table, dataFilter, expirationConfig, expireTimestamp)) {
      Queue<FileEntry> fileEntries = new LinkedTransferQueue<>();
      entries.forEach(
          e -> {
            if (mayExpired(e, partitionFreshness, expireTimestamp)) {
              fileEntries.add(e);
            }
          });
      fileEntries
          .parallelStream()
          .filter(e -> willNotRetain(e, expirationConfig, partitionFreshness))
          .forEach(expiredFiles::addFile);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return expiredFiles;
  }

  static boolean willNotRetain(
      FileEntry fileEntry,
      DataExpirationConfig expirationConfig,
      Map<StructLike, DataFileFreshness> partitionFreshness) {
    ContentFile<?> contentFile = fileEntry.getFile();

    switch (expirationConfig.getExpirationLevel()) {
      case PARTITION:
        // if only partial expired files in a partition, all the files in that partition should be
        // preserved
        return partitionFreshness.containsKey(contentFile.partition())
            && partitionFreshness.get(contentFile.partition()).expiredDataFileCount
                == partitionFreshness.get(contentFile.partition()).totalDataFileCount;
      case FILE:
        if (!contentFile.content().equals(FileContent.DATA)) {
          long seqUpperBound =
              partitionFreshness.getOrDefault(
                      contentFile.partition(),
                      new DataFileFreshness(Long.MIN_VALUE, Long.MAX_VALUE))
                  .latestExpiredSeq;
          // only expire delete files with sequence-number less or equal to expired data file
          // there may be some dangling delete files, they will be cleaned by
          // OrphanFileCleaningExecutor
          return fileEntry.getFile().dataSequenceNumber() <= seqUpperBound;
        } else {
          return true;
        }
      default:
        return false;
    }
  }

  /**
   * Create a filter expression for expired files for the `FILE` level. For the `PARTITION` level,
   * we need to collect the oldest files to determine if the partition is obsolete, so we will not
   * filter for expired files at the scanning stage
   *
   * @param expirationConfig expiration configuration
   * @param expireTimestamp expired timestamp
   */
  protected static Expression getDataExpression(
      Schema schema, DataExpirationConfig expirationConfig, long expireTimestamp) {
    if (expirationConfig.getExpirationLevel().equals(DataExpirationConfig.ExpireLevel.PARTITION)) {
      return Expressions.alwaysTrue();
    }

    Types.NestedField field = schema.findField(expirationConfig.getExpirationField());
    Type.TypeID typeID = field.type().typeId();
    switch (typeID) {
      case TIMESTAMP:
        return Expressions.lessThanOrEqual(field.name(), expireTimestamp * 1000);
      case LONG:
        if (expirationConfig.getNumberDateFormat().equals(EXPIRE_TIMESTAMP_MS)) {
          return Expressions.lessThanOrEqual(field.name(), expireTimestamp);
        } else if (expirationConfig.getNumberDateFormat().equals(EXPIRE_TIMESTAMP_S)) {
          return Expressions.lessThanOrEqual(field.name(), expireTimestamp / 1000);
        } else {
          return Expressions.alwaysTrue();
        }
      case STRING:
        String expireDateTime =
            LocalDateTime.ofInstant(Instant.ofEpochMilli(expireTimestamp), getDefaultZoneId(field))
                .format(
                    DateTimeFormatter.ofPattern(
                        expirationConfig.getDateTimePattern(), Locale.getDefault()));
        return Expressions.lessThanOrEqual(field.name(), expireDateTime);
      default:
        return Expressions.alwaysTrue();
    }
  }

  static boolean mayExpired(
      FileEntry fileEntry,
      Map<StructLike, DataFileFreshness> partitionFreshness,
      Long expireTimestamp) {
    ContentFile<?> contentFile = fileEntry.getFile();
    StructLike partition = contentFile.partition();

    boolean expired = true;
    if (contentFile.content().equals(FileContent.DATA)) {
      Literal<Long> literal = fileEntry.getTsBound();
      if (partitionFreshness.containsKey(partition)) {
        DataFileFreshness freshness = partitionFreshness.get(partition).incTotalCount();
        if (freshness.latestUpdateMillis <= literal.value()) {
          partitionFreshness.put(partition, freshness.updateLatestMillis(literal.value()));
        }
      } else {
        partitionFreshness.putIfAbsent(
            partition,
            new DataFileFreshness(fileEntry.getFile().dataSequenceNumber(), literal.value())
                .incTotalCount());
      }
      expired = literal.comparator().compare(expireTimestamp, literal.value()) >= 0;
      if (expired) {
        partitionFreshness.computeIfPresent(
            partition,
            (k, v) ->
                v.updateExpiredSeq(fileEntry.getFile().dataSequenceNumber()).incExpiredCount());
      }
    }

    return expired;
  }

  CloseableIterable<FileEntry> fileScan(
      Table table,
      Expression dataFilter,
      DataExpirationConfig expirationConfig,
      long expireTimestamp) {
    TableScan tableScan = table.newScan().filter(dataFilter).includeColumnStats();

    CloseableIterable<FileScanTask> tasks;
    Snapshot snapshot = IcebergTableUtil.getSnapshot(table, false);
    if (snapshot == null) {
      return CloseableIterable.empty();
    }
    long snapshotId = snapshot.snapshotId();
    if (snapshotId == Constants.INVALID_SNAPSHOT_ID) {
      tasks = tableScan.planFiles();
    } else {
      tasks = tableScan.useSnapshot(snapshotId).planFiles();
    }
    long deleteFileCnt =
        Long.parseLong(
            snapshot.summary().getOrDefault(SnapshotSummary.TOTAL_DELETE_FILES_PROP, "0"));
    CloseableIterable<DataFile> dataFiles =
        CloseableIterable.transform(tasks, ContentScanTask::file);
    CloseableIterable<FileScanTask> hasDeleteTask =
        deleteFileCnt > 0
            ? CloseableIterable.filter(tasks, t -> !t.deletes().isEmpty())
            : CloseableIterable.empty();

    Set<DeleteFile> deleteFiles =
        StreamSupport.stream(hasDeleteTask.spliterator(), true)
            .flatMap(e -> e.deletes().stream())
            .collect(Collectors.toSet());

    Types.NestedField field = table.schema().findField(expirationConfig.getExpirationField());
    Comparable<?> expireValue = getExpireValue(expirationConfig, field, expireTimestamp);
    return CloseableIterable.transform(
        CloseableIterable.withNoopClose(Iterables.concat(dataFiles, deleteFiles)),
        contentFile -> {
          Literal<Long> literal =
              getExpireTimestampLiteral(
                  contentFile,
                  field,
                  DateTimeFormatter.ofPattern(
                      expirationConfig.getDateTimePattern(), Locale.getDefault()),
                  expirationConfig.getNumberDateFormat(),
                  expireValue);
          return new FileEntry(contentFile.copyWithoutStats(), literal);
        });
  }

  private Literal<Long> getExpireTimestampLiteral(
      ContentFile<?> contentFile,
      Types.NestedField field,
      DateTimeFormatter formatter,
      String numberDateFormatter,
      Comparable<?> expireValue) {
    Type type = field.type();
    Literal<Long> literal = Literal.of(Long.MAX_VALUE);

    Map<Integer, ByteBuffer> upperBounds = contentFile.upperBounds();
    if (upperBounds == null || !upperBounds.containsKey(field.fieldId())) {
      return canBeExpireByPartitionValue(contentFile, field, expireValue)
          ? Literal.of(0L)
          : literal;
    }

    Object upperBound = Conversions.fromByteBuffer(type, upperBounds.get(field.fieldId()));
    if (upperBound instanceof Long) {
      if (type.typeId() == Type.TypeID.TIMESTAMP) {
        // nanosecond -> millisecond
        literal = Literal.of((Long) upperBound / 1000);
      } else {
        if (numberDateFormatter.equals(EXPIRE_TIMESTAMP_MS)) {
          literal = Literal.of((Long) upperBound);
        } else if (numberDateFormatter.equals(EXPIRE_TIMESTAMP_S)) {
          // second -> millisecond
          literal = Literal.of((Long) upperBound * 1000);
        }
      }
    } else if (type.typeId() == Type.TypeID.STRING) {
      literal =
          Literal.of(
              LocalDate.parse(upperBound.toString(), formatter)
                  .atStartOfDay()
                  .atZone(getDefaultZoneId(field))
                  .toInstant()
                  .toEpochMilli());
    }

    return literal;
  }

  @SuppressWarnings("unchecked")
  private boolean canBeExpireByPartitionValue(
      ContentFile<?> contentFile, Types.NestedField expireField, Comparable<?> expireValue) {
    PartitionSpec partitionSpec = table.specs().get(contentFile.specId());
    int pos = 0;
    List<Boolean> compareResults = new ArrayList<>();
    for (PartitionField partitionField : partitionSpec.fields()) {
      if (partitionField.sourceId() == expireField.fieldId()) {
        if (partitionField.transform().isVoid()) {
          return false;
        }

        Comparable<?> partitionUpperBound =
            ((SerializableFunction<Comparable<?>, Comparable<?>>)
                    partitionField.transform().bind(expireField.type()))
                .apply(expireValue);
        Comparable<Object> filePartitionValue =
            contentFile.partition().get(pos, partitionUpperBound.getClass());
        int compared = filePartitionValue.compareTo(partitionUpperBound);
        Boolean compareResult =
            expireField.type() == Types.StringType.get() ? compared <= 0 : compared < 0;
        compareResults.add(compareResult);
      }

      pos++;
    }

    return !compareResults.isEmpty() && compareResults.stream().allMatch(Boolean::booleanValue);
  }

  private Comparable<?> getExpireValue(
      DataExpirationConfig expirationConfig, Types.NestedField field, long expireTimestamp) {
    switch (field.type().typeId()) {
        // expireTimestamp is in milliseconds, TIMESTAMP type is in microseconds
      case TIMESTAMP:
        return expireTimestamp * 1000;
      case LONG:
        if (expirationConfig.getNumberDateFormat().equals(EXPIRE_TIMESTAMP_MS)) {
          return expireTimestamp;
        } else if (expirationConfig.getNumberDateFormat().equals(EXPIRE_TIMESTAMP_S)) {
          return expireTimestamp / 1000;
        } else {
          throw new IllegalArgumentException(
              "Number dateformat: " + expirationConfig.getNumberDateFormat());
        }
      case STRING:
        return LocalDateTime.ofInstant(
                Instant.ofEpochMilli(expireTimestamp), getDefaultZoneId(field))
            .format(
                DateTimeFormatter.ofPattern(
                    expirationConfig.getDateTimePattern(), Locale.getDefault()));
      default:
        throw new IllegalArgumentException(
            "Unsupported expiration field type: " + field.type().typeId());
    }
  }

  public static ZoneId getDefaultZoneId(Types.NestedField expireField) {
    Type type = expireField.type();
    if (type.typeId() == Type.TypeID.STRING) {
      return ZoneId.systemDefault();
    }
    return ZoneOffset.UTC;
  }

  public static class ExpireFiles {
    Queue<DataFile> dataFiles;
    Queue<DeleteFile> deleteFiles;

    ExpireFiles() {
      this.dataFiles = new LinkedTransferQueue<>();
      this.deleteFiles = new LinkedTransferQueue<>();
    }

    void addFile(FileEntry entry) {
      ContentFile<?> file = entry.getFile();
      switch (file.content()) {
        case DATA:
          dataFiles.add((DataFile) file.copyWithoutStats());
          break;
        case EQUALITY_DELETES:
        case POSITION_DELETES:
          deleteFiles.add((DeleteFile) file.copyWithoutStats());
          break;
        default:
          throw new IllegalArgumentException(file.content().name() + "cannot be expired");
      }
    }
  }

  public static class FileEntry {
    private final ContentFile<?> file;
    private final Literal<Long> tsBound;

    FileEntry(ContentFile<?> file, Literal<Long> tsBound) {
      this.file = file;
      this.tsBound = tsBound;
    }

    public ContentFile<?> getFile() {
      return file;
    }

    public Literal<Long> getTsBound() {
      return tsBound;
    }
  }

  public static class DataFileFreshness {
    long latestExpiredSeq;
    long latestUpdateMillis;
    long expiredDataFileCount;
    long totalDataFileCount;

    DataFileFreshness(long sequenceNumber, long latestUpdateMillis) {
      this.latestExpiredSeq = sequenceNumber;
      this.latestUpdateMillis = latestUpdateMillis;
    }

    DataFileFreshness updateLatestMillis(long ts) {
      this.latestUpdateMillis = ts;
      return this;
    }

    DataFileFreshness updateExpiredSeq(Long seq) {
      this.latestExpiredSeq = seq;
      return this;
    }

    DataFileFreshness incTotalCount() {
      totalDataFileCount++;
      return this;
    }

    DataFileFreshness incExpiredCount() {
      expiredDataFileCount++;
      return this;
    }
  }

  public static final Set<Type.TypeID> DATA_EXPIRATION_FIELD_TYPES =
      Sets.newHashSet(Type.TypeID.TIMESTAMP, Type.TypeID.STRING, Type.TypeID.LONG);

  public static boolean isValidDataExpirationField(
      DataExpirationConfig config, Types.NestedField field, String name) {
    return config.isEnabled()
        && config.getRetentionTime() > 0
        && validateExpirationField(field, name, config.getExpirationField());
  }

  private static boolean validateExpirationField(
      Types.NestedField field, String name, String expirationField) {
    if (StringUtils.isBlank(expirationField) || null == field) {
      LOG.warn(
          String.format(
              "Field(%s) used to determine data expiration is illegal for table(%s)",
              expirationField, name));
      return false;
    }
    Type.TypeID typeID = field.type().typeId();
    if (!DATA_EXPIRATION_FIELD_TYPES.contains(typeID)) {
      LOG.warn(
          String.format(
              "Table(%s) field(%s) type(%s) is not supported for data expiration, please use the "
                  + "following types: %s",
              name,
              expirationField,
              typeID.name(),
              StringUtils.join(DATA_EXPIRATION_FIELD_TYPES, ", ")));
      return false;
    }

    return true;
  }

  IcebergExpireDataOutput expireFiles(ExpireFiles expiredFiles, long expireTimestamp) {
    long snapshotId = IcebergTableUtil.getSnapshotId(table, false);
    Queue<DataFile> dataFiles = expiredFiles.dataFiles;
    Queue<DeleteFile> deleteFiles = expiredFiles.deleteFiles;
    if (dataFiles.isEmpty() && deleteFiles.isEmpty()) {
      return null;
    }
    // expire data files
    DeleteFiles delete = table.newDelete();
    dataFiles.forEach(delete::deleteFile);
    delete.set(
        org.apache.amoro.op.SnapshotSummary.SNAPSHOT_PRODUCER,
        CommitMetaProducer.DATA_EXPIRATION.name());
    delete.commit();
    // expire delete files
    if (!deleteFiles.isEmpty()) {
      RewriteFiles rewriteFiles = table.newRewrite().validateFromSnapshot(snapshotId);
      deleteFiles.forEach(rewriteFiles::deleteFile);
      rewriteFiles.set(
          org.apache.amoro.op.SnapshotSummary.SNAPSHOT_PRODUCER,
          CommitMetaProducer.DATA_EXPIRATION.name());
      rewriteFiles.commit();
    }

    // TODO: persistent table expiration record. Contains some meta information such as table_id,
    // snapshotId,
    //  file_infos(file_content, path, recordCount, fileSizeInBytes, equalityFieldIds,
    // partitionPath,
    //  sequenceNumber) and expireTimestamp...

    LOG.info(
        "Expired files older than {}, {} data files[{}] and {} delete files[{}] for table {}",
        expireTimestamp,
        dataFiles.size(),
        dataFiles.stream().map(ContentFile::path).collect(Collectors.joining(",")),
        deleteFiles.size(),
        deleteFiles.stream().map(ContentFile::path).collect(Collectors.joining(",")),
        table.name());
    return new IcebergExpireDataOutput(
        catalogMeta.getCatalogName(),
        database,
        table.name(),
        this.type().name(),
        System.currentTimeMillis(),
        expireTimestamp,
        dataFiles.size(),
        deleteFiles.size());
  }
}
