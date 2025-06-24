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

import org.apache.amoro.maintainer.api.MaintainerExecutor;
import org.apache.amoro.maintainer.output.CleanOrphanOutPut;
import org.apache.amoro.optimizing.IcebergCleanOrphanInput;
import org.apache.amoro.optimizing.IcebergDanglingDeleteFilesInput;
import org.apache.amoro.optimizing.IcebergDeleteFilesOutput;
import org.apache.amoro.optimizing.IcebergExpireDataInput;
import org.apache.amoro.optimizing.IcebergExpireDataOutput;
import org.apache.amoro.optimizing.IcebergExpireSnapshotInput;
import org.apache.amoro.optimizing.IcebergExpireSnapshotsOutput;
import org.apache.iceberg.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AbstractIcebergMaintainer implements IcebergTableMaintainerV2 {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractIcebergMaintainer.class);

  protected final Table table;

  public AbstractIcebergMaintainer(Table table) {
    this.table = table;
  }

  @Override
  public CleanOrphanOutPut cleanOrphanFiles(IcebergCleanOrphanInput input) {
    CleanOrphanFilesFactory cleanOrphanFilesFactory = new CleanOrphanFilesFactory();
    MaintainerExecutor<CleanOrphanOutPut> cleanOrphanFilesFactoryExecutor =
        cleanOrphanFilesFactory.createExecutor(input);
    CleanOrphanOutPut cleanOrphanOutPut = cleanOrphanFilesFactoryExecutor.execute();
    LOG.info("cleanOrphanOutPut : {}", cleanOrphanOutPut);
    table.refresh();
    return cleanOrphanOutPut;
  }

  @Override
  public IcebergDeleteFilesOutput cleanDanglingDeleteFiles(IcebergDanglingDeleteFilesInput input) {
    DanglingDeleteFilesCleaningFactory danglingDeleteFilesCleaningFactory =
        new DanglingDeleteFilesCleaningFactory();
    MaintainerExecutor<IcebergDeleteFilesOutput> cleaningFactoryExecutor =
        danglingDeleteFilesCleaningFactory.createExecutor(input);
    IcebergDeleteFilesOutput icebergDeleteFilesOutput = cleaningFactoryExecutor.execute();
    LOG.info("icebergDeleteFilesOutput : {}", icebergDeleteFilesOutput);
    table.refresh();
    return icebergDeleteFilesOutput;
  }

  @Override
  public IcebergExpireSnapshotsOutput expireSnapshots(IcebergExpireSnapshotInput input) {
    ExpireSnapshotsFactory expireSnapshotsFactory = new ExpireSnapshotsFactory();
    MaintainerExecutor<IcebergExpireSnapshotsOutput> snapshotsFactoryExecutor =
        expireSnapshotsFactory.createExecutor(input);
    IcebergExpireSnapshotsOutput snapshotsOutput = snapshotsFactoryExecutor.execute();
    LOG.info("snapshotsOutput : {}", snapshotsOutput);
    table.refresh();
    return snapshotsOutput;
  }

  @Override
  public IcebergExpireDataOutput expireData(IcebergExpireDataInput input) {
    ExpireDataFactory expireDataFactory = new ExpireDataFactory();
    MaintainerExecutor<IcebergExpireDataOutput> expireDataFactoryExecutor =
        expireDataFactory.createExecutor(input);
    IcebergExpireDataOutput expireDataOutput = expireDataFactoryExecutor.execute();
    LOG.info("expireDataOutput : {}", expireDataOutput);
    table.refresh();
    return expireDataOutput;
  }

  @Override
  public void autoCreateTags() {}
}
