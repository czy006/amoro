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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.amoro.optimizing.OptimizingExecutor;
import org.apache.amoro.utils.SerializationUtil;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

@DisplayName("Test Paimon Compaction executor skeleton")
public class TestPaimonCompactionExecutor {

  @Test
  @DisplayName("Factory initialize should accept properties without error")
  void testFactoryInitialize() {
    PaimonCompactionExecutorFactory factory = new PaimonCompactionExecutorFactory();
    Map<String, String> props = new HashMap<>();
    props.put("key1", "value1");
    factory.initialize(props);
  }

  @Test
  @DisplayName("Factory createExecutor should return non-null PaimonCompactionExecutor")
  void testFactoryCreateExecutor() {
    PaimonCompactionExecutorFactory factory = new PaimonCompactionExecutorFactory();
    factory.initialize(new HashMap<>());

    PaimonCompactionInput input = new PaimonCompactionInput();
    OptimizingExecutor<PaimonCompactionOutput> executor = factory.createExecutor(input);
    assertNotNull(executor);
    assertTrue(executor instanceof PaimonCompactionExecutor);
  }

  @Test
  @DisplayName("Executor execute() should throw UnsupportedOperationException")
  void testExecutorThrowsUnsupported() {
    PaimonCompactionInput input = new PaimonCompactionInput();
    PaimonCompactionExecutor executor = new PaimonCompactionExecutor(input);

    UnsupportedOperationException exception =
        assertThrows(UnsupportedOperationException.class, executor::execute);
    assertTrue(exception.getMessage().contains("not yet implemented"));
  }

  @Test
  @DisplayName(
      "PaimonCompactionInput should support option/options/getOptions from BaseOptimizingInput")
  void testCompactionInputOptions() {
    PaimonCompactionInput input = new PaimonCompactionInput();
    input.option("key1", "value1");

    Map<String, String> batch = new HashMap<>();
    batch.put("key2", "value2");
    batch.put("key3", "value3");
    input.options(batch);

    assertEquals(3, input.getOptions().size());
    assertEquals("value1", input.getOptions().get("key1"));
    assertEquals("value2", input.getOptions().get("key2"));
    assertEquals("value3", input.getOptions().get("key3"));
  }

  @Test
  @DisplayName("PaimonCompactionOutput summary should return empty map")
  void testCompactionOutputSummary() {
    PaimonCompactionOutput output = new PaimonCompactionOutput();
    assertNotNull(output.summary());
    assertTrue(output.summary().isEmpty());
  }

  @Test
  @DisplayName("PaimonCompactionInput should be serializable via SerializationUtil")
  void testInputSerialization() {
    PaimonCompactionInput input = new PaimonCompactionInput();
    input.option("key1", "value1");
    input.option("key2", "value2");

    ByteBuffer buffer = SerializationUtil.simpleSerialize(input);
    assertNotNull(buffer);

    byte[] bytes = new byte[buffer.remaining()];
    buffer.get(bytes);
    PaimonCompactionInput deserialized = SerializationUtil.simpleDeserialize(bytes);
    assertNotNull(deserialized);
    assertEquals("value1", deserialized.getOptions().get("key1"));
    assertEquals("value2", deserialized.getOptions().get("key2"));
  }
}
