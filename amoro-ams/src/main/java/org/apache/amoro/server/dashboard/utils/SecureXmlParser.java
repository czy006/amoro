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

package org.apache.amoro.server.dashboard.utils;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import java.io.ByteArrayInputStream;

/**
 * Validates untrusted XML with DTDs and external entities disabled. XML uploaded by dashboard users
 * must never be handed to a parser that resolves external entities: the entity system id would let
 * the server be turned into an SSRF pivot, and entity expansion enables denial-of-service via
 * exponential entity blowup. Every property below fails closed on both the JDK-built-in and
 * Woodstox factories.
 */
public final class SecureXmlParser {

  private SecureXmlParser() {}

  /**
   * Check that the given bytes are well-formed XML without resolving any DTD or entity it declares.
   * Documents containing a DOCTYPE are rejected.
   *
   * @param xml bytes of the document to validate
   * @throws XMLStreamException if the document is not well-formed or declares a DTD
   */
  public static void validateWellFormed(byte[] xml) throws XMLStreamException {
    XMLStreamReader reader = newFactory().createXMLStreamReader(new ByteArrayInputStream(xml));
    try {
      while (reader.hasNext()) {
        int event = reader.next();
        if (event == XMLStreamConstants.DTD || event == XMLStreamConstants.ENTITY_REFERENCE) {
          // parsers may skip DTDs silently when disabled; reject them explicitly so a DOCTYPE
          // can never slip into storage regardless of factory behavior
          throw new XMLStreamException(
              "DTD declarations and entity references are not accepted in uploaded files");
        }
      }
    } finally {
      reader.close();
    }
  }

  private static XMLInputFactory newFactory() {
    XMLInputFactory factory = XMLInputFactory.newInstance();
    // No DTD support at all: internal subsets, external entities and entity-expansion attacks
    // (billion laughs) all disappear when DTDs are refused up front.
    factory.setProperty(XMLInputFactory.SUPPORT_DTD, false);
    factory.setProperty(XMLInputFactory.IS_SUPPORTING_EXTERNAL_ENTITIES, false);
    factory.setProperty(XMLInputFactory.IS_REPLACING_ENTITY_REFERENCES, false);
    return factory;
  }
}
