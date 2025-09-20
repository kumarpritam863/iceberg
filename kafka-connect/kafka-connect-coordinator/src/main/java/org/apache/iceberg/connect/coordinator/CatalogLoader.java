/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.connect.coordinator;

import java.util.Map;
import org.apache.iceberg.catalog.Catalog;

/**
 * Utility class for loading Iceberg catalogs based on configuration.
 */
public class CatalogLoader {

  /**
   * Load a catalog with the given name and properties.
   *
   * @param catalogName the catalog name
   * @param catalogProperties the catalog configuration properties
   * @return the loaded catalog
   * @throws RuntimeException if catalog loading fails
   */
  public static Catalog load(String catalogName, Map<String, String> catalogProperties) {
    try {
      String catalogImpl = catalogProperties.get("catalog-impl");
      if (catalogImpl == null) {
        throw new IllegalArgumentException("Missing catalog-impl property");
      }

      @SuppressWarnings("unchecked")
      Class<? extends Catalog> catalogClass = (Class<? extends Catalog>) Class.forName(catalogImpl);
      Catalog catalog = catalogClass.getDeclaredConstructor().newInstance();

      if (catalog instanceof org.apache.iceberg.BaseMetastoreCatalog) {
        ((org.apache.iceberg.BaseMetastoreCatalog) catalog).initialize(catalogName, catalogProperties);
      }

      return catalog;
    } catch (Exception e) {
      throw new RuntimeException("Failed to load catalog: " + catalogName, e);
    }
  }
}