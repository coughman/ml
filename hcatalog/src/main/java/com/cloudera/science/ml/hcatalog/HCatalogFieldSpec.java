/**
 * Copyright (c) 2013, Cloudera, Inc. All Rights Reserved.
 *
 * Cloudera, Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the
 * License.
 */
package com.cloudera.science.ml.hcatalog;

import org.apache.hcatalog.data.schema.HCatFieldSchema;

import com.cloudera.science.ml.core.records.BasicSpec;
import com.cloudera.science.ml.core.records.DataType;
import com.cloudera.science.ml.core.records.FieldSpec;
import com.cloudera.science.ml.core.records.Spec;

public class HCatalogFieldSpec implements FieldSpec {

  private final HCatFieldSchema schema;
  private final int position;
  
  public HCatalogFieldSpec(HCatFieldSchema schema, int position) {
    this.schema = schema;
    this.position = position;
  }
  
  @Override
  public String name() {
    return schema.getName();
  }

  @Override
  public int position() {
    return position;
  }

  @Override
  public Spec spec() {
    switch (schema.getType()) {
    case BOOLEAN:
      return new BasicSpec(DataType.BOOLEAN);
    case INT:
    case SMALLINT:
    case TINYINT:
      return new BasicSpec(DataType.INT);
    case BIGINT:
      return new BasicSpec(DataType.LONG);
    case FLOAT:
    case DOUBLE:
      return new BasicSpec(DataType.DOUBLE);
    case STRING:
      return new BasicSpec(DataType.STRING);
      default:
        throw new IllegalArgumentException(
            "Unsupported field type: " + schema.getType());
    }
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder(schema.getName());
    sb.append(' ').append(schema.getType());
    return sb.toString();
  }
}
