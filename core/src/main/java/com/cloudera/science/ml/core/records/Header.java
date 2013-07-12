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
package com.cloudera.science.ml.core.records;

import java.io.File;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Files;

/**
 *
 */
public class Header {

  public enum Type {
    ID,
    IGNORED,
    NUMERIC,
    SYMBOLIC
  }
  private static final Set<String> SYMBOL_META = ImmutableSet.of("symbolic",
      "categorical", "nominal", "string");
  private static final Set<String> NUMERIC_META = ImmutableSet.of("numeric",
      "continuous", "real", "double");
  private static final Pattern COMMA = Pattern.compile(",");

  private final LinkedHashMap<String, Type> data;
  
  public static Header fromFile(File headerFile) throws IOException {
    LinkedHashMap<String, Type> data = Maps.newLinkedHashMap();
    boolean hasId = false;
    for (String line : Files.readLines(headerFile, Charsets.UTF_8)) {
      if (line.contains(",")) {
        String[] pieces = COMMA.split(line);
        if (pieces.length != 2) {
          throw new IllegalArgumentException("Invalid header file row: " + line);
        }
        String name = pieces[0];
        String meta = pieces[1].toLowerCase(Locale.ENGLISH).trim();
        if (meta.startsWith("ignore")) {
          data.put(name, Type.IGNORED);
        } else if (meta.startsWith("id")) { 
          if (hasId) {
            throw new IllegalArgumentException("Multiple ID columns in header file");
          } else {
            data.put(name, Type.ID);
            hasId = true;
          }
        } else if (SYMBOL_META.contains(meta)) {
          data.put(name, Type.SYMBOLIC);
        } else if (NUMERIC_META.contains(meta)) {
          data.put(name, Type.NUMERIC);
        } else {
          throw new IllegalArgumentException(String.format(
              "Did not recognize metadata %s for field %s", meta, name));
        }
      } else if (!line.matches("\\s*")) {
        data.put(line, Type.NUMERIC);
      }
    }
    return new Header(data);
  }
  
  public static class Builder {
    
    LinkedHashMap<String, Type> data = Maps.newLinkedHashMap();
    
    public Builder addNumeric(String name) {
      data.put(name, Type.NUMERIC);
      return this;
    }
    
    public Builder addSymbolic(String name) {
      data.put(name, Type.SYMBOLIC);
      return this;
    }
    
    public Builder addIgnored(String name) {
      data.put(name, Type.IGNORED);
      return this;
    }
    
    public Builder addIdentifier(String name) {
      data.put(name, Type.ID);
      return this;
    }
    
    public Header build() {
      return new Header(data);
    }
  }
  
  public static Builder builder() {
    return new Builder();
  }
  
  private Header(LinkedHashMap<String, Type> data) {
    this.data = data;
  }
  
  public Spec toSpec() {
    RecordSpec.Builder rsb = RecordSpec.builder();
    for (Map.Entry<String, Type> e : data.entrySet()) {
      switch (e.getValue()) {
      case NUMERIC:
        rsb.add(e.getKey(), DataType.DOUBLE);
        break;
      case SYMBOLIC:
      case ID:
      case IGNORED:
        rsb.add(e.getKey(), DataType.STRING);
        break;
      }
    }
    return rsb.build();
  }
  
  public List<Integer> getNumericColumns() {
    return getColumns(Type.NUMERIC);
  }
  
  public List<Integer> getSymbolicColumns() {
    return getColumns(Type.SYMBOLIC);
  }
  
  public List<Integer> getIgnoredColumns() {
    return getColumns(Type.IGNORED, Type.ID);
  }
  
  public Integer getIdColumn() {
    List<Integer> id = getColumns(Type.ID);
    if (id.isEmpty()) {
      return null;
    } else {
      return id.get(0);
    }
  }
  
  private List<Integer> getColumns(Type... targets) {
    List<Integer> ret = Lists.newArrayList();
    int index = 0;
    for (Type t : data.values())  {
      for (Type target : targets) {
        if (t == target) {
          ret.add(index);
          break;
        }
      }
      index++;
    }
    return ret;
  }
  
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    for (Map.Entry<String, Type> e : data.entrySet()) {
      sb.append(e.getKey()).append(",").append(e.getValue().toString().toLowerCase()).append("\n");
    }
    return sb.toString();
  }
}
