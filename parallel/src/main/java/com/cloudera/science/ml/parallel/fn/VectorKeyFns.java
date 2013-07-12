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
package com.cloudera.science.ml.parallel.fn;

import org.apache.crunch.MapFn;
import org.apache.mahout.math.NamedVector;
import org.apache.mahout.math.Vector;

/**
 * {@code MapFn}s for extracting the key type from a {@code Vector} instance
 * that has an identifier associated with it, like the name of a {@code NamedVector}.
 */
public final class VectorKeyFns {

  public static <V extends Vector> MapFn<V, Long> longKeyFn() {
    return new MapFn<V, Long>() {
      @Override
      public Long map(V v) {
        if (v instanceof NamedVector) {
          return Long.valueOf(((NamedVector) v).getName());
        }
        return 0L;
      }
    };
  }
  
  public static <V extends Vector> MapFn<V, Integer> intKeyFn() {
    return new MapFn<V, Integer>() {
      @Override
      public Integer map(V v) {
        if (v instanceof NamedVector) {
          return Integer.valueOf(((NamedVector) v).getName());
        }
        return 0;
      }
    };
  }

  public static <V extends Vector> MapFn<V, String> textKeyFn() {
    return new MapFn<V, String>() {
      @Override
      public String map(V v) {
        if (v instanceof NamedVector) {
          return ((NamedVector) v).getName();
        }
        return "";
      }
    };
  }
 
  private VectorKeyFns() { }
}
