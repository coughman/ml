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
package com.cloudera.science.ml.client.util;

import java.util.List;

import org.apache.crunch.PCollection;

import com.google.common.base.Function;
import com.google.common.collect.Lists;

public final class UnionIO {

  private UnionIO() {
  }

  public static <T> PCollection<T> from(List<String> paths, Function<String, PCollection<T>> f) {
    PCollection<T> ret = null;
    for (PCollection<T> p : Lists.transform(paths, f)) {
      if (ret == null) {
        ret = p;
      } else {
        ret = ret.union(p);
      }
    }
    return ret;
  }
}
