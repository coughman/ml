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
package com.cloudera.science.ml.core.summary;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.cloudera.science.ml.core.records.Spec;
import com.cloudera.science.ml.core.records.Specs;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

public class SummaryBuilder {

  private final Spec spec;
  private final List<SummaryStats> stats;
  
  public SummaryBuilder(Spec spec) {
    this.spec = Preconditions.checkNotNull(spec);
    this.stats = Arrays.asList(new SummaryStats[spec.size()]);
  }
  
  public SummaryBuilder numeric(String name, Numeric numeric) {
    int index = Specs.getFieldId(spec, name);
    SummaryStats stat = new SummaryStats(name, numeric);
    stats.set(index, stat);
    return this;
  }
  
  public SummaryBuilder categorical(String name, List<String> levels) {
    int index = Specs.getFieldId(spec, name);
    Map<String, Entry> hist = Maps.newTreeMap();
    for (String level : levels) {
      hist.put(level, new Entry(1L));
    }
    SummaryStats stat = new SummaryStats(name, hist, false);
    stats.set(index, stat);
    return this;
  }
  
  public Summary build() {
    return new Summary(spec, stats);
  }
}
