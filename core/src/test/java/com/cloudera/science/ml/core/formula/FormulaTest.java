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
package com.cloudera.science.ml.core.formula;

import static org.junit.Assert.assertEquals;

import java.util.Map;

import org.junit.Test;

import com.cloudera.science.ml.core.records.DataType;
import com.cloudera.science.ml.core.records.Record;
import com.cloudera.science.ml.core.records.RecordSpec;
import com.cloudera.science.ml.core.records.SimpleRecord;
import com.cloudera.science.ml.core.records.Spec;
import com.cloudera.science.ml.core.summary.Entry;
import com.cloudera.science.ml.core.summary.Numeric;
import com.cloudera.science.ml.core.summary.Summary;
import com.cloudera.science.ml.core.summary.SummaryStats;
import com.cloudera.science.ml.core.vectors.Vectors;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;

public class FormulaTest {

  private final Spec abSpec = RecordSpec.builder()
      .add("a", DataType.DOUBLE)
      .add("b", DataType.STRING)
      .build();
  
  @Test
  public void testBasic() {
    Map<String, Entry> bHistogram = Maps.newTreeMap();
    bHistogram.put("x", new Entry(1L));
    bHistogram.put("y", new Entry(1L));
    bHistogram.put("z", new Entry(1L));
    Summary summary = new Summary(1, 2, ImmutableList.of(
        new SummaryStats("a", new Numeric(17.29, 17.29, 17.29, 0.0)),
        new SummaryStats("b", bHistogram, false)));
    Formula f = Formula.compile(ImmutableList.of(Term.INTERCEPT, Term.$("a"), Term.$("a", "b")),
        summary);
    
    Record r = new SimpleRecord(abSpec, 17.29, "z");
    assertEquals(Vectors.of(1.0, 17.29, 0.0, 17.29), f.apply(r));
    r = new SimpleRecord(abSpec, 1.2, "x");
    assertEquals(Vectors.of(1.0, 1.2, 0.0, 0.0), f.apply(r));
    r = new SimpleRecord(abSpec, 2, "y");
    assertEquals(Vectors.of(1.0, 2.0, 2.0, 0.0), f.apply(r));
  }
}
