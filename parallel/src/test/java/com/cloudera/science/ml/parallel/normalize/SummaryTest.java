/**
 * Copyright (c) 2012, Cloudera, Inc. All Rights Reserved.
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
package com.cloudera.science.ml.parallel.normalize;

import static org.junit.Assert.assertEquals;

import java.io.Serializable;
import java.util.Arrays;

import org.apache.crunch.MapFn;
import org.apache.crunch.PCollection;
import org.apache.crunch.impl.mem.MemPipeline;
import org.apache.crunch.types.avro.AvroTypeFamily;
import org.apache.crunch.types.avro.Avros;
import org.apache.mahout.math.Vector;
import org.junit.Test;

import com.cloudera.science.ml.core.records.DataType;
import com.cloudera.science.ml.core.records.Record;
import com.cloudera.science.ml.core.records.RecordSpec;
import com.cloudera.science.ml.core.records.csv.CSVRecord;
import com.cloudera.science.ml.core.records.vectors.VectorRecord;
import com.cloudera.science.ml.core.summary.Summary;
import com.cloudera.science.ml.core.vectors.Vectors;
import com.cloudera.science.ml.parallel.summary.Summarizer;
import com.cloudera.science.ml.parallel.types.MLAvros;
import com.cloudera.science.ml.parallel.types.MLRecords;
import com.google.common.collect.ImmutableList;

public class SummaryTest implements Serializable {
  private final PCollection<Vector> vecs = MemPipeline.typedCollectionOf(
      MLAvros.vector(),
      Vectors.of(1.0, 3.0),
      Vectors.of(1.0, 1.0),
      Vectors.of(3.0, 1.0),
      Vectors.of(3.0, 3.0));
  
  private final PCollection<String> strings = MemPipeline.typedCollectionOf(
      Avros.strings(),
      "1.0,NA,2.0",
      "1.0,2.0,3.0");
  
  @Test
  public void testZScores() {
    PCollection<Record> elems = vecs.parallelDo(new MapFn<Vector, Record>() {
      @Override
      public Record map(Vector vec) {
        return new VectorRecord(vec);
      }
    }, null);
    Summarizer sr = new Summarizer();
    Summary s = sr.build(elems).getValue();
    Normalizer stand = Normalizer.builder()
        .summary(s)
        .defaultTransform(Transform.Z)
        .build();
    assertEquals(ImmutableList.of(Vectors.of(-1, 1),
        Vectors.of(-1, -1), Vectors.of(1, -1),
        Vectors.of(1, 1)), stand.apply(elems, MLAvros.vector()).materialize());
  }
  
  @Test
  public void testMissing() throws Exception {
    PCollection<Record> elems = strings.parallelDo(new MapFn<String, Record>() {
      @Override
      public Record map(String input) {
        return new CSVRecord(Arrays.asList(input.split(",")));
      }
    }, MLRecords.csvRecord(AvroTypeFamily.getInstance(), ","));
    Summarizer sr = new Summarizer();
    Summary s = sr.build(elems).getValue();
    assertEquals(1, s.getStats(1).getMissing());
    assertEquals(2.0, s.getStats(1).mean(), 0.01);
    assertEquals(0.0, s.getStats(1).stdDev(), 0.01);
  }
  
  @Test
  public void testTrailingIgnoredFields() throws Exception {
    RecordSpec spec = RecordSpec.builder().add("field1", DataType.DOUBLE)
        .add("field2", DataType.DOUBLE).add("field3", DataType.DOUBLE).build();
    PCollection<Record> elems = strings.parallelDo(new MapFn<String, Record>() {
      @Override
      public Record map(String input) {
        return new CSVRecord(Arrays.asList(input.split(",")));
      }
    }, MLRecords.csvRecord(AvroTypeFamily.getInstance(), ","));
    Summarizer sr = new Summarizer().spec(spec).ignoreColumns(2);
    sr.build(elems).getValue();
  }
}
