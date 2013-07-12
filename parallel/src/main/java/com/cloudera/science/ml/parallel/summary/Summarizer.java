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
package com.cloudera.science.ml.parallel.summary;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.PCollection;
import org.apache.crunch.PObject;
import org.apache.crunch.Pair;
import org.apache.crunch.fn.Aggregators;
import org.apache.crunch.materialize.pobject.PObjectImpl;
import org.apache.crunch.types.avro.Avros;

import com.cloudera.science.ml.core.records.Record;
import com.cloudera.science.ml.core.records.Spec;
import com.cloudera.science.ml.core.summary.Summary;
import com.cloudera.science.ml.core.summary.SummaryStats;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Summarizer {
  
  private static final Logger LOG = LoggerFactory.getLogger(Summarizer.class);
  
  private final Set<Integer> ignoredColumns = Sets.newHashSet();
  private boolean defaultToSymbolic = false;
  private final Set<Integer> exceptionColumns = Sets.newHashSet();
  private Spec spec = null;
  private int maxLevels = 1000000;
  
  public Summarizer spec(Spec spec) {
    this.spec = spec;
    return this;
  }
  
  public Summarizer maxLevels(int maxLevels) {
    Preconditions.checkArgument(maxLevels >= 0);
    this.maxLevels = maxLevels;
    return this;
  }
  
  public Summarizer ignoreColumns(Integer... columns) {
    return ignoreColumns(Arrays.asList(columns));
  }

  public Summarizer defaultToSymbolic(boolean defaultToSymbolic) {
    this.defaultToSymbolic = defaultToSymbolic;
    return this;
  }

  public Summarizer ignoreColumns(Iterable<Integer> columns) {
    for (Integer c : columns) {
      ignoredColumns.add(c);
    }
    return this;
  }

  public Summarizer exceptionColumns(Integer... columns) {
    return exceptionColumns(Arrays.asList(columns));
  }

  public Summarizer exceptionColumns(Iterable<Integer> columns) {
    for (Integer c : columns) {
      exceptionColumns.add(c);
    }
    return this;
  }

  public PObject<Summary> build(PCollection<Record> input) {
    return new SummaryPObject(spec, input.parallelDo("summarize",
        new SummarizeFn(ignoredColumns, defaultToSymbolic, exceptionColumns, maxLevels),
        Avros.tableOf(Avros.ints(), Avros.pairs(Avros.longs(), Avros.reflects(InternalStats.class))))
        .groupByKey(1)
        .combineValues(Aggregators.pairAggregator(Aggregators.SUM_LONGS(), new InternalStats.Aggregator(maxLevels))));
  }

  private static class SummaryPObject extends PObjectImpl<Pair<Integer, Pair<Long, InternalStats>>, Summary> {
    private final Spec spec;
    
    private SummaryPObject(Spec spec, PCollection<Pair<Integer, Pair<Long, InternalStats>>> pc) {
      super(pc);
      this.spec = spec;
    }
    
    @Override
    protected Summary process(Iterable<Pair<Integer, Pair<Long, InternalStats>>> iter) {
      List<SummaryStats> ss = Lists.newArrayList();
      int fieldCount = 0;
      long recordCount = 0L;
      for (Pair<Integer, Pair<Long, InternalStats>> p : iter) {
        fieldCount++;
        recordCount = p.second().first();
        String name = spec != null ? spec.getField(p.first()).name() : "c" + p.first();
        SummaryStats stats = p.second().second().toSummaryStats(name, recordCount);
        if (stats.getMissing() > 0) {
          LOG.warn("{} missing/invalid values for numeric field {}, named '{}'",
                   new Object[] {stats.getMissing(), p.first(), name});
        }
        while (ss.size() <= p.first()) {
          ss.add(null); // fill in any blanks
        }
        ss.set(p.first(), stats);
      }
      if (spec != null) {
        while (ss.size() <= spec.size()) {
          ss.add(null); // Add blanks at the end
        }
        // Add placeholders for ignored fields in the summary
        for (int i = 0; i < spec.size(); i++) {
          SummaryStats s = ss.get(i);
          if (s == null) {
            String name = spec.getField(i).name();
            ss.set(i, new SummaryStats(name));
          }
        }
      }
      return new Summary(recordCount, fieldCount, ss);
    }
  }

  private static class SummarizeFn extends DoFn<Record, Pair<Integer, Pair<Long, InternalStats>>> {
    private final Set<Integer> ignoredColumns;
    private final boolean defaultToSymbolic;
    private final Set<Integer> exceptionColumns;
    private final int maxLevels;
    private final Map<Integer, InternalStats> stats;
    private long count;
    
    private SummarizeFn(
        Set<Integer> ignoreColumns,
        boolean defaultToSymbolic,
        Set<Integer> exceptionColumns,
        int maxLevels) {
      this.ignoredColumns = ignoreColumns;
      this.defaultToSymbolic = defaultToSymbolic;
      this.exceptionColumns = exceptionColumns;
      this.maxLevels = maxLevels;
      this.stats = Maps.newHashMap();
      this.count = 0;
    }
    
    @Override
    public void process(Record record,
        Emitter<Pair<Integer, Pair<Long, InternalStats>>> emitter) {
      for (int idx = 0; idx < record.getSpec().size(); idx++) {
        if (!ignoredColumns.contains(idx)) {
          InternalStats ss = stats.get(idx);
          if (ss == null) {
            ss = new InternalStats();
            stats.put(idx, ss);
          }
          boolean symbolic = exceptionColumns.contains(idx) ? !defaultToSymbolic : defaultToSymbolic;
          if (symbolic) {
            ss.addSymbol(record.getAsString(idx), maxLevels);
          } else {
            ss.addNumeric(record.getAsDouble(idx));
          }
        }
      }
      count++;
    }
    
    @Override
    public void cleanup(Emitter<Pair<Integer, Pair<Long, InternalStats>>> emitter) {
      for (Map.Entry<Integer, InternalStats> e : stats.entrySet()) {
        emitter.emit(Pair.of(e.getKey(), Pair.of(count, e.getValue())));
      }
      stats.clear();
    }
  }
}
