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

import java.io.Serializable;
import java.util.Collections;
import java.util.List;

import org.apache.mahout.math.Vector;

import com.cloudera.science.ml.core.records.FieldSpec;
import com.cloudera.science.ml.core.records.Record;
import com.cloudera.science.ml.core.records.Spec;
import com.cloudera.science.ml.core.summary.Summary;
import com.cloudera.science.ml.core.summary.SummaryStats;
import com.cloudera.science.ml.core.vectors.Vectors;
import com.google.common.base.Function;
import com.google.common.collect.Lists;

/**
 *
 */
public class Formula implements Function<Record, Vector>, Serializable {

  private final List<CompiledTerm> terms;
  private final int size;
  private final boolean sparse;
  private final boolean hasIntercept;
  
  public static Formula compile(List<Term> terms, Summary summary) {
    Spec spec = summary.getSpec();
    List<Term> internalTerms = Lists.newArrayList(terms);
    Collections.sort(internalTerms);
    List<CompiledTerm> compiled = Lists.newArrayListWithExpectedSize(terms.size());
    int offset = 0;
    boolean hasIntercept = internalTerms.get(0).isIntercept();
    for (Term t : terms) {
      List<Integer> numerics = Lists.newArrayList();
      List<Integer> categoricals = Lists.newArrayList();
      List<List<String>> hist = Lists.newArrayList();
      for (String field : t) {
        FieldSpec fs = spec.getField(field);
        SummaryStats ss = summary.getStats(fs.position());
        if (ss.isNumeric()) {
          numerics.add(fs.position());
        } else {
          categoricals.add(fs.position());
          hist.add(ss.getLevels());
        }
      }
      compiled.add(new CompiledTerm(numerics, categoricals, hist, offset));
      if (categoricals.isEmpty()) {
        offset++;
      } else {
        int prod = 1;
        for (List<String> h : hist) {
          prod *= h.size();
        }
        offset += prod - (hasIntercept ? 1 : 0);
      }
    }
    return new Formula(compiled, offset, offset > 2 * terms.size(), hasIntercept);
  }
  
  private Formula(List<CompiledTerm> terms, int size, boolean sparse, boolean hasIntercept) {
    this.terms = terms;
    this.size = size;
    this.sparse = sparse;
    this.hasIntercept = hasIntercept;
  }
  
  @Override
  public Vector apply(Record record) {
    Vector v = sparse ? Vectors.sparse(size) : Vectors.dense(size);
    for (CompiledTerm t : terms) {
      t.update(record, v, hasIntercept);
    }
    return v;
  }

  private static class CompiledTerm implements Serializable {
    private final List<Integer> numericTerms;
    private final List<Integer> categoricalTerms;
    private final List<List<String>> histograms;
    private final int baseOffset;
    
    CompiledTerm(List<Integer> numericTerms, List<Integer> categoricalTerms,
        List<List<String>> histograms, int baseOffset) {
      this.numericTerms = numericTerms;
      this.categoricalTerms = categoricalTerms;
      this.histograms = histograms;
      this.baseOffset = baseOffset;
    }
    
    public void update(Record input, Vector output, boolean hasIntercept) {
      double value = 1.0;
      for (Integer numericTerm : numericTerms) {
        value *= input.getAsDouble(numericTerm);
      }
      int base = 1;
      int offset = 0;
      for (int i = 0; i < categoricalTerms.size(); i++) {
        String level = input.getAsString(categoricalTerms.get(i));
        int index = Collections.binarySearch(histograms.get(i), level);
        offset += base * index;
        base *= histograms.get(i).size();
      }
      int index = baseOffset + offset;
      if (!hasIntercept || categoricalTerms.isEmpty()) {
        output.set(index, value);
      } else if (index > 0) {
        output.set(index - 1, value);
      }
    }
  }
}
