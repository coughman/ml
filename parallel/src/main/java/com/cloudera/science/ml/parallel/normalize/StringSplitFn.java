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
package com.cloudera.science.ml.parallel.normalize;

import java.io.IOException;
import java.io.StringReader;

import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVStrategy;
import org.apache.crunch.CrunchRuntimeException;
import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.PCollection;

import com.cloudera.science.ml.core.records.Record;
import com.cloudera.science.ml.core.records.csv.CSVRecord;
import com.cloudera.science.ml.parallel.types.MLRecords;

public class StringSplitFn extends DoFn<String, Record> {

  private final char delim;
  private final char quote;
  private final char comment;
  private transient CSVStrategy csvStrategy;
  
  public static PCollection<Record> apply(PCollection<String> in, char delim) {
    return apply(in, delim, null, null);
  }
  
  public static PCollection<Record> apply(PCollection<String> in, char delim,
      Character quote,
      Character comment) {
    if (quote == null) {
      quote = '"';
    }
    if (comment == null) {
      comment = CSVStrategy.COMMENTS_DISABLED;
    }
    return in.parallelDo("string-split",
        new StringSplitFn(delim, quote, comment),
        MLRecords.csvRecord(in.getTypeFamily(), String.valueOf(delim)));
  }
  
  public StringSplitFn(char delim, char quote, char comment) {
    this.delim = delim;
    this.quote = quote;
    this.comment = comment;
  }

  @Override
  public void initialize() {
    this.csvStrategy = new CSVStrategy(delim, quote, comment);
  }
  
  @Override
  public void process(String line, Emitter<Record> emitter) {
    if (line == null || line.isEmpty()) {
      return;
    }
    try {
      String[] pieces = new CSVParser(new StringReader(line), csvStrategy).getLine();
      if (pieces != null) {
        emitter.emit(new CSVRecord(pieces));
      }
    } catch (IOException e) {
      throw new CrunchRuntimeException(e);
    }
  }
}
