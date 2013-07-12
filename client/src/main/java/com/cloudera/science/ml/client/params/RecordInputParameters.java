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
package com.cloudera.science.ml.client.params;

import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;

import org.apache.crunch.PCollection;
import org.apache.crunch.Pipeline;
import org.apache.crunch.io.From;
import org.apache.crunch.types.PType;
import org.apache.crunch.types.avro.AvroType;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.converters.CommaParameterSplitter;
import com.cloudera.science.ml.client.cmd.CommandException;
import com.cloudera.science.ml.client.util.UnionIO;
import com.cloudera.science.ml.core.records.Header;
import com.cloudera.science.ml.core.records.Record;
import com.cloudera.science.ml.core.records.Spec;
import com.cloudera.science.ml.core.summary.Summary;
import com.cloudera.science.ml.hcatalog.HCatalog;
import com.cloudera.science.ml.hcatalog.HCatalogSource;
import com.cloudera.science.ml.mahout.types.MLWritables;
import com.cloudera.science.ml.parallel.normalize.StringSplitFn;
import com.cloudera.science.ml.parallel.records.Records;
import com.cloudera.science.ml.parallel.records.SummarizedRecords;
import com.cloudera.science.ml.parallel.types.MLAvros;
import com.cloudera.science.ml.parallel.types.MLRecords;
import com.google.common.base.Function;

/**
 * Class specifies the common input parameters that may be used across ML commands
 * that process records.
 *
 * The following options are supported:
 *
 * <PRE>
 * <b>--input-paths</b>
 *     Comma separated paths to be used as input or Hive table names
 *
 * <b>--format</b>
 *     format of the Input. Possible values are seq, text, hive, and avro
 *
 * <b>--delim</b>
 *     Delimited to be used for text input files. Default is ','
 *
 * <b>--ignore-lines</b>
 *     Regular expression based on which lines in text input file shall be ignored
 * </PRE>
 */
public class RecordInputParameters {

  public static final String TEXT = "text";
  public static final String FORMAT_SEQ = "seq";
  public static final String FORMAT_AVRO = "avro";
  public static final String FORMAT_HIVE = "hive";
  
  @Parameter(names = "--input-paths",
      description = "CSV of the input paths/tables to consider",
      splitter = CommaParameterSplitter.class,
      required = true)
  private List<String> inputPaths;

  @Parameter(names = "--format",
      description = "One of 'text', 'hive', 'seq', or 'avro' to describe the format of the input",
      required = true)
  private String format;
  
  @Parameter(names = "--delim",
      description = "For text files, the character delimiter to use for separate fields")
  private String delim = ",";
  
  @Parameter(names = "--quote-char",
      description = "For text files, the character that indicates quoted text (usually ''' or '\"')")
  private String quote;
  
  @Parameter(names = "--comment-char",
      description = "For text files, the character that indicates that a line should be ignored")
  private String comment;
  
  public String getDelimiter() {
    return delim;
  }
  
  public SummarizedRecords getSummarizedRecords(Pipeline pipeline, Summary summary) {
    Records records = getRecords(pipeline, summary.getSpec());
    return new SummarizedRecords(records.get(), summary);
  }
  
  public Records getRecords(Pipeline pipeline, Header header) {
    Spec spec = header == null ? null : header.toSpec();
    return getRecords(pipeline, spec);
  }
  
  private Records getRecords(final Pipeline pipeline, Spec spec) {
    format = format.toLowerCase(Locale.ENGLISH);
    PCollection<Record> ret;
    if (TEXT.equals(format)) {
      PCollection<String> text = fromInputs(new Function<String, PCollection<String>>() {
        @Override
        public PCollection<String> apply(String input) {
          return pipeline.readTextFile(input);
        }
      });
      ret = StringSplitFn.apply(text, delim.charAt(0), quote == null ? null : quote.charAt(0),
          comment == null ? null : comment.charAt(0));
      if (spec == null) {
        throw new CommandException("Text input records must have a --header-file provided");
      }
    } else if (FORMAT_SEQ.equals(format)) {
      final PType<Record> ptype = MLRecords.vectorRecord(MLWritables.vector());
      ret = fromInputs(new Function<String, PCollection<Record>>() {
        @Override
        public PCollection<Record> apply(String input) {
          return pipeline.read(From.sequenceFile(input, ptype));
        }
      });
    } else if (FORMAT_AVRO.equals(format)) {
      final AvroType<Record> ptype = (AvroType<Record>) MLRecords.vectorRecord(MLAvros.vector());
      ret = fromInputs(new Function<String, PCollection<Record>>() {
        @Override
        public PCollection<Record> apply(String input) {
          return pipeline.read(From.avroFile(input, ptype));
        }
      });
    } else if (FORMAT_HIVE.equals(format)) {
 
      ret = fromInputs(new Function<String, PCollection<Record>>() {
        @Override
        public PCollection<Record> apply(String table) {
          return pipeline.read(new HCatalogSource(HCatalog.getDbName(table),
              HCatalog.getTableName(table)));
        }
      });
      if (spec == null) {
        spec = HCatalog.getSpec(HCatalog.getDbName(inputPaths.get(0)),
            HCatalog.getTableName(inputPaths.get(0)));
      }
    } else {
      throw new CommandException("Unknown format: " + format);
    }
    return new Records(ret, spec);
  }

  private <T> PCollection<T> fromInputs(Function<String, PCollection<T>> f) {
    return UnionIO.from(inputPaths, f);
  }
}
