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
package com.cloudera.science.ml.hcatalog;

import java.io.IOException;
import java.util.Properties;

import org.apache.crunch.Source;
import org.apache.crunch.io.CrunchInputs;
import org.apache.crunch.io.FormatBundle;
import org.apache.crunch.io.SourceTargetHelper;
import org.apache.crunch.types.PType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hcatalog.common.HCatConstants;
import org.apache.hcatalog.common.HCatException;
import org.apache.hcatalog.common.HCatUtil;
import org.apache.hcatalog.data.schema.HCatSchema;
import org.apache.hcatalog.mapreduce.HCatInputFormat;
import org.apache.hcatalog.mapreduce.InputJobInfo;

import com.cloudera.science.ml.core.records.Record;

/**
 * A basic {@code Source} for reading {@code Record} instances from Hive
 * via HCatalog.
 */
public class HCatalogSource implements Source<Record> {

  private final InputJobInfo info;
  
  private HCatSchema schema;
  private Path location;
  
  public HCatalogSource(String tableName) {
    this("default", tableName);
  }
  
  public HCatalogSource(String dbName, String tableName) {
    this(dbName, tableName, null);
  }
  public HCatalogSource(String dbName, String tableName, String filter) {
    this(dbName, tableName, filter, null);
  }
  
  public HCatalogSource(String dbName, String tableName, String filter, Properties props) {
    this.info = InputJobInfo.create(dbName, tableName, filter, props);
  }
  
  public InputJobInfo getInfo() {
    return info;
  }
  
  @Override
  public void configureSource(Job job, int inputId) throws IOException {
    if (inputId == -1) {
      HCatInputFormat.setInput(job, info);
      job.setInputFormatClass(HCatInputFormat.class);
    } else {
      FormatBundle<HCatInputFormat> bundle = FormatBundle.forInput(HCatInputFormat.class);
      bundle.set(HCatConstants.HCAT_KEY_JOB_INFO, HCatUtil.serialize(info));
      CrunchInputs.addInputPath(job, new Path(info.getTableName()), bundle, inputId);
    }
  }

  @Override
  public long getSize(Configuration conf) {
    if (location == null) {
      fetchTableData();
    }
    try {
      // TODO: be smarter about partitions
      return SourceTargetHelper.getPathSize(conf, location);
    } catch (IOException e) {
      throw new IllegalStateException("Failed to get the size of table at: " + location, e);
    }
  }

  @Override
  public PType<Record> getType() {
    if (schema == null) {
      fetchTableData();
    }
    return HCatalog.records(schema);
  }
  
  private void fetchTableData() {
    Table table = HCatalog.getTable(info.getDatabaseName(), info.getTableName());
    try {
      this.schema = HCatUtil.extractSchema(table);
    } catch (HCatException e) {
      throw new RuntimeException("Error fetching HCatalog schema", e);
    }
    this.location = table.getPath();
  }
  
  @Override
  public boolean equals(Object other) {
    if (!(other instanceof HCatalogSource)) {
      return false;
    }
    HCatalogSource hs = (HCatalogSource) other;
    return info.equals(hs.info);
  }
  
  @Override
  public int hashCode() {
    return info.hashCode();
  }
  
  @Override
  public String toString() {
    return "Hive(" + info.getTableName() + ')';
  }
}
