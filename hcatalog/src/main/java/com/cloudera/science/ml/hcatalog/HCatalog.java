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

import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.crunch.MapFn;
import org.apache.crunch.types.PType;
import org.apache.crunch.types.writable.Writables;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hcatalog.common.HCatException;
import org.apache.hcatalog.common.HCatUtil;
import org.apache.hcatalog.data.DefaultHCatRecord;
import org.apache.hcatalog.data.HCatRecord;
import org.apache.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hcatalog.data.schema.HCatSchema;
import org.apache.hcatalog.data.schema.HCatFieldSchema.Type;

import com.cloudera.science.ml.core.records.DataType;
import com.cloudera.science.ml.core.records.FieldSpec;
import com.cloudera.science.ml.core.records.Record;
import com.cloudera.science.ml.core.records.Spec;
import com.google.common.collect.Lists;

public final class HCatalog {

  private static final Log LOG = LogFactory.getLog(HCatalog.class);
  
  private static boolean HIVE_CHECKS = false;
  
  private static HiveMetaStoreClient getClientInstance() {
    if (!HIVE_CHECKS) {
      try {
        Class.forName("org.apache.hadoop.hive.conf.HiveConf");
      } catch (ClassNotFoundException e) {
        throw new IllegalStateException("Hive features requested, but Hive libraries not on classpath");
      }
      HiveConf hc = new HiveConf();
      LOG.info("Connecting to Hive Server at: " + hc.getVar(HiveConf.ConfVars.METASTOREURIS));
      HIVE_CHECKS = true;
    }
    try {
      return HCatUtil.getHiveClient(new HiveConf());
    } catch (Exception e) {
      throw new RuntimeException("Could not connect to Hive", e);
    }
  }
  
  public static Table getTable(String dbName, String tableName) {
    HiveMetaStoreClient client = getClientInstance();
    Table table;
    try {
      table = HCatUtil.getTable(client, dbName, tableName);
    } catch (Exception e) {
      throw new RuntimeException("Hive table lookup exception", e);
    }
    
    if (table == null) {
      throw new IllegalStateException("Could not find info for table: " + tableName);
    }
    return table;
  }
  
  public static boolean tableExists(String dbName, String tableName) {
    HiveMetaStoreClient client = getClientInstance();
    try {
      return client.tableExists(dbName, tableName);
    } catch (Exception e) {
      throw new RuntimeException("Hive metastore exception", e);
    }
  }
  
  public static void createTable(Table tbl) {
    HiveMetaStoreClient client = getClientInstance();
    try {
      client.createTable(tbl.getTTable());
    } catch (Exception e) {
      throw new RuntimeException("Hive table creation exception", e);
    }
  }
  
  public static void dropTable(String dbName, String tableName) {
    HiveMetaStoreClient client = getClientInstance();
    try {
      client.dropTable(dbName, tableName, true /* deleteData */,
          true /* ignoreUnknownTable */);
    } catch (Exception e) {
      throw new RuntimeException("Hive metastore exception", e);
    }
  }
  
  public static String getDbName(String hiveStr) {
    int dotIdx = hiveStr.indexOf('.');
    if (dotIdx == -1) {
      return "default";
    }
    return hiveStr.substring(0, dotIdx);
  }
  
  public static String getTableName(String hiveStr) {
    int dotIdx = hiveStr.indexOf('.');
    if (dotIdx == -1) {
      return hiveStr;
    }
    return hiveStr.substring(dotIdx + 1, hiveStr.length());
  }
  
  public static HCatalogSpec getSpec(String dbName, String tableName) {
    Table table = getTable(dbName, tableName);
    try {
      return new HCatalogSpec(HCatUtil.extractSchema(table));
    } catch (HCatException e) {
      throw new RuntimeException("HCatalog schema extraction error", e);
    }
  }
  
  public static HCatSchema getHCatSchema(Spec spec) {
    if (spec instanceof HCatalogSpec) {
      return ((HCatalogSpec) spec).getImpl();
    }
    List<HCatFieldSchema> fields = Lists.newArrayListWithExpectedSize(spec.size());
    try {
      for (int i = 0; i < spec.size(); i++) {
        FieldSpec fs = spec.getField(i);
        DataType dt = fs.spec().getDataType();
        switch (dt) {
        case BOOLEAN:
          fields.add(new HCatFieldSchema(fs.name(), Type.BOOLEAN, ""));
          break;
        case INT:
          fields.add(new HCatFieldSchema(fs.name(), Type.INT, ""));
          break;
        case DOUBLE:
          fields.add(new HCatFieldSchema(fs.name(), Type.DOUBLE, ""));
          break;
        case STRING:
          fields.add(new HCatFieldSchema(fs.name(), Type.STRING, ""));
          break;
        case LONG:
          fields.add(new HCatFieldSchema(fs.name(), Type.BIGINT, ""));
          break;
        default:
          throw new UnsupportedOperationException("Unhandled data type = " + dt);
        }
      }
    } catch (HCatException e) {
      throw new RuntimeException(e);
    }
    return new HCatSchema(fields);
  }
  
  public static PType<Record> records(HCatSchema dataSchema) {
    return Writables.derived(Record.class, new HCatInFn(dataSchema),
        new HCatOutFn(dataSchema), Writables.writables(HCatRecord.class));
  }

  public static PType<Record> records(Spec spec) {
    return records(getHCatSchema(spec));
  }
  
  private static class HCatInFn extends MapFn<HCatRecord, Record> {
    private final HCatSchema dataSchema;
    
    HCatInFn(HCatSchema dataSchema) {
      this.dataSchema = dataSchema;
    }
    
    @Override
    public Record map(HCatRecord impl) {
      return new HCatalogRecord(impl, dataSchema);
    }
  }
  
  private static class HCatOutFn extends MapFn<Record, HCatRecord> {
    private final HCatSchema dataSchema;
    
    HCatOutFn(HCatSchema dataSchema) {
      this.dataSchema = dataSchema;
    }
    
    @Override
    public HCatRecord map(Record rec) {
      if (rec instanceof HCatalogRecord) {
        HCatalogRecord hcrec = (HCatalogRecord) rec;
        if (dataSchema.equals(hcrec.getSchema())) {
          return ((HCatalogRecord) rec).getImpl();
        }
      }

      Spec spec = rec.getSpec();
      List<Object> base = Arrays.asList(new Object[spec.size()]);
      DefaultHCatRecord out = new DefaultHCatRecord(base);
      try {
        for (int i = 0; i < spec.size(); i++) {
          FieldSpec fs = spec.getField(i);
          DataType dt = fs.spec().getDataType();
          switch (dt) {
          case BOOLEAN:
            out.setBoolean(fs.name(), dataSchema, rec.getBoolean(i));
            break;
          case INT:
            out.setInteger(fs.name(), dataSchema, rec.getInteger(i));
            break;
          case DOUBLE:
            out.setDouble(fs.name(), dataSchema, rec.getAsDouble(i));
            break;
          case STRING:
            out.setString(fs.name(), dataSchema, rec.getAsString(i));
            break;
          case LONG:
            out.setLong(fs.name(), dataSchema, rec.getLong(i));
            break;
          default:
            throw new UnsupportedOperationException("Unhandled data type = " + dt);
          }
        }
        return out;
      } catch (HCatException e) {
        throw new RuntimeException(e);
      }
    }
  }
  
  private HCatalog() { }
}
