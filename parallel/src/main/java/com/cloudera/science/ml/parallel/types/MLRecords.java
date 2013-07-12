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
package com.cloudera.science.ml.parallel.types;

import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.crunch.MapFn;
import org.apache.crunch.types.PType;
import org.apache.crunch.types.PTypeFamily;
import org.apache.crunch.types.avro.Avros;
import org.apache.mahout.math.Vector;

import com.cloudera.science.ml.core.records.Record;
import com.cloudera.science.ml.core.records.Spec;
import com.cloudera.science.ml.core.records.avro.AvroRecord;
import com.cloudera.science.ml.core.records.avro.Spec2Schema;
import com.cloudera.science.ml.core.records.csv.CSVRecord;
import com.cloudera.science.ml.core.records.vectors.VectorRecord;
import com.cloudera.science.ml.core.vectors.Vectors;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

public final class MLRecords {

  private MLRecords() {
  }

  public static PType<Record> record(Spec spec) {
    Schema schema = Spec2Schema.create(spec);
    return Avros.derived(Record.class,
        new MapFn<GenericData.Record, Record>() {
          @Override
          public Record map(GenericData.Record gdr) {
            GenericData.Record copy = new GenericData.Record(gdr, true);
            return new AvroRecord(copy);
          }
        },
        new AvroRecordFn(schema),
        Avros.generics(schema));
  }
  
  public static PType<Record> csvRecord(PTypeFamily ptf, String delim) {
    return ptf.derived(Record.class,
        new CSV2RecordMapFn(delim),
        new Record2CSVMapFn(delim),
        ptf.strings());
  }
  
  public static PType<Record> vectorRecord(PType<Vector> ptype) {
    return vectorRecord(ptype, false);
  }
  
  public static PType<Record> vectorRecord(PType<Vector> ptype, boolean sparse) {
    return ptype.getFamily().derived(Record.class,
        new MapFn<Vector, Record>() {
          @Override
          public Record map(Vector v) {
            return new VectorRecord(v);
          }
        },
        new Record2VectorFn(sparse),
        ptype);
  }
  
  private static class AvroRecordFn extends MapFn<Record, GenericData.Record> {
    
    private final String schemaJson;
    private transient Schema schema;
    private transient List<Schema.Field> fields;
    AvroRecordFn(Schema schema) {
      this.schemaJson = schema.toString();
    }
    
    @Override
    public void initialize() {
      this.schema = (new Schema.Parser()).parse(schemaJson);
      this.fields = schema.getFields();
    }
    
    @Override
    public GenericData.Record map(Record r) {
      if (r instanceof AvroRecord) {
        return ((AvroRecord) r).getImpl();
      } else {
        GenericData.Record gdr = new GenericData.Record(schema);
        for (int i = 0; i < fields.size(); i++) {
          Schema.Type t = fields.get(i).schema().getType();
          switch (t) {
          case DOUBLE:
            gdr.put(i, r.getAsDouble(i));
            break;
          case STRING:
            gdr.put(i, r.getAsString(i));
            break;
          case INT:
            gdr.put(i, r.getInteger(i));
            break;
          case BOOLEAN:
            gdr.put(i, r.getBoolean(i));
            break;
          case LONG:
            gdr.put(i, r.getLong(i));
            break;
            default:
              throw new UnsupportedOperationException(
                  "Unsupported Avro schema type = " + t);
          }
        }
        return gdr;
      }
    }
    
  }
  
  private static class Record2VectorFn extends MapFn<Record, Vector> {
    private final boolean sparse;
    
    private Record2VectorFn(boolean sparse) {
      this.sparse = sparse;
    }
    
    @Override
    public Vector map(Record r) {
      if (r instanceof VectorRecord) {
        return ((VectorRecord) r).getVector();
      } else {
        int sz = r.getSpec().size();
        Vector v = sparse ? Vectors.sparse(sz) : Vectors.dense(sz);
        for (int i = 0; i < sz; i++) {
          v.setQuick(i, r.getAsDouble(i));
        }
        return v;
      }
    }
  }
  
  private static class CSV2RecordMapFn extends MapFn<String, Record> {

    private final String delim;
    
    private CSV2RecordMapFn(String delim) {
      this.delim = delim;
    }
    
    @Override
    public Record map(String str) {
      return new CSVRecord(str.split(delim));
    }
  }

  private static class Record2CSVMapFn extends MapFn<Record, String> {
    private final String delim;
    
    private Record2CSVMapFn(String delim) {
      this.delim = delim;
    }
    
    @Override
    public String map(Record r) {
      List<String> entries = Lists.newArrayList();
      for (int i = 0; i < r.getSpec().size(); i++) {
        entries.add(r.getAsString(i));
      }
      return Joiner.on(delim).join(entries);
    }
  }
}
