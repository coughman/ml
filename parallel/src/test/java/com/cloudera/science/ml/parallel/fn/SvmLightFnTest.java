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
package com.cloudera.science.ml.parallel.fn;

import static org.junit.Assert.assertEquals;

import org.apache.mahout.math.NamedVector;
import org.apache.mahout.math.Vector;
import org.junit.Test;

import com.cloudera.science.ml.core.vectors.Vectors;

public class SvmLightFnTest {

  private final SvmLightFn<Vector> fn = new SvmLightFn<Vector>();
  
  @Test
  public void testVector() throws Exception {
    Vector v = Vectors.of(1.0, 2.0, 3.0);
    assertEquals("0:1.0 1:2.0 2:3.0", fn.map(v));
    
    v = Vectors.sparse(10);
    v.set(3, 7.2);
    v.set(6, 12.0);
    assertEquals("3:7.2 6:12.0", fn.map(v));
  }
  
  @Test
  public void testNamedVector() throws Exception {
    Vector v = Vectors.named("foo", 1.0, 2.0, 3.0);
    assertEquals("foo 0:1.0 1:2.0 2:3.0", fn.map(v));
    
    v = Vectors.sparse(10);
    v.set(3, 7.2);
    v.set(6, 12.0);
    v = new NamedVector(v, "bar");
    assertEquals("bar 3:7.2 6:12.0", fn.map(v));
  }
}
