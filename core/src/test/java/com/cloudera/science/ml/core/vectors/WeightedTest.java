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
package com.cloudera.science.ml.core.vectors;

import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.Random;

import org.junit.Test;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

public class WeightedTest {

  private final Random rand = new Random(1729L);
  
  private static class ThingFn<T> implements Function<Weighted<T>, T> {
    @Override
    public T apply(Weighted<T> arg0) {
      return arg0.thing();
    }
  }
  
  @Test
  public void testBasic() throws Exception {
    List<Weighted<Integer>> things = Lists.newArrayList();
    for (int i = 0; i < 50; i++) {
      things.add(new Weighted<Integer>(i, rand.nextDouble()));
    }
    List<Weighted<Integer>> s = Weighted.sample(things, 5, rand);
    assertEquals(ImmutableList.of(2, 29, 21, 0, 3), Lists.transform(s, new ThingFn<Integer>()));
  }
  
  @Test
  public void testEmpty() throws Exception {
    assertEquals(0, Weighted.sample(ImmutableList.<Weighted<Long>>of(), 10, rand).size());
  }
  
  @Test
  public void testSmallerCollectionThanSize() throws Exception {
    List<Weighted<Integer>> things = Lists.newArrayList();
    for (int i = 0; i < 5; i++) {
      things.add(new Weighted<Integer>(i, rand.nextDouble()));
    }
    List<Weighted<Integer>> s = Weighted.sample(things, 10, rand);
    assertEquals(ImmutableList.of(3, 0, 1, 2, 4), Lists.transform(s, new ThingFn<Integer>()));
  }
}
