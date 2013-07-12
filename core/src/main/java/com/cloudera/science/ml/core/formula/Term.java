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


import java.util.Arrays;
import java.util.Iterator;
import java.util.Set;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

public class Term implements Iterable<String>, Comparable<Term> {
  
  public static final Term INTERCEPT = new Term();
  
  private final Set<String> names;
  
  public static Term $(String... names) {
    return new Term(names);
  }
  
  public Term(String... names) {
    this.names = Sets.newTreeSet(Arrays.asList(names));
    Preconditions.checkArgument(!this.names.isEmpty(), "Terms must have >= 1 named variables");
  }
  
  private Term() {
    // private constructor for the intercept term
    this.names = ImmutableSet.of();
  }
  
  public boolean isIntercept() {
    return names.isEmpty();
  }
  
  public boolean hasInteractions() {
    return names.size() > 1;
  }
  
  @Override
  public Iterator<String> iterator() {
    return names.iterator();
  }

  @Override
  public String toString() {
    if (names.isEmpty()) {
      return "Intercept";
    }
    return Joiner.on(':').join(names);
  }
  
  @Override
  public boolean equals(Object other) {
    if (!(other instanceof Term)) {
      return false;
    }
    Term t = (Term) other;
    return names.equals(t.names);
  }
  
  @Override
  public int hashCode() {
    return names.hashCode();
  }

  @Override
  public int compareTo(Term t) {
    int thisSize = names.size();
    int otherSize = t.names.size();
    if (thisSize == otherSize) {
      // Compare elements
      Iterator<String> me = names.iterator();
      Iterator<String> them = t.names.iterator();
      while (me.hasNext() && them.hasNext()) {
        int diff = me.next().compareTo(them.next());
        if (diff != 0) {
          return diff;
        }
      }
      return 0;
    }
    return thisSize < otherSize ? -1 : 1;
  }
}
