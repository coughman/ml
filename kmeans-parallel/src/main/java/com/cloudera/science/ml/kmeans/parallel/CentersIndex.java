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
package com.cloudera.science.ml.kmeans.parallel;

import com.cloudera.science.ml.core.vectors.Centers;
import com.cloudera.science.ml.core.vectors.Vectors;
import com.cloudera.science.ml.core.vectors.Weighted;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.io.Serializable;
import java.util.BitSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.SortedSet;

import org.apache.mahout.math.Vector;

/**
 * An internal data structure that manages the locations of the current centers during
 * k-means|| processing.
 */
class CentersIndex implements Serializable {
  private final int[] pointsPerCenter;
  private final List<List<BitSet>> indices;
  private final List<List<double[]>> points;
  private final List<List<Double>> lengthSquared;
  private final int dimensions;
  private final int projectionBits;
  private final int projectionSamples;
  private final long seed;
  
  private double[] projection;
  private boolean updated;
  
  public static class Distances {
    public double[] clusterDistances;
    public int[] closestPoints;
    
    public Distances(double[] clusterDistances, int[] closestPoints) {
      this.clusterDistances = clusterDistances;
      this.closestPoints = closestPoints;
    }
  }
  
  CentersIndex(int numClusterings, int dimensions) {
    this(numClusterings, dimensions, 128, 10, 1729L);
  }
  
  CentersIndex(int numClusterings, int dimensions, int projectionBits, int projectionSamples,
      long seed) {
    this.pointsPerCenter = new int[numClusterings];
    this.indices = Lists.newArrayList();
    this.points = Lists.newArrayList();
    this.lengthSquared = Lists.newArrayList();
    for (int i = 0; i < numClusterings; i++) {
      points.add(Lists.<double[]>newArrayList());
      lengthSquared.add(Lists.<Double>newArrayList());
    }
    this.dimensions = dimensions;
    this.projectionBits = projectionBits;
    this.projectionSamples = projectionSamples;
    this.seed = seed;
  }
  
  CentersIndex(List<Centers> centers) {
    this(centers, 128, 10, 1729L);
  }
  
  CentersIndex(List<Centers> centers, int projectionBits, int projectionSamples, long seed) {
    this(centers.size(), centers.get(0).get(0).size(), projectionBits, projectionSamples, seed);
    for (int centerId = 0; centerId < centers.size(); centerId++) {
      for (Vector v : centers.get(centerId)) {
        add(v, centerId);
      }
    }
  }
  
  public int getNumCenters() {
    return pointsPerCenter.length;
  }

  public int[] getPointsPerCluster() {
    return pointsPerCenter;
  }
  
  private void buildIndices() {
    if (projection == null) {
      Random r = new Random(seed);
      this.projection = new double[dimensions * projectionBits];
      for (int i = 0; i < projection.length; i++) {
        projection[i] = r.nextGaussian();
      }
    }
    indices.clear();
    for (List<double[]> px : points) {
      List<BitSet> indx = Lists.newArrayList();
      for (double[] aPx : px) {
        indx.add(index(Vectors.of(aPx)));
      }
      indices.add(indx);
    }
    updated = false;
  }
  
  public void add(Vector vec, int centerId) {
    points.get(centerId).add(Vectors.toArray(vec));
    lengthSquared.get(centerId).add(vec.getLengthSquared());
    pointsPerCenter[centerId]++;
    updated = true;
  }
  
  private BitSet index(Vector vec) {
    double[] prod = new double[projectionBits];
    if (vec.isDense()) {
      for (int i = 0; i < vec.size(); i++) {
        double v = vec.getQuick(i);
        if (v != 0.0) {
          for (int j = 0; j < projectionBits; j++) {
            prod[j] += v * projection[i + j * dimensions];
          }
        }
      }
    } else {
      Iterator<Vector.Element> iter = vec.iterateNonZero();
      while (iter.hasNext()) {
        Vector.Element e = iter.next();
        for (int j = 0; j < projectionBits; j++) {
          prod[j] = e.get() * projection[e.index() + j * dimensions];
        }
      }
    }
    BitSet bitset = new BitSet(projectionBits);
    for (int i = 0; i < projectionBits; i++) {
      if (prod[i] > 0.0) {
        bitset.set(i);
      }
    }
    return bitset;
  }
  
  public Distances getDistances(Vector vec, boolean approx) {
    int[] closestPoints = new int[pointsPerCenter.length];
    double[] distances = new double[pointsPerCenter.length];
    
    if (approx) {
      if (updated) {
        buildIndices();
      }
      
      BitSet q = index(vec);
      for (int i = 0; i < pointsPerCenter.length; i++) {
        List<BitSet> index = indices.get(i);
        SortedSet<Idx> lookup = Sets.newTreeSet();
        for (int j = 0; j < index.size(); j++) {
          Idx idx = new Idx(hammingDistance(q, index.get(j)), j);
          if (lookup.size() < projectionSamples) {
            lookup.add(idx);
          } else if (idx.compareTo(lookup.last()) < 0) {
            lookup.add(idx);
            lookup.remove(lookup.last());
          }
        }

        List<double[]> p = points.get(i);
        distances[i] = Double.POSITIVE_INFINITY;
        for (Idx idx : lookup) {
          double lenSq = lengthSquared.get(i).get(idx.index);
          double d = vec.getLengthSquared() + lenSq - 2 * dot(vec, p.get(idx.index));
          if (d < distances[i]) {
            distances[i] = d;
            closestPoints[i] = idx.index;
          }
        }
      }
    } else { // More expensive exact computation
      for (int i = 0; i < pointsPerCenter.length; i++) {
        distances[i] = Double.POSITIVE_INFINITY;
        List<double[]> px = points.get(i);
        List<Double> lsq = lengthSquared.get(i);
        for (int j = 0; j < px.size(); j++) {
          double[] p = px.get(j);
          double lenSq = lsq.get(j);
          double d = vec.getLengthSquared() + lenSq - 2 * dot(vec, p);
          if (d < distances[i]) {
            distances[i] = d;
            closestPoints[i] = j;
          }
        }
      }
    }
    
    return new Distances(distances, closestPoints);
  }
  
  static class Idx implements Comparable<Idx> {
    private final int distance;
    private final int index;
    
    Idx(int distance, int index) {
      this.distance = distance;
      this.index = index;
    }

    @Override
    public int compareTo(Idx idx) {
      if (distance < idx.distance) {
        return -1;
      }
      if (distance > idx.distance) {
        return 1;
      }
      return 0;
    }
    
    @Override
    public boolean equals(Object o) {
      if (!(o instanceof Idx)) {
        return false;
      }
      Idx other = (Idx) o;
      return distance == other.distance && index == other.index;
    }
    
    @Override
    public int hashCode() {
      return distance ^ index;
    }
  }
  
  private static int hammingDistance(BitSet q, BitSet idx) {
    BitSet x = new BitSet(q.size());
    x.or(q);
    x.xor(idx);
    return x.cardinality();
  }
  
  private static double dot(Vector vec, double[] p) {
    double dot = 0;
    if (vec.isDense()) {
      for (int i = 0; i < p.length; i++) {
        dot += vec.getQuick(i) * p[i];
      }
    } else {
      Iterator<Vector.Element> iter = vec.iterateNonZero();
      while (iter.hasNext()) {
        Vector.Element e = iter.next();
        dot += e.get() * p[e.index()];
      }
    }
    return dot;
  }
  
  public List<List<Weighted<Vector>>> getWeightedVectors(List<List<Long>> pointCounts) {
    List<List<Weighted<Vector>>> ret = Lists.newArrayList();
    for (int i = 0; i < pointCounts.size(); i++) {
      List<Long> counts = pointCounts.get(i);
      List<double[]> p = points.get(i);
      List<Weighted<Vector>> weighted = Lists.newArrayList();
      for (int j = 0; j < counts.size(); j++) {
        weighted.add(new Weighted<Vector>(Vectors.of(p.get(j)), counts.get(j)));
      }
      ret.add(weighted);
    }
    return ret;
  }
  
}
