/**
 * Copyright (c) 2012, Cloudera, Inc. All Rights Reserved.
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

import static org.apache.crunch.types.avro.Avros.doubles;
import static org.apache.crunch.types.avro.Avros.ints;
import static org.apache.crunch.types.avro.Avros.pairs;
import static org.apache.crunch.types.avro.Avros.tableOf;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.crunch.Aggregator;
import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.PCollection;
import org.apache.crunch.PObject;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.fn.Aggregators;
import org.apache.crunch.materialize.pobject.PObjectImpl;
import org.apache.crunch.types.PTableType;
import org.apache.crunch.types.PType;
import org.apache.crunch.types.PTypeFamily;
import org.apache.mahout.math.NamedVector;
import org.apache.mahout.math.Vector;

import com.cloudera.science.ml.avro.MLVector;
import com.cloudera.science.ml.core.records.Record;
import com.cloudera.science.ml.core.records.RecordSpec;
import com.cloudera.science.ml.core.records.SimpleRecord;
import com.cloudera.science.ml.core.records.Spec;
import com.cloudera.science.ml.core.vectors.Centers;
import com.cloudera.science.ml.core.vectors.VectorConvert;
import com.cloudera.science.ml.core.vectors.Weighted;
import com.cloudera.science.ml.kmeans.parallel.CentersIndex.Distances;
import com.cloudera.science.ml.parallel.crossfold.Crossfold;
import com.cloudera.science.ml.parallel.fn.SumVectorsAggregator;
import com.cloudera.science.ml.parallel.pobject.ListOfListsPObject;
import com.cloudera.science.ml.parallel.pobject.ListPObject;
import com.cloudera.science.ml.parallel.records.Records;
import com.cloudera.science.ml.parallel.sample.ReservoirSampling;
import com.cloudera.science.ml.parallel.types.MLRecords;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * <p>An implementation of the k-means|| algorithm, as described in
 * <a href="http://theory.stanford.edu/~sergei/papers/vldb12-kmpar.pdf">Bahmani et al. (2012)</a>
 * 
 * <p>The main algorithm is executed by the {@link #computeClusterAssignments(PCollection, List)} 
 * method, which takes a number of
 * configured instances and runs over a given dataset of points for a fixed number
 * of iterations in order to find a candidate set of points to stream into the client and
 * cluster using the in-memory k-means algorithms defined in the {@code kmeans} package.
 */
public class KMeansParallel {

  private static final Log LOG = LogFactory.getLog(KMeansParallel.class);
  
  public static final Spec ASSIGNMENT_SPEC = RecordSpec.builder()
      .addString("vector_id")
      .addInt("cluster_id")
      .addInt("closest_center_id")
      .addDouble("distance")
      .build();
  
  private final int projectionBits;
  private final int projectionSamples;
  private final long seed;
  private final Random random;
  
  public KMeansParallel() {
    this(null, 128, 32);
  }
  
  /**
   * Main constructor that includes the option to uses a fixed {@code Random} instance
   * for running the k-means algorithm for testing purposes.
   */
  public KMeansParallel(Random random, int projectionBits, int projectionSamples) {
    this.projectionBits = projectionBits;
    this.projectionSamples = projectionSamples;
    if (random == null) {
      this.seed = System.currentTimeMillis();
    } else {
      this.seed = random.nextLong();
    }
    this.random = random;
  }
  
  /**
   * Calculates the <i>cost</i> of each of the given {@code Centers} instances on the given
   * dataset, where the cost is defined in Bahmani et al. as the sum of squared distances
   * from each point in the dataset to the {@code Centers}.
   * 
   * @param vecs The dataset
   * @param centers The candidate centers
   * @param approx Whether or not to use approximate assignments to speed up computations
   * @return A reference to the Crunch job that calculates the cost for each centers instance
   */
  public <V extends Vector> PObject<List<Double>> getCosts(PCollection<V> vecs, List<Centers> centers,
      boolean approx) {
    Preconditions.checkArgument(!centers.isEmpty(), "No centers specified");
    return getCosts(vecs, createIndex(centers), approx);
  }

  private CentersIndex createIndex(List<Centers> centers) {
    return new CentersIndex(centers, projectionBits, projectionSamples, seed);
  }
  
  private static <V extends Vector> PObject<List<Double>> getCosts(PCollection<V> vecs, CentersIndex centers,
      boolean approx) {
    return new ListPObject<Double>(vecs
        .parallelDo("center-costs", new CenterCostFn<V>(centers, approx), tableOf(ints(), doubles()))
        .groupByKey(1)
        .combineValues(Aggregators.SUM_DOUBLES()));    
  }
  
  /**
   * For each of the {@code NamedVector} instances in the given {@code PCollection}, calculate which
   * cluster in each of the {@code Centers} it is assigned (i.e., closest) to and its distance from
   * that closest center. The clusters will be identified by their position in the given list.
   * 
   * @param vecs The named vectors, with the name used as a unique identifier
   * @param centers The centers of the clusters
   * @return A {@code Records} instance containing the cluster assignment info for each point
   */
  public <V extends NamedVector> Records computeClusterAssignments(
      PCollection<V> vecs, List<Centers> centers) {
    return computeClusterAssignments(vecs, centers, null);
  }
  
  /**
   * For each of the {@code NamedVector} instances in the given {@code PCollection}, calculate which
   * cluster in each of the {@code Centers} it is assigned (i.e., closest) to and its distance from
   * that closest center. The clusters will be identified by the given list of cluster IDs.
   * 
   * @param vecs The named vectors, with the name used as a unique identifier
   * @param centers The centers of the clusters
   * @param clusterIds Integer identifiers to use for the clusters
   * @return A {@code Records} instance containing the cluster assignment info for each point
   */
  public <V extends NamedVector> Records computeClusterAssignments(
      PCollection<V> vecs, List<Centers> centers, List<Integer> clusterIds) {
    if (clusterIds != null && !clusterIds.isEmpty()) {
      Preconditions.checkArgument(centers.size() == clusterIds.size(),
          "Num centers and num clusters must be equal");
    }
    CentersIndex index = createIndex(centers);
    return new Records(vecs.parallelDo("assignments", new AssignedCenterFn<V>(index, clusterIds),
        MLRecords.record(ASSIGNMENT_SPEC)), ASSIGNMENT_SPEC);
  }
  
  /**
   * For each of the points in each of the given {@code Centers}, calculate the number of points
   * in the dataset that are closer to that point than they are to any other point in the same
   * {@code Centers} instance.
   * 
   * @param vecs The dataset
   * @param centers The collection of {@code Centers} to do the calculations on
   * @return A reference to the output file that contains the calculation for each of the centers
   */
  public <V extends Vector> PObject<List<List<Long>>> getCountsOfClosest(
      PCollection<V> vecs, List<Centers> centers) {
    Preconditions.checkArgument(!centers.isEmpty(), "No centers specified");
    Crossfold cf = new Crossfold(1); //TODO
    return getCountsOfClosest(cf.apply(vecs), createIndex(centers));
  }

  private static <V extends Vector> PObject<List<List<Long>>> getCountsOfClosest(
      PCollection<Pair<Integer, V>> vecs, CentersIndex centers) {
    return new ListOfListsPObject<Long>(
        vecs
        .parallelDo("closest-center", new ClosestCenterFn<V>(centers), pairs(ints(), ints()))
        .count(), centers.getPointsPerCluster(), 0L);
  }
  
  public <V extends Vector> List<Weighted<Vector>> initialization(
      PCollection<V> vecs, int numIterations, int samplesPerIteration,
      List<Vector> initialPoints) {
    return initialization(vecs, numIterations, samplesPerIteration, initialPoints,
        new Crossfold(1)).get(0);
  }
  
  /**
   * Performs the k-means|| initialization to generate a set of candidate {@code Weighted<Vector>}
   * instances for each of the given {@code KMPConfig} parameters from the underlying dataset. This
   * is the first stage of the execution pipeline performed by the {@code compute} method. At least five
   * iterations of the initialization procedure are recommended, but more may be warranted if the
   * {@code scaleFactor} for any of the {@code KMPConfig} params is particularly small (e.g., less than 0.5).
   */
  public <V extends Vector> List<List<Weighted<Vector>>> initialization(
      PCollection<V> vecs, int numIterations, int samplesPerIteration,
      List<Vector> initialPoints, Crossfold crossfold) {

    CentersIndex centers = new CentersIndex(crossfold.getNumFolds(),
        initialPoints.get(0).size(), projectionBits, projectionSamples,
        random == null ? System.currentTimeMillis() : random.nextLong());

    for (Vector initialPoint : initialPoints) {
      for (int j = 0; j < crossfold.getNumFolds(); j++) {
        centers.add(initialPoint, j);
      }
    }
    
    PType<V> ptype = vecs.getPType();
    PTypeFamily ptf = ptype.getFamily();
    PTableType<Integer, Pair<V, Double>> ptt = ptf.tableOf(
        ptf.ints(), ptf.pairs(ptype, ptf.doubles()));
    PCollection<Pair<Integer, V>> folds = crossfold.apply(vecs);
    for (int i = 0; i < numIterations; i++) {
      LOG.info(String.format("Running iteration %d of k-means|| initialization procedure", i + 1));
      ScoringFn<V> scoringFn = new ScoringFn<V>(centers);
      PTable<Integer, Pair<V, Double>> scores = folds.parallelDo("computeDistances", scoringFn, ptt);
      PTable<Integer, V> sample = ReservoirSampling.groupedWeightedSample(
          scores, samplesPerIteration, random);
      updateCenters(sample.materialize(), centers);
    }
    return getWeightedVectors(folds, centers);
  }
  
  /**
   * Runs Lloyd's algorithm on the given points for a given number of iterations, returning the final
   * centers that result.
   * 
   * @param points The data points to cluster
   * @param centers The list of initial centers
   * @param numIterations The number of iterations to run, with each iteration corresponding to a MapReduce job
   * @param approx Whether to use random projection for assigning points to centers
   * @return
   */
  public <V extends Vector> List<Centers> lloydsAlgorithm(PCollection<V> points, List<Centers> centers,
      int numIterations, boolean approx) {
    PTypeFamily tf = points.getTypeFamily();
    PTableType<Pair<Integer, Integer>, Pair<V, Long>> ptt = tf.tableOf(tf.pairs(tf.ints(), tf.ints()),
        tf.pairs(points.getPType(), tf.longs()));
    Aggregator<Pair<V, Long>> agg = new SumVectorsAggregator<V>();
    for (int i = 0; i < numIterations; i++) {
      CentersIndex index = createIndex(centers);
      LloydsMapFn<V> mapFn = new LloydsMapFn<V>(index, approx);
      centers = new LloydsCenters<V>(points.parallelDo("lloyds-" + i, mapFn, ptt)
          .groupByKey()
          .combineValues(agg), centers.size()).getValue();
    }
    return centers;
  }
  
  private static <V extends Vector> List<List<Weighted<Vector>>> getWeightedVectors(
      PCollection<Pair<Integer, V>> folds, CentersIndex centers) {
    LOG.info("Computing the weight of each candidate center");
    List<List<Long>> indexWeights = getCountsOfClosest(folds, centers).getValue();
    return centers.getWeightedVectors(indexWeights); 
  }
  
  private static <V extends Vector> void updateCenters(
      Iterable<Pair<Integer, V>> vecs,
      CentersIndex centers) {
    for (Pair<Integer, V> p : vecs) {
      centers.add(p.second(), p.first());
    }
  }
  
  private static class LloydsMapFn<V extends Vector> extends DoFn<V, Pair<Pair<Integer, Integer>, Pair<V, Long>>> {
    private final CentersIndex centers;
    private final boolean approx;
    
    private LloydsMapFn(CentersIndex centers, boolean approx) {
      this.centers = centers;
      this.approx = approx;
    }
    
    @Override
    public void process(V vec, Emitter<Pair<Pair<Integer, Integer>, Pair<V, Long>>> emitFn) {
      Distances d = centers.getDistances(vec, approx);
      Pair<V, Long> out = Pair.of(vec, 1L);
      for (int i = 0; i < d.closestPoints.length; i++) {
        // TODO: cache
        emitFn.emit(Pair.of(Pair.of(i, d.closestPoints[i]), out));
      }
    }
  }
  
  private static class LloydsCenters<V extends Vector> extends PObjectImpl<Pair<Pair<Integer, Integer>, Pair<V, Long>>, List<Centers>> {

    private final int numCenters;
    
    LloydsCenters(PTable<Pair<Integer, Integer>, Pair<V, Long>> collect, int numCenters) {
      super(collect);
      this.numCenters = numCenters;
    }

    @Override
    protected List<Centers> process(Iterable<Pair<Pair<Integer, Integer>, Pair<V, Long>>> values) {
      List<Centers> centers = Lists.newArrayListWithExpectedSize(numCenters);
      for (int i = 0; i < numCenters; i++) {
        centers.add(new Centers());
      }
      for (Pair<Pair<Integer, Integer>, Pair<V, Long>> p : values) {
        int centerId = p.first().first();
        Vector c = p.second().first().divide(p.second().second()); 
        centers.set(centerId, centers.get(centerId).extendWith(c));
      }
      return centers;
    }
  }
  
  private static class ScoringFn<V extends Vector> extends DoFn<Pair<Integer, V>, Pair<Integer, Pair<V, Double>>> {
    private final CentersIndex centers;
    
    private ScoringFn(CentersIndex centers) {
      this.centers = centers;
    }
    
    @Override
    public void process(Pair<Integer, V> in, Emitter<Pair<Integer, Pair<V, Double>>> emitter) {
      Distances d = centers.getDistances(in.second(), true);
      double dist = d.clusterDistances[in.first()];
      if (dist > 0.0) {
        emitter.emit(Pair.of(in.first(), Pair.of(in.second(), dist)));
      }
    }
  }
  
  private static class ClosestCenterFn<V extends Vector> extends DoFn<Pair<Integer, V>, Pair<Integer, Integer>> {
    private final CentersIndex centers;
    
    private ClosestCenterFn(CentersIndex centers) {
      this.centers = centers;
    }

    @Override
    public void process(Pair<Integer, V> in, Emitter<Pair<Integer, Integer>> emitter) {
      Distances d = centers.getDistances(in.second(), true);
      emitter.emit(Pair.of(in.first(), d.closestPoints[in.first()]));
    }
  }
  
  private static class AssignedCenterFn<V extends NamedVector> extends DoFn<V, Record> {
    private final CentersIndex centers;
    private final List<Integer> clusterIds;
    
    private AssignedCenterFn(CentersIndex centers, List<Integer> clusterIds) {
      this.centers = centers;
      this.clusterIds = clusterIds;
    }

    @Override
    public void process(V vec, Emitter<Record> emitter) {
      MLVector mlvec = VectorConvert.fromVector(vec);
      Distances d = centers.getDistances(vec, false);
      for (int i = 0; i < d.closestPoints.length; i++) {
        Record r = new SimpleRecord(ASSIGNMENT_SPEC);
        r.set("vector_id", mlvec.getId().toString())
         .set("cluster_id", getClusterId(i))
         .set("closest_center_id", d.closestPoints[i])
         .set("distance", d.clusterDistances[i]);
       emitter.emit(r);
      }
    }

    private Integer getClusterId(int index) {
      if (clusterIds == null) {
        return index;
      } else {
        return clusterIds.get(index);
      }
    }
  }
  
  private static class CenterCostFn<V extends Vector> extends DoFn<V, Pair<Integer, Double>> {
    private final CentersIndex centers;
    private final double[] currentCosts;
    private final boolean approx;
    
    private CenterCostFn(CentersIndex centers, boolean approx) {
      this.centers = centers;
      this.currentCosts = new double[centers.getNumCenters()];
      this.approx = approx;
    }
    
    @Override
    public void initialize() {
      Arrays.fill(currentCosts, 0.0);
    }
    
    @Override
    public void process(V vec, Emitter<Pair<Integer, Double>> emitter) {
      Distances d = centers.getDistances(vec, approx);
      for (int i = 0; i < currentCosts.length; i++) {
        currentCosts[i] += d.clusterDistances[i];
      }
    }
    
    @Override
    public void cleanup(Emitter<Pair<Integer, Double>> emitter) {
      for (int i = 0; i < currentCosts.length; i++) {
        emitter.emit(Pair.of(i, currentCosts[i]));
      }
    }
  }
}
