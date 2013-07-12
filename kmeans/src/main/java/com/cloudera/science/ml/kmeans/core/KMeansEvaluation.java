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
package com.cloudera.science.ml.kmeans.core;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.List;

import org.apache.mahout.math.Vector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.science.ml.core.vectors.Centers;
import com.cloudera.science.ml.core.vectors.Weighted;
import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.io.Files;

/**
 * A strategy for evaluating different choices of K based on how well the points
 * in a dataset overlap between clusters that were created from different subsets
 * of the data.
 */
public class KMeansEvaluation {

  private static final Logger LOG = LoggerFactory.getLogger(KMeansEvaluation.class);
  
  private static final Joiner NEWLINE_JOINER = Joiner.on('\n');
  
  private final List<Centers> testCenters;
  private final List<Weighted<Vector>> testPoints;
  private final List<Centers> trainCenters;
  private final File detailsFile;
  
  private List<Double> predictionStrengths;
  private List<Double> trainCosts;
  private List<Double> testCosts;
  private List<Double> stablePoints;
  private List<Double> stableClusters;
  
  /**
   * Construct a new evaluation instance.
   * 
   * @param testCenters The centers of the clusters for the test sketch data
   * @param testPoints The test sketch data
   * @param trainCenters The centers of the clusters for the train sketch data
   * @param detailsFileName An optional file name to write detailed cluster statistics to
   */
  public KMeansEvaluation(List<Centers> testCenters, List<Weighted<Vector>> testPoints,
      List<Centers> trainCenters, String detailsFileName) {
    this.testCenters = testCenters;
    this.testPoints = testPoints;
    this.trainCenters = trainCenters;
    this.detailsFile = detailsFileName == null ? null : new File(detailsFileName);
    init();
  }
  
  public void writeStatsToFile(File file) throws IOException {
    PrintStream ps = new PrintStream(file);
    writeStats(ps);
    ps.close();
  }
  
  public void writeStats(PrintStream ps) {
    ps.println("ID,NumClusters,TestCost,TrainCost,PredStrength,StableClusters,StablePoints");
    for (int i = 0; i < trainCenters.size(); i++) {
      ps.println(String.format("%d,%d,%.2f,%.2f,%.4f,%.2f,%.4f",
          i, trainCenters.get(i).size(), getTestCenterCosts().get(i),
          getTrainCosts().get(i), getPredictionStrengths().get(i),
          getStableClusters().get(i), getStablePoints().get(i)));
    }
  }
  
  public List<Double> getPredictionStrengths() {
    return predictionStrengths;
  }
  
  public List<Double> getTestCenterCosts() {
    return testCosts;
  }
  
  public List<Double> getTrainCosts() {
    return trainCosts;
  }
  
  public List<Double> getStableClusters() {
    return stableClusters;
  }
  
  public List<Double> getStablePoints() {
    return stablePoints;
  }
  
  private void init() {
    predictionStrengths = Lists.newArrayListWithExpectedSize(testCenters.size());
    trainCosts = Lists.newArrayListWithExpectedSize(testCenters.size());
    testCosts = Lists.newArrayListWithExpectedSize(testCenters.size());
    stableClusters = Lists.newArrayListWithExpectedSize(testCenters.size());
    stablePoints = Lists.newArrayListWithExpectedSize(testCenters.size());
    
    for (int i = 0; i < testCenters.size(); i++) {
      Centers test = testCenters.get(i);
      Centers train = trainCenters.get(i);
      double trainCost = 0.0;
      double testCost = 0.0;
      double[][] assignments = new double[test.size()][train.size()];
      double totalPoints = 0.0;
      for (Weighted<Vector> wv : testPoints) {
        double wt = wv.weight();
        totalPoints += wt;
        Vector v = wv.thing();
        int testId = test.indexOfClosest(v);
        testCost += wt * v.getDistanceSquared(test.get(testId));
        int trainId = train.indexOfClosest(wv.thing());
        trainCost += wt * v.getDistanceSquared(train.get(trainId));
        assignments[testId][trainId] += wt;
      }
      trainCosts.add(trainCost);
      testCosts.add(testCost);
      
      double minScore = Double.POSITIVE_INFINITY;
      double points = 0;
      double clusters = 0;
      List<String> details = Lists.newArrayList();
      for (int j = 0; j < assignments.length; j++) {
        double[] assignment = assignments[j];
        double total = 0.0;
        double same = 0.0;
        for (double a : assignment) {
          total += a;
          same += a * (a - 1);
        }
        double score = total > 1 ? same / (total * (total - 1)) : 1.0;
        // Only consider clusters that contain a non-trivial number of obs
        if (total > assignment.length && score < minScore) {
          minScore = score;
        }
        if (score > 0.8) { // stability threshold
          clusters++;
          points += total;
        }
        if (detailsFile != null) {
          details.add(String.format("%d,%d,%d,%.4f", i, j,
              (int) total, score));
        }
      }
      predictionStrengths.add(minScore);
      stableClusters.add(clusters / assignments.length);
      stablePoints.add(points / totalPoints);
      if (detailsFile != null) {
        try {
          if (i == 0) {
            Files.write("ClusteringId,CenterId,NumPoints,PredictionStrength\n", detailsFile,
                Charsets.UTF_8);
          }
          Files.append(NEWLINE_JOINER.join(details) + '\n', detailsFile, Charsets.UTF_8);
        } catch (IOException e) {
          LOG.warn("Exception writing evaluation details file: {}", detailsFile, e);
        }
      }
    }
  }
}
