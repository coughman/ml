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
package com.cloudera.science.ml.classifier.parallel;

import java.util.List;

import org.apache.crunch.Emitter;
import org.apache.crunch.Pair;

import com.cloudera.science.ml.classifier.core.OnlineLearnerRun;
import com.cloudera.science.ml.classifier.simple.SimpleOnlineLearner;
import com.cloudera.science.ml.core.vectors.LabeledVector;
import com.cloudera.science.ml.parallel.base.Pairs;

/**
 *
 */
public class SimpleFitFn extends FitFn {
  
  private List<SimpleOnlineLearner> learners;
  
  public SimpleFitFn(List<SimpleOnlineLearner> learners) {
    this.learners = learners;
  }
  
  @Override
  public void process(Pair<Pair<Integer, Integer>, Iterable<Pair<Integer, LabeledVector>>> in,
      Emitter<OnlineLearnerRun> emitter) {
    for (LabeledVector obs : Pairs.second(in.second())) {
      for (SimpleOnlineLearner learner : learners) {
        learner.update(obs);
      }
    }
    for (int i = 0; i < learners.size(); i++) {
      SimpleOnlineLearner learner = learners.get(i);
      int fold = in.first().first();
      int partition = in.first().second();
      emitter.emit(new OnlineLearnerRun(learner.getClassifier(), learner.getParams(),
          fold, partition, i));
    }
  } 

}