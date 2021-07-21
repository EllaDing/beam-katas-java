/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.beam.learning.katas.coretransforms.combine.combinefn;

import java.io.Serializable;
import java.util.Objects;
import org.apache.beam.learning.katas.util.Log;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;

public class Task {

  public static void main(String[] args) {
    PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
    Pipeline pipeline = Pipeline.create(options);

    PCollection<Integer> numbers = pipeline.apply(Create.of(10, 20, 50, 70, 90));

    PCollection<Double> output = applyTransform(numbers);

    output.apply(Log.ofElements());

    pipeline.run();
  }

  static PCollection<Double> applyTransform(PCollection<Integer> input) {
    return input.apply(Combine.globally(new AverageFn()));
  }

  static class AverageFn extends CombineFn<Integer, AverageFn.Accum, Double> {

    static class Accum implements Serializable {
      Integer num;
      Integer sum;
      public Accum(Integer num_, Integer sum_) {
        num = num_;
        sum = sum_;
      }
    }
    /**
     * Returns a new, mutable accumulator value, representing the accumulation of zero input values.
     */
    @Override
    public AverageFn.Accum createAccumulator() {
      return new Accum(0, 0);
    }

    /**
     * Adds the given input value to the given accumulator, returning the new accumulator value.
     *
     * @param mutableAccumulator may be modified and returned for efficiency
     * @param input              should not be mutated
     */
    @Override
    public AverageFn.Accum addInput(AverageFn.Accum mutableAccumulator, Integer input) {
      mutableAccumulator.num+=1;
      mutableAccumulator.sum+=input;
      return mutableAccumulator;
    }

    /**
     * Returns an accumulator representing the accumulation of all the input values accumulated in
     * the merging accumulators.
     *
     * @param accumulators only the first accumulator may be modified and returned for efficiency;
     *                     the other accumulators should not be mutated, because they may be shared with other code
     *                     and mutating them could lead to incorrect results or data corruption.
     */
    @Override
    public AverageFn.Accum mergeAccumulators(Iterable<AverageFn.Accum> accumulators) {
      Accum result = new Accum(0, 0);
      for (Accum acc: accumulators) {
        result.sum += acc.sum;
        result.num += acc.num;
      }
      return result;
    }

    /**
     * Returns the output value that is the result of combining all the input values represented by
     * the given accumulator.
     *
     * @param accumulator can be modified for efficiency
     */
    @Override
    public Double extractOutput(AverageFn.Accum accumulator) {
      return Double.valueOf(accumulator.sum) / Double.valueOf(accumulator.num);
    }
  }

}