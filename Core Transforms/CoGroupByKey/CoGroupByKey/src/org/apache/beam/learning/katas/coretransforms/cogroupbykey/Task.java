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

package org.apache.beam.learning.katas.coretransforms.cogroupbykey;

import static org.apache.beam.sdk.values.TypeDescriptors.kvs;
import static org.apache.beam.sdk.values.TypeDescriptors.strings;

import org.apache.beam.learning.katas.util.Log;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TypeDescriptors;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class Task {

  public static void main(String[] args) {
    PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
    Pipeline pipeline = Pipeline.create(options);

    PCollection<String> fruits =
        pipeline.apply("Fruits",
            Create.of("apple", "banana", "cherry")
        );

    PCollection<String> countries =
        pipeline.apply("Countries",
            Create.of("australia", "brazil", "canada")
        );

    PCollection<String> output = applyTransform(fruits, countries);

    output.apply(Log.ofElements());

    pipeline.run();
  }

  static PCollection<String> applyTransform(
      PCollection<String> fruits, PCollection<String> countries) {
    String fruit_tag = "fruits";
    String country_tag = "countries";

    PCollection<KV<String, String>> fruits_map;
    fruits_map = fruits.apply("CreateFruitMap",
            MapElements.into(kvs(strings(),
                    strings())).via((String input) -> KV.of(input.substring(0, 1), input)));
    PCollection<KV<String, String>> countries_map;
    countries_map = countries.apply("CreateCountryMap",
            MapElements.into(kvs(strings(),
                    strings())).via((String input) -> KV.of(input.substring(0, 1), input)));
    PCollection<KV<String, CoGbkResult>> keyed_elements = KeyedPCollectionTuple.of(fruit_tag,
            fruits_map).and(country_tag,
            countries_map).apply("CreateCoGroupByMap", CoGroupByKey.<String>create());
    final PCollection<String> result = keyed_elements.apply("FormatOutput", ParDo.of(new DoFn<KV<String, CoGbkResult>, String>() {
      @ProcessElement
      public void ProcessElement(ProcessContext c) {
        KV<String, CoGbkResult> e = c.element();
        Iterable<String> fruit_gbk = e.getValue().getAll(fruit_tag);
        Iterable<String> country_gbk = e.getValue().getAll(country_tag);
        Iterator<String> fruit_iterator = fruit_gbk.iterator();
        Iterator<String> country_iterator = country_gbk.iterator();
        while (fruit_iterator.hasNext()) {
          WordsAlphabet words_alphabet = new WordsAlphabet(e.getKey(), fruit_iterator.next(), country_iterator.next());
          c.output(words_alphabet.toString());
        }
      }
    }));
      return result;
  };
}