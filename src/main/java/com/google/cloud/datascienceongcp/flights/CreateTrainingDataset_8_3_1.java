/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.datascienceongcp.flights;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Mean;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A starter example for writing Beam programs.
 *
 * <p>The example takes two strings, converts them to their upper-case
 * representation and logs them.
 *
 * <p>To run this starter example locally using DirectRunner, just
 * execute it without any additional parameters from your favorite development
 * environment.
 *
 * <p>To run this starter example using managed resource in Google Cloud
 * Platform, you should specify the following command-line options:
 * --project=<YOUR_PROJECT_ID>
 * --stagingLocation=<STAGING_LOCATION_IN_CLOUD_STORAGE>
 * --runner=DataflowRunner
 */
public class CreateTrainingDataset_8_3_1 {
  private static final Logger LOG = LoggerFactory.getLogger(CreateTrainingDataset_8_3_1.class);

  public static interface MyOptions extends PipelineOptions {
    @Description("Path of the file to read from")
    @Default.String("./small.csv")
    String getInput();

    void setInput(String s);

    @Description("Path of the output to read from")
    @Default.String("./output/")
    String getOutput();

    void setOutput(String s);
  }

  public static void main(String[] args) {
    var options = PipelineOptionsFactory.fromArgs(args).withValidation()
        .as(MyOptions.class);
    var p = Pipeline.create(options);

    var flights = p
        .apply("ReadLines", TextIO.read().from(options.getInput()))
        .apply("ParseFlights", ParDo.of(new DoFn<String, Flight>() {
          @ProcessElement
          public void processElement(ProcessContext c) throws Exception {
            var line = c.element();
            var f = Flight.fromCsv(line);
            if (f != null) {
              c.output(f);
            }
          }
        }))
        .apply("GoodFlights", ParDo.of(new DoFn<Flight, Flight>() {
          @ProcessElement
          public void processElement(ProcessContext c) throws Exception {
            var f = c.element();
            if (f.isNotDiverted() && f.isNotCancelled()) {
              c.output(f);
            }
          }
        }));

    var delays = flights
        .apply("airport:hour", ParDo.of(new DoFn<Flight, KV<String, Double>>() {
          @ProcessElement
          public void processElement(ProcessContext c) {
            var f = c.element();
            if (f.getField(Flight.INPUTCORS.EVENT).equals("wheelsoff")) {
              var key = f.getField(Flight.INPUTCORS.ORIGIN) + ":" + f.getDepartureHour();
              var value = Double.valueOf(f.getFieldAsFloat(Flight.INPUTCORS.DEP_DELAY) + f.getFieldAsFloat(Flight.INPUTCORS.TAXI_OUT));
              c.output(KV.of(key, value));
            }
          }
        }))
        .apply("avgDepDelay", Mean.perKey());

    delays
        .apply("DelayToCsv", ParDo.of(new DoFn<KV<String, Double>, String>() {
          @ProcessElement
          public void processElement(ProcessContext c) throws Exception {
            var kv = c.element();
            c.output(kv.getKey() + "," + kv.getValue());
          }
        }))
        .apply("WriteDelays", TextIO.write().to(options.getOutput() + "delays4")
            .withSuffix(".csv").withoutSharding());

    p.run();
  }
}
