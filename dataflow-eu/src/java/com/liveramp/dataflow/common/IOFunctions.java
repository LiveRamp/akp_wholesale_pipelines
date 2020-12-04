package com.liveramp.dataflow.common;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;

public class IOFunctions {

  public static PCollection<String> loadInputData(Pipeline pipeline, ValueProvider<String> inputFile) {
    return pipeline.apply("LoadInputData", TextIO.read().from(inputFile));
  }

  public static PCollection<String> loadInputData(Pipeline pipeline, String inputFile) {
    return pipeline.apply("LoadInputData", TextIO.read().from(inputFile));
  }

  public static PCollection<String> createPathConfig(Pipeline pipeline, ValueProvider<String> inputPath) {
    return pipeline.apply("Create Cloud Storage Config", Create.ofProvider(inputPath, StringUtf8Coder.of()));
  }

}
