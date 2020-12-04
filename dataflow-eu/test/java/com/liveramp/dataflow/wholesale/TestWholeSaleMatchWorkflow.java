package com.liveramp.dataflow.wholesale;

import static com.liveramp.dataflow.wholesale.WholeSaleMatchWorkflow.getWholeSaleWrite;

import java.io.Serializable;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;

public class TestWholeSaleMatchWorkflow implements Serializable {

  @Rule
  public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void
  testWrite() {

    ValueProvider<String> outputPath = ValueProvider.StaticValueProvider.of("./hereIsnEWfILE");
    PCollection<String> aLine = pipeline.apply("London is beautiful", Create.of("delimiter"));
    aLine.apply("Write Output File", getWholeSaleWrite(outputPath));
    // Todo: how to test written file? or is it worth to have unit test if integration test result makes sense..,
  }
}
