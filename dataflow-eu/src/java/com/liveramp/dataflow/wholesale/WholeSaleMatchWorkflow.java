package com.liveramp.dataflow.wholesale;

import com.liveramp.dataflow.wholesale.steps.ScanWholeSaleFn;
import com.liveramp.dataflow.wholesale.steps.SeparatedForKVFn;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.TextIO.Write;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

public class WholeSaleMatchWorkflow {

  public static final String WHOLESALE_HEADER = "cookie|md5|sha1|sha256";

  public static void main(String[] args) {
    WholeSaleOptions options = PipelineOptionsFactory.fromArgs(args)
        .withValidation()
        .as(WholeSaleOptions.class);

    Pipeline pipeline = Pipeline.create(options);

    PCollection<String> lines = pipeline
        .apply("Read Lines", TextIO.read().from(options.getInputFile()));

    PCollection<KV<String, String>> cookieAndPel = lines
        .apply("Get CookieId & Pel", ParDo.of(new SeparatedForKVFn()));

    PCollection<String> emailValues = cookieAndPel
        .apply("Scan For Emails", ParDo.of(new ScanWholeSaleFn(options.getDelimiter())));

    emailValues.apply("Write Output File", getWholeSaleWrite(options.getOutputFile()));

    pipeline.run();

  }

  public static Write getWholeSaleWrite(ValueProvider<String> outputPath) {
    return TextIO.write().to(outputPath)
        .withHeader(WHOLESALE_HEADER).withoutSharding();
  }


}