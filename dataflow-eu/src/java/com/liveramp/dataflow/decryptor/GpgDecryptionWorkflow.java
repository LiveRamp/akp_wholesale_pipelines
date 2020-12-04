package com.liveramp.dataflow.decryptor;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

import com.liveramp.dataflow.common.IOFunctions;

public class GpgDecryptionWorkflow {

  public static void main(String[] args) {
    DecryptionOptions options = PipelineOptionsFactory.fromArgs(args)
        .withValidation()
        .as(DecryptionOptions.class);

    Pipeline pipeline = Pipeline.create(options);

    DecryptionFn decryption = new DecryptionFn(
        options.getMode(),
        "UTF-8");

    PCollection<String> pathConfig = IOFunctions.createPathConfig(pipeline, options.getInputPath());

    pathConfig.apply(
        "Decryption",
        ParDo.of(decryption))
        .apply("output", TextIO.write()
            .to(options.getOutputPath())
            .withoutSharding()); // write only on a single machine to get a single file output
    pipeline.run();

  }
}
