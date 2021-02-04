package com.liveramp.dataflow.akp;

import com.liveramp.dataflow.common.SecretManagerProvider;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import com.liveramp.dataflow.akp.steps.BigTableInsertFlow;
import com.liveramp.dataflow.akp.steps.ParseAkpLineFn;
import com.liveramp.dataflow.akp.steps.setup.ArlTranslatorSupplier;

public class AKPIncrementalWorkflow {

  private static final SecretManagerProvider secretProvider = SecretManagerProvider.production();
  private static final ArlTranslatorSupplier arlTranslatorSupplier = new ArlTranslatorSupplier(secretProvider);

  public static void main(String[] args) {
    AkpLoadingOptions options = PipelineOptionsFactory.fromArgs(args)
        .withValidation()
        .as(AkpLoadingOptions.class);

    Pipeline pipeline = Pipeline.create(options);

    PCollection<String> lines = pipeline.apply("Read Lines", TextIO.read().from(options.getInputFile()));
    PCollection<KV<String, String>> processData = lines.apply(
        "File Filter",
        ParDo.of(new ParseAkpLineFn(options.getCidKey(), options.getPreferredPelKey(), options.getInputFile())));
    BigTableInsertFlow.insert(arlTranslatorSupplier, pipeline, processData, options);
  }
}
