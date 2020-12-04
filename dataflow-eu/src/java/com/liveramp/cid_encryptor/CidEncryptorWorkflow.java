package com.liveramp.cid_encryptor;

import com.opencsv.CSVWriter;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;

public class CidEncryptorWorkflow {

  public interface CidEncryptorOptions extends PipelineOptions {

    ValueProvider<String> getInputFile();

    void setInputFile(ValueProvider<String> value);

    ValueProvider<String> getOutputFile();

    void setOutputFile(ValueProvider<String> value);

    @Default.Boolean(true)
    @Description("If true, values in column 0 will be translated into PELs. If false, they will be passed through")
    ValueProvider<Boolean> getShouldTranslateClink();

    void setShouldTranslateClink(ValueProvider<Boolean> value);
  }

  public static void main(String[] args) {
    CidEncryptorOptions options = PipelineOptionsFactory.fromArgs(args)
        .withValidation()
        .as(CidEncryptorOptions.class);

    TextIO.Read readLinesStep = TextIO.read().from(options.getInputFile());
    ParDo.SingleOutput<String, String> pelAndEncryptStep = ParDo
        .of(new CidEncryptorFn(options.getShouldTranslateClink()));
    TextIO.Write writerStep = TextIO.write().to(options.getOutputFile())
        .withHeader("PEL,EXTERN_ID,EMAIL,LN,FN,GEN,FI,CT,ZIP,PHONE,DOBY,DOBM,DOBD,COUNTRY")
        .withNumShards(1);

    Pipeline pipeline = Pipeline.create(options);

    pipeline
        .apply("Read Lines", readLinesStep)
        .apply("Pel and Encrypt", pelAndEncryptStep)
        .apply("Write To file", writerStep);

    pipeline.run();
  }
}
