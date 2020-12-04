package com.liveramp.international.bigtable;

import com.google.common.collect.Lists;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

public class ClinkLoader {

  //Parse row
  static class SplitRowToKV extends DoFn<String, KV<String, String>> {

    ValueProvider<String> delimiter;

    public SplitRowToKV(ValueProvider<String> delimiter) {
      this.delimiter = delimiter;
    }

    @ProcessElement
    public void processElement(@Element String element, OutputReceiver<KV<String, String>> receiver) {
      String delimiterRegex = String.format("\\%s", delimiter);
      String[] mapping = element.split(delimiterRegex, -1);
      List<String> StringList =
          Lists.newArrayList(mapping).stream().filter(s -> !s.isEmpty()).collect(Collectors.toList());
      if (StringList.size() > 1) {
        List<String> valueList = StringList.subList(1, StringList.size());
        String key = StringList.get(0);
        valueList.forEach(v -> receiver.output(KV.of(key, v)));
      }
    }
  }


  public static void main(String[] args) {
    ClinkLoaderOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(ClinkLoaderOptions.class);
    Pipeline pipeline = Pipeline.create(options);
    long loadMicrosTimestamp = TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis());
    PCollection<KV<String, String>> clinkToValue = pipeline
        .apply(TextIO.read().from(options.getInputFile()))
        .apply(ParDo.of(new SplitRowToKV(options.getDelimiter())));

    //Write clink as rowkey, with KV value placed in column qualifier of bigtableColumnFamily
    BigtableLoaderHelper.writeKeyAsRowkey(
        clinkToValue,
        loadMicrosTimestamp,
        options.getClinkAsRowkeyTable(),
        options.getColumnFamily(),
        options.getProjectId(),
        options.getBigtableInstanceId()
    );

    //Write KV value as rowkey, with clink as value in column qualifier of bigtableColumnFamily
    BigtableLoaderHelper.writeValueAsRowkey(
        clinkToValue,
        loadMicrosTimestamp,
        options.getClinkAsValueTable(),
        options.getColumnFamily(),
        options.getProjectId(),
        options.getBigtableInstanceId()
    );

    pipeline.run();
  }
}
