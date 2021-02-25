package com.liveramp.dataflow.arlpel;

import java.util.Arrays;

import com.google.bigtable.v2.Mutation;
import com.google.bigtable.v2.Mutation.SetCell;
import com.google.protobuf.ByteString;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;

import com.liveramp.dataflow.common.IOFunctions;
import com.liveramp.ingestion.secret.SecretProvider;

public class LoadArlPelBigtable {

  private static final String QUALIFIER = "pel";

  static class GenBigtableMutationsFn extends DoFn<KV<String, String>, KV<ByteString, Iterable<Mutation>>> {

    ValueProvider<String> columnFamily;

    GenBigtableMutationsFn(ValueProvider<String> columnFamily) {
      this.columnFamily = columnFamily;
    }

    @ProcessElement
    public void processElement(
        @Element KV<String, String> element,
        OutputReceiver<KV<ByteString, Iterable<Mutation>>> receiver) throws DecoderException {

      Mutation pelWrite = Mutation.newBuilder()
          .setSetCell(SetCell.newBuilder()
              .setTimestampMicros(0) //make sure we pick the last of any duplicates
              .setFamilyName(columnFamily.get())
              .setColumnQualifier(ByteString.copyFromUtf8(QUALIFIER))
              .setValue(ByteString.copyFromUtf8(element.getValue())).build()).build();

      receiver
          .output(KV.of(ByteString.copyFrom(Hex.decodeHex(element.getKey().toCharArray())), Arrays.asList(pelWrite)));
    }
  }

  static class GenArlPelMappingFn extends DoFn<String, KV<String, String>> {

    @ProcessElement
    public void processElement(@Element String element, OutputReceiver<KV<String, String>> receiver) {
      // Split the line into arl->pel mapping pairs.
      String[] mapping = element.split("\\|", -1);

      if (mapping.length == 2 && !mapping[0].isEmpty() && !mapping[1].isEmpty()) {
        receiver.output(KV.of(mapping[0], mapping[1]));
      }
    }
  }


  public static class BigtableMutations
      extends PTransform<PCollection<KV<String, String>>, PCollection<KV<ByteString, Iterable<Mutation>>>> {

    private ValueProvider<String> columnFamily;

    BigtableMutations(ValueProvider<String> columnFamily) {
      this.columnFamily = columnFamily;
    }

    @Override
    public PCollection<KV<ByteString, Iterable<Mutation>>> expand(PCollection<KV<String, String>> lines) {
      return lines.apply(ParDo.of(new GenBigtableMutationsFn(columnFamily)));
    }
  }

  public interface LoadArlPelOptions extends PipelineOptions {

    ValueProvider<String> getInputFile();

    void setInputFile(ValueProvider<String> value);

    ValueProvider<String> getProjectId();

    void setProjectId(ValueProvider<String> projectId);

    ValueProvider<String> getBigTableInstanceId();

    void setBigTableInstanceId(ValueProvider<String> bigtableInstanceId);

    ValueProvider<String> getBigTableTableName();

    void setBigTableTableName(ValueProvider<String> bigTableTableName);

    ValueProvider<String> getBigtableColumnFamily();

    void setBigtableColumnFamily(ValueProvider<String> bigTableColumnFamily);

  }

  static void writeArlPelToBigtable(
      PCollection<KV<String, String>> inputData,
      ValueProvider<String> tableName,
      ValueProvider<String> columnFamily,
      ValueProvider<String> projectId,
      ValueProvider<String> bigtableInstanceId) {
    inputData
        .apply(new BigtableMutations(columnFamily))
        .apply(
            BigtableIO.write()
                .withProjectId(projectId)
                .withInstanceId(bigtableInstanceId)
                .withTableId(tableName));
  }

  static PCollection<KV<String, String>> generateArlPelMapping(PCollection<String> rawInputData) {
    return rawInputData.apply(ParDo.of(new GenArlPelMappingFn()));
  }

  public static void main(String[] args) {

    String arlSecret = new SecretProvider().get("arl.key");
    System.out.print(arlSecret);

    LoadArlPelOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(LoadArlPelOptions.class);

    Pipeline p = Pipeline.create(options);

    PCollection<String> rawInputData = IOFunctions.loadInputData(p, options.getInputFile());

    PCollection<KV<String, String>> arlPelMapping = generateArlPelMapping(rawInputData);

    writeArlPelToBigtable(arlPelMapping, options.getBigTableTableName(), options.getBigtableColumnFamily(),
        options.getProjectId(), options.getBigTableInstanceId());

    p.run();
  }
}
