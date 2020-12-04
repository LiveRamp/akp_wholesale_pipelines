package com.liveramp.international.wholesale;

import java.util.Arrays;
import java.util.Map;

import com.google.bigtable.v2.Mutation;
import com.google.protobuf.ByteString;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.extensions.joinlibrary.Join;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.codec.DecoderException;

import com.liveramp.dataflow.common.MapToKVFn;
import com.liveramp.dataflow.common.ParseToColumnMapFn;
import com.liveramp.ingestion.abilitec.ProductionEuTranslators;
import com.liveramp.ingestion.secret.EuSecretGroups;
import com.rapleaf.types.new_person_data.AbiliTecId;


public class EuWholesaleMatchTableLoader {

  private static PCollection<KV<String, String>> convertSha256BytesToHex(
      ValueProvider<String> sha256InputPath,
      ValueProvider<Integer> sha256Index,
      Pipeline p) {

    PCollection<KV<String, String>> sha256BytesToHex = p.apply(TextIO.read()
        .from(sha256InputPath))

        //parse to column map and drop empty
        .apply(ParDo.of(
            ParseToColumnMapFn.csv(sha256Index)))

        .apply(ParDo.of(
            new MapToKVFn<>(
                columnMap -> columnMap.get(sha256Index.get()),
                columnMap -> columnMap.get(sha256Index.get()))
        ));

    return sha256BytesToHex;
  }

  private static PCollection<KV<String, PelAndEmailHashes>> mapSha256ToClinkAndHashedEmails(
      ValueProvider<String> rawEmailToPelInputPath,
      ValueProvider<Integer> clinkIndex,
      ValueProvider<Integer> md5EmailColumnIndex,
      ValueProvider<Integer> sha1EmailColumnIndex,
      ValueProvider<Integer> sha256EmailColumnIndex,
      Map<String, String> secrets, Pipeline p) {

    PCollection<KV<String, PelAndEmailHashes>> sha256ToPelAndHashes = p
        .apply(TextIO.read().from(rawEmailToPelInputPath))

        .apply(ParDo.of(
            ParseToColumnMapFn.csv(
                sha256EmailColumnIndex,
                clinkIndex)))

        .apply(ParDo.of(
            new MapToKVFn<>(
                columnMap -> (columnMap.get(sha256EmailColumnIndex.get())),
                columnMap -> new PelAndEmailHashes(
                    ProductionEuTranslators.getClinkToMaintainedIndividualPelTranslator(secrets)
                        .apply(new AbiliTecId(columnMap.get(clinkIndex.get()))).get_maintained_individual_pel()
                        .get_partner_id(),
                    columnMap.get(md5EmailColumnIndex.get()),
                    columnMap.get(sha1EmailColumnIndex.get()),
                    columnMap.get(sha256EmailColumnIndex.get())
                )
            )));

    return sha256ToPelAndHashes;
  }

  static Mutation MutationGenerator(String columnQualifier, String columnValue, String columnFamily) {

    return Mutation.newBuilder()
        .setSetCell(Mutation.SetCell.newBuilder()
            .setTimestampMicros(0)
            .setFamilyName(columnFamily)
            .setColumnQualifier(ByteString.copyFromUtf8(columnQualifier))
            .setValue(ByteString.copyFromUtf8(columnValue)).build()).build();
  }

  static class GenBigtableMutationsFn extends
      DoFn<KV<String, KV<String, PelAndEmailHashes>>, KV<ByteString, Iterable<Mutation>>> {

    String columnFamily;

    GenBigtableMutationsFn(String columnFamily) {
      this.columnFamily = columnFamily;
    }

    @ProcessElement
    public void processElement(
        @Element KV<String, KV<String, PelAndEmailHashes>> element,
        OutputReceiver<KV<ByteString, Iterable<Mutation>>> receiver) throws DecoderException {

      Mutation md5hashedEmailWrite = MutationGenerator(
          WholesaleConstants.MD5_COLUMN_QUALIFIER, element.getValue().getValue().md5, columnFamily);
      Mutation sha1hashedEmailWrite = MutationGenerator(
          WholesaleConstants.SHA1_COLUMN_QUALIFIER, element.getValue().getValue().sha1, columnFamily);
      Mutation sha256hashedEmailWrite = MutationGenerator(
          WholesaleConstants.SHA256_COLUMN_QUALIFIER, element.getValue().getValue().sha256, columnFamily);

      receiver.output(
          KV.of(ByteString.copyFromUtf8(element.getValue().getValue().pel), Arrays.asList(md5hashedEmailWrite)));
      receiver.output(
          KV.of(ByteString.copyFromUtf8(element.getValue().getValue().pel), Arrays.asList(sha1hashedEmailWrite)));
      receiver.output(
          KV.of(ByteString.copyFromUtf8(element.getValue().getValue().pel), Arrays.asList(sha256hashedEmailWrite)));
    }
  }

  public static class BigtableMutations
      extends
      PTransform<PCollection<KV<String, KV<String, PelAndEmailHashes>>>, PCollection<KV<ByteString, Iterable<Mutation>>>> {

    private String columnFamily;

    BigtableMutations(String columnFamily) {
      this.columnFamily = columnFamily;
    }

    @Override
    public PCollection<KV<ByteString, Iterable<Mutation>>> expand(
        PCollection<KV<String, KV<String, PelAndEmailHashes>>> lines) {
      return lines.apply(ParDo.of(new EuWholesaleMatchTableLoader.GenBigtableMutationsFn((columnFamily))));
    }
  }


  private static void writeToBigTable(
      String Md5ColumnQualifier, String Sha1ColumnQualifier, String Sha256ColumnQualifier, String columnFamily,
      PCollection<KV<String, KV<String, PelAndEmailHashes>>> joined) {

    joined
        .apply(new BigtableMutations(columnFamily))
        .apply(
            BigtableIO.write()
                .withProjectId(WholesaleConstants.PROJECT_ID)
                .withInstanceId(WholesaleConstants.INSTANCE_ID)
                .withTableId(WholesaleConstants.TABLE_ID));
  }

  public static void main(String[] args) {
    EuWholesaleMatchTableLoaderOptions options = PipelineOptionsFactory.fromArgs(args)
        .withValidation()
        .as(EuWholesaleMatchTableLoaderOptions.class);
    options.setRunner(DataflowRunner.class);
    Pipeline pipeline = Pipeline.create(options);

    // Experian Email data
    ValueProvider<String> sha256FileInputPath = options.getSha256Inputpath();
    ValueProvider<Integer> sha256Index = options.getSha256ColumnIndex();

    // Knowledge Base data
    ValueProvider<String> clinkToHashEmailInputPath = options.getRawEmailToPelInputpath();
    ValueProvider<Integer> md5EmailColumnIndex = options.getMd5EmailColumnIndex();
    ValueProvider<Integer> sha1EmailColumnIndex = options.getSha1EmailColumnIndex();
    ValueProvider<Integer> sha256EmailColumnIndex = options.getSha256EmailColumnIndex();
    ValueProvider<Integer> clinkIndex = options.getPelColumnIndex();

    // BigTable schema
    String columnFamily = options.getColumnFamily();
    String sha256ColumnQualifier = options.getSha256ColumnQualifier();
    String sha1ColumnQualifier = options.getSha1ColumnQualifier();
    String md5ColumnQualifier = options.getMd5ColumnQualifier();

    Map<String, String> secrets = EuSecretGroups.buildSecretMap(EuSecretGroups.TRANSLATOR_SECRETS);

    PCollection<KV<String, String>> sha256HashesFromEmailProvider = convertSha256BytesToHex(
        sha256FileInputPath, sha256Index, pipeline)
        .setCoder(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()));

    PCollection<KV<String, PelAndEmailHashes>> kbIndexedBySha256Hash = mapSha256ToClinkAndHashedEmails(
        clinkToHashEmailInputPath, clinkIndex,
        md5EmailColumnIndex, sha1EmailColumnIndex, sha256EmailColumnIndex, secrets, pipeline)
        .setCoder(KvCoder.of(StringUtf8Coder.of(), SerializableCoder.of(PelAndEmailHashes.class)));

    PCollection<KV<String, KV<String, PelAndEmailHashes>>> joined = Join
        .innerJoin(sha256HashesFromEmailProvider, kbIndexedBySha256Hash);

    writeToBigTable(md5ColumnQualifier, sha1ColumnQualifier, sha256ColumnQualifier, columnFamily, joined);

    pipeline.run();
  }
}
