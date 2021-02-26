package com.liveramp.dataflow.arlpel;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.commons.lang.StringUtils;

import com.liveramp.abilitec.generated.PEL;
import com.liveramp.commons.Accessors;
import com.liveramp.dataflow.common.IOFunctions;
import com.liveramp.ingestion.secret.EuSecretGroups;
import com.liveramp.ingestion.secret.EuSecretNames;
import com.liveramp.translation_zone_hashing.PINToARLTranslator;
import com.liveramp.translation_zone_hashing.Translators;
import com.rapleaf.types.new_person_data.AbiliTecId;

import static com.liveramp.ingestion.secret.EuSecretGroups.TRANSLATOR_SECRETS;

public class ConvertRawToArlPel {

  public interface ConvertRawToArlPelOptions extends PipelineOptions {

    ValueProvider<String> getInputFile();

    void setInputFile(ValueProvider<String> value);

    ValueProvider<String> getOutputFile();

    void setOutputFile(ValueProvider<String> value);

    ValueProvider<Integer> getClinkIndex();

    void setClinkIndex(ValueProvider<Integer> value);

    ValueProvider<Integer> getMd5Index();

    void setMd5Index(ValueProvider<Integer> value);

    ValueProvider<Integer> getSha1Index();

    void setSha1Index(ValueProvider<Integer> value);

    ValueProvider<Integer> getSha256Index();

    void setSha256Index(ValueProvider<Integer> value);

  }

  /*
   * This class compares two Clinks as follows:
   * - it first takes the 16 character suffix from each clink
   * - does the default string compare on the 16 character suffix obtained in previous step
   * */
  private static class ClinkComparator implements Comparator<KV<String, String>>, Serializable {

    @Override
    public int compare(KV<String, String> clinkPair1, KV<String, String> clinkPair2) {
      String clink1Suffix = StringUtils.right(clinkPair1.getKey(), 16);
      String clink2Suffix = StringUtils.right(clinkPair2.getKey(), 16);
      return clink1Suffix.compareTo(clink2Suffix);
    }
  }

  private static PCollection<KV<String, KV<String, String>>> groupAndDedupClink(
      PCollection<String> rawInputData,
      int clinkIndex, int md5Index,
      int sha1Index, int sha256Index) throws IOException {
    return rawInputData
        .apply("hashed->clink", ParDo.of(new GenHashedEmailClinkMapping(clinkIndex, md5Index, sha1Index, sha256Index)))
        .apply("groupbyHashedEmail", GroupByKey.create())
        .apply("dedupClink", ParDo.of(new DoFn<KV<String, Iterable<KV<String, String>>>, KV<String, KV<String,
            String>>>() {

          @ProcessElement
          public void processElement(
              @Element KV<String, Iterable<KV<String, String>>> element,
              OutputReceiver<KV<String, KV<String, String>>> receiver) {
            List<KV<String, String>> clinks = new ArrayList<>();
            element.getValue().forEach(clinks::add);

            //Sort the clinks by the last 16 characters of each clink and pick the first one
            clinks.sort(new ClinkComparator());
            receiver.output(KV.of(element.getKey(), Accessors.first(clinks)));
          }
        }));
  }

  static PCollection<KV<String, String>> generateArlPelMapping(
      PCollection<String> rawInputData,
      ValueProvider<Integer> clinkIndex,
      ValueProvider<Integer> md5Index,
      ValueProvider<Integer> sha1Index,
      ValueProvider<Integer> sha256Index,
      PINToARLTranslator pinToArlTranslator,
      PCollectionView<Map<String, String>> secretsView) throws IOException {
    return groupAndDedupClink(
        rawInputData,
        clinkIndex.isAccessible() ? clinkIndex.get() : 0,
        md5Index.isAccessible() ? md5Index.get() : 2,
        sha1Index.isAccessible() ? sha1Index.get() : 1,
        sha256Index.isAccessible() ? sha256Index.get() : 3)
        .apply("genArlPel", ParDo.of(new ParseAbillitecRecord(pinToArlTranslator, secretsView))
            .withSideInputs(secretsView));
  }

  static PCollection<KV<String, String>> generateArlPelMapping(
      PCollection<String> rawInputData,
      ValueProvider<Integer> clinkIndex,
      ValueProvider<Integer> md5Index,
      ValueProvider<Integer> sha1Index,
      ValueProvider<Integer> sha256Index,
      PINToARLTranslator pinToArlTranslator,
      BiFunction<DoFn.ProcessContext,
          PCollectionView<Map<String, String>>,
          Function<AbiliTecId, PEL>> pelTranslatorFunction) throws IOException {
    return groupAndDedupClink(
        rawInputData,
        clinkIndex.isAccessible() ? clinkIndex.get() : 0,
        md5Index.isAccessible() ? md5Index.get() : 2,
        sha1Index.isAccessible() ? sha1Index.get() : 1,
        sha256Index.isAccessible() ? sha256Index.get() : 3)
        .apply("genArlPel", ParDo.of(new ParseAbillitecRecord(pinToArlTranslator, pelTranslatorFunction)));
  }

  static void writeArlPelMapping(PCollection<KV<String, String>> arlPelMapping, ConvertRawToArlPelOptions options) {
    arlPelMapping
        .apply(ParDo.of(new DoFn<KV<String, String>, String>() {
          @ProcessElement
          public void processElement(
              @Element KV<String, String> element,
              OutputReceiver<String> receiver) {
            receiver.output(element.getKey() + "|" + element.getValue());
          }
        }))
        .apply("writeOutput", TextIO.write().to(options.getOutputFile()).withNumShards(1));
  }

  public static void main(String[] args) throws IOException {

    ConvertRawToArlPelOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(ConvertRawToArlPelOptions.class);

    Pipeline p = Pipeline.create(options);

    PCollection<String> rawInputData = IOFunctions.loadInputData(p, options.getInputFile());

    Map<String, String> secrets = EuSecretGroups.buildSecretMap(TRANSLATOR_SECRETS);

    PCollectionView<Map<String, String>> secretsView = p.apply(Create.of(secrets)).apply(View.asMap());

    String salt = secrets.get(EuSecretNames.ARL_SECRET.getSecretName());
    PINToARLTranslator pinToARLTranslator = Translators.createPINToARLTranslator(salt);
    PCollection<KV<String, String>> arlPelMapping = generateArlPelMapping(rawInputData, options.getClinkIndex()
        , options.getMd5Index(), options.getSha1Index(), options.getSha256Index(), pinToARLTranslator, secretsView);

    writeArlPelMapping(arlPelMapping, options);

    p.run();
  }


}
