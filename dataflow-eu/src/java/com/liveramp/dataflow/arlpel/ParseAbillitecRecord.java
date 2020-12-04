package com.liveramp.dataflow.arlpel;

import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

import com.google.common.io.Files;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;

import com.liveramp.abilitec.generated.ARLToPELMapping;
import com.liveramp.abilitec.generated.Arl;
import com.liveramp.abilitec.generated.PEL;
import com.liveramp.ingestion.abilitec.ProductionEuTranslators;
import com.liveramp.translation_zone_hashing.PINToARLTranslator;
import com.rapleaf.types.new_person_data.AbiliTecId;
import com.rapleaf.types.new_person_data.HashedEmailPIN;
import com.rapleaf.types.new_person_data.PIN;

public class ParseAbillitecRecord extends DoFn<KV<String, KV<String, String>>, KV<String, String>> {

  private PINToARLTranslator pinToARLTranslator;

  private BiFunction<DoFn.ProcessContext, PCollectionView<Map<String, String>>, Function<AbiliTecId, PEL>> pelTranslatorFunction;

  private PCollectionView<Map<String, String>> secrets;

  private final Counter invalidHashesBadLength = Metrics.counter(ParseAbillitecRecord.class, "invalid hashed emails: " +
      "odd length");

  private final Counter invalidHashesBadChars = Metrics.counter(ParseAbillitecRecord.class, "invalid hashed emails: " +
      "bad characters");

  private final Counter successfullyProcessed = Metrics.counter(ParseAbillitecRecord.class, "successfully processed");

  public ParseAbillitecRecord(PINToARLTranslator pinToARLTranslator, PCollectionView<Map<String, String>> secrets) {
    this.pinToARLTranslator = pinToARLTranslator;
    this.secrets = secrets;
    this.pelTranslatorFunction = (BiFunction<DoFn.ProcessContext, PCollectionView<Map<String, String>>,
        Function<AbiliTecId, PEL>> & Serializable) (context, secretsView) -> (Function<AbiliTecId, PEL> & Serializable) (clink) ->
        ProductionEuTranslators
            .getClinkToMaintainedIndividualPelTranslator((Map<String, String>) context.sideInput(secrets)).apply(clink);
  }

  public ParseAbillitecRecord(
      PINToARLTranslator pinToARLTranslator, BiFunction<DoFn.ProcessContext,
      PCollectionView<Map<String, String>>, Function<AbiliTecId, PEL>> pelTranslatorFunction) {
    this.pinToARLTranslator = pinToARLTranslator;
    this.pelTranslatorFunction = pelTranslatorFunction;
  }

  @ProcessElement
  public void processElement(
      @Element KV<String, KV<String, String>> element,
      OutputReceiver<KV<String, String>> receiver, ProcessContext context) throws DecoderException {

    String clink = element.getValue().getKey();
    String hashedPii = element.getKey();
    String piiType = element.getValue().getValue();

    if (hashedPii.length() % 2 != 0) {
      invalidHashesBadLength.inc();
      return;
    }

    if (!hashedPii.matches("-?[0-9a-fA-F]+")) {
      invalidHashesBadChars.inc();
      return;
    }

    PEL pel = pelTranslatorFunction.apply(context, secrets).apply(new AbiliTecId(clink));

    long l = System.currentTimeMillis();

    if (piiType.equals(GenHashedEmailClinkMapping.MD5_TYPE)) {
      Arl md5Arl =
          this.pinToARLTranslator.apply(PIN.hashed_email(HashedEmailPIN.md5(Hex.decodeHex(hashedPii.toCharArray()))));
      ARLToPELMapping mapping = new ARLToPELMapping(md5Arl, pel.get_maintained_individual_pel(), l);
      receiver.output(getKVPairFromArlToPelMapping(mapping));
    } else if (piiType.equals(GenHashedEmailClinkMapping.SHA1_TYPE)) {
      Arl sha1Arl =
          this.pinToARLTranslator.apply(PIN.hashed_email(HashedEmailPIN.sha1(Hex.decodeHex(hashedPii.toCharArray()))));
      ARLToPELMapping mapping = new ARLToPELMapping(sha1Arl, pel.get_maintained_individual_pel(), l);
      receiver.output(getKVPairFromArlToPelMapping(mapping));
    } else if (piiType.equals(GenHashedEmailClinkMapping.SHA256_TYPE)) {
      Arl sha256Arl =
          this.pinToARLTranslator
              .apply(PIN.hashed_email(HashedEmailPIN.sha256(Hex.decodeHex(hashedPii.toCharArray()))));
      ARLToPELMapping mapping = new ARLToPELMapping(sha256Arl, pel.get_maintained_individual_pel(), l);
      receiver.output(getKVPairFromArlToPelMapping(mapping));
    }
    successfullyProcessed.inc();
  }

  private static KV<String, String> getKVPairFromArlToPelMapping(ARLToPELMapping mapping) {
    return KV.of(Hex.encodeHexString(mapping.get_arl().get_arl()), mapping.get_pel().get_partner_id());
  }

  private static String readFirstLine(Path filePath) throws IOException {
    return Files.readFirstLine(filePath.toFile(), Charset.forName("UTF-8"));
  }

}
