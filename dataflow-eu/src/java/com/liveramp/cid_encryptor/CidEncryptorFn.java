package com.liveramp.cid_encryptor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.liveramp.abilitec.generated.PEL;
import com.liveramp.ingestion.abilitec.ProductionEuTranslators;
import com.liveramp.ingestion.secret.EuSecretGroups;
import com.rapleaf.spruce_lib.encryption.AcxiomPremiumPublisherIdEncryptor;
import com.rapleaf.types.new_person_data.AbiliTecId;

import static com.liveramp.ingestion.secret.EuSecretGroups.TRANSLATOR_SECRETS;

public class CidEncryptorFn extends DoFn<String, String> {

  private static final Logger LOG = LoggerFactory.getLogger(CidEncryptorFn.class);

  public static final String DELIMITER = ",";
  private final ValueProvider<Boolean> shouldTranslateClink;

  public CidEncryptorFn(ValueProvider<Boolean> shouldTranslateClink) {
    this.shouldTranslateClink = shouldTranslateClink;
  }

  private Function<String, String> clinkTranslator;
  private AcxiomPremiumPublisherIdEncryptor acxiomPremiumPublisherIdEncryptor;

  @Setup
  public void setup() {
    acxiomPremiumPublisherIdEncryptor = new TpidEncryptorSupplier().get();
    Function<AbiliTecId, PEL> clinkPelTranslator = ProductionEuTranslators
        .getClinkToMaintainedIndividualPelTranslator(EuSecretGroups.buildSecretMap(TRANSLATOR_SECRETS));
    if (clinkTranslator == null) {
      clinkTranslator = isClink()
          ? clink -> translateToPel(clink, clinkPelTranslator)
          : Function.identity();
      LOG.info("DELIMITER type: " + DELIMITER);
      LOG.info("Translation of CLinks? : " + isClink());
    }
  }

  @ProcessElement
  public void processElement(@Element String line, OutputReceiver<String> receiver) {
    List<String> encryptedValues = new ArrayList<>();
    // -1 is to ensure output is rectangular as csv may be sparse
    String[] splitLine = line.split(DELIMITER, -1);

    //CLink should always be in column 0  and validated if translating clinks
    if (shouldTranslateClink.get() && !validateClink(splitLine[0])) {
      LOG.error("INVALID CLINK");
      return;
    } //PEL should always be in column 0 and validated if not translating clinks
    else if (!shouldTranslateClink.get() && !validatePel(splitLine[0])) {
      LOG.error("INVALID PEL");
      return;
    }

    encryptedValues.add(clinkTranslator.apply(splitLine[0]));

    Arrays.stream(Arrays.copyOfRange(splitLine, 1, splitLine.length))
        .map(this::encryptIfNonBlank)
        .forEach(encryptedValues::add);

    receiver.output(String.join(DELIMITER, encryptedValues));
  }

  private boolean validatePel(String pel) {
    return (pel.startsWith("XY") || pel.startsWith("Xi"));
  }

  private String encryptIfNonBlank(String value) {
    if (StringUtils.isNotBlank(value)) {
      return acxiomPremiumPublisherIdEncryptor.encrypt(value);
    } else {
      return "";
    }
  }

  private boolean validateClink(String clink) {
    return clink.length() == 16 && (clink.startsWith("0000GB") || clink.startsWith("0000FR"));
  }

  private String translateToPel(String clink, Function<AbiliTecId, PEL> pelConverter) {
    return pelConverter.apply(new AbiliTecId(clink))
        .get_maintained_individual_pel()
        .get_partner_id();
  }

  private boolean isClink() {
    return shouldTranslateClink.isAccessible() && shouldTranslateClink.get();
  }
}
