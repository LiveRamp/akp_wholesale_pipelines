package com.liveramp.dataflow.akp.steps.setup;

import java.io.Serializable;
import java.util.Map;
import java.util.function.Supplier;

import com.liveramp.ingestion.secret.EuSecretGroups;
import com.liveramp.ingestion.secret.EuSecretNames;
import com.liveramp.translation_zone_hashing.CustomIdToArlTranslator;

import static com.liveramp.ingestion.secret.EuSecretGroups.TRANSLATOR_SECRETS;

public class ArlTranslatorSupplier implements Supplier<CustomIdToArlTranslator>, Serializable {

  @Override
  public CustomIdToArlTranslator get() {
    Map<String, String> secrets = EuSecretGroups.buildSecretMap(TRANSLATOR_SECRETS);
    String salt = secrets.get(EuSecretNames.ARL_SECRET.getSecretName());
    //    String salt = "deadbeef";
    return new CustomIdToArlTranslator(salt);
  }
}
