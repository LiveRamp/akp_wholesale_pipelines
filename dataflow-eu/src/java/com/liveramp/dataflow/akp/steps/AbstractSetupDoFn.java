package com.liveramp.dataflow.akp.steps;

import java.util.Map;

import org.apache.beam.sdk.transforms.DoFn;

import com.liveramp.ingestion.secret.EuSecretGroups;
import com.liveramp.ingestion.secret.EuSecretNames;
import com.liveramp.translation_zone_hashing.CustomIdToArlTranslator;

import static com.liveramp.ingestion.secret.EuSecretGroups.TRANSLATOR_SECRETS;

public abstract class AbstractSetupDoFn<I, O> extends DoFn<I, O> {

  private CustomIdToArlTranslator customIdToArlTranslator;

  @Setup
  public void setup() {
    Map<String, String> secrets = EuSecretGroups.buildSecretMap(TRANSLATOR_SECRETS);
    String salt = secrets.get(EuSecretNames.ARL_SECRET.getSecretName());
    //    String salt = "deadbeef";
    customIdToArlTranslator = new CustomIdToArlTranslator(salt);
    postSetup();
  }

  public abstract void postSetup();

  public CustomIdToArlTranslator getCustomIdToArlTranslator() {
    return customIdToArlTranslator;
  }

}
