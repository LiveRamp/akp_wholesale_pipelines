package com.liveramp.dataflow.akp.steps.setup;

import java.io.Serializable;
import java.util.function.Supplier;

import com.liveramp.dataflow.common.SecretManagerProvider;
import com.liveramp.translation_zone_hashing.CustomIdToArlTranslator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ArlTranslatorSupplier implements Supplier<CustomIdToArlTranslator>, Serializable {
  private  final SecretManagerProvider secretProvider;
  private Logger LOG = LoggerFactory.getLogger(ArlTranslatorSupplier.class);


  // the key name in EU Central prod instead of EuSecretNames
  // https://console.cloud.google.com/security/secret-manager?folder=&project=eu-central-prod
  private  static final String ARL_KEY_NAME = "projects/467137229199/secrets/arl-key/versions/1";
  public ArlTranslatorSupplier(SecretManagerProvider secretProvider) {
    this.secretProvider = secretProvider;
  }

  @Override
  public CustomIdToArlTranslator get() {
    String arlSecretVal = secretProvider.get(ARL_KEY_NAME);
    if (arlSecretVal != null && arlSecretVal.length() > 0){
      LOG.info("Succeded to obtain arlSecretVal. Length:{}", arlSecretVal.length());
    }else{
      LOG.error("Failed to obtain arlSecretVal!!!!");
    }
    return new CustomIdToArlTranslator(arlSecretVal);
  }
}
