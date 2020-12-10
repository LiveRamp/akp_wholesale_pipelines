package com.liveramp.dataflow.akp.steps.setup;

import java.io.Serializable;
import java.util.Map;
import java.util.function.Supplier;

import com.liveramp.dataflow.common.SecretManagerProvider;
import com.liveramp.ingestion.secret.EuSecretGroups;
import com.liveramp.ingestion.secret.EuSecretNames;
import com.liveramp.ingestion.secret.SecretProvider;
import com.liveramp.translation_zone_hashing.CustomIdToArlTranslator;

import static com.liveramp.ingestion.secret.EuSecretGroups.TRANSLATOR_SECRETS;

public class ArlTranslatorSupplier implements Supplier<CustomIdToArlTranslator>, Serializable {
  private  final SecretProvider secretProvider;

  // the key name in EU Central prod instead of EuSecretNames
  // https://console.cloud.google.com/security/secret-manager?folder=&project=eu-central-prod
  private  static final String ARL_KEY_NAME = "arl-key";
  public ArlTranslatorSupplier(SecretProvider secretProvider) {
    this.secretProvider = secretProvider;
  }

  @Override
  public CustomIdToArlTranslator get() {
    String salt = secretProvider.get(ARL_KEY_NAME);
    return new CustomIdToArlTranslator(salt);
  }
}
