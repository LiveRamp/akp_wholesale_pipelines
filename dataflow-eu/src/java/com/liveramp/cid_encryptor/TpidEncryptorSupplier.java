package com.liveramp.cid_encryptor;

import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.function.Supplier;

import com.liveramp.ingestion.secret.EuSecretGroups;
import com.liveramp.ingestion.secret.EuSecretNames;
import com.rapleaf.spruce_lib.encryption.AcxiomPremiumPublisherIdEncryptor;
import com.rapleaf.spruce_lib.encryption.AcxiomPremiumPublisherIdEncryptorImpl;

public class TpidEncryptorSupplier implements Supplier<AcxiomPremiumPublisherIdEncryptor> {

  @Override
  public AcxiomPremiumPublisherIdEncryptor get() {
    try {
      Map<String, String> secretMap = EuSecretGroups.buildSecretMap(
          new ArrayList<>(Collections.singletonList(EuSecretNames.PREMIUM_PUBLISHER_ENCRYPTION_KEY.getSecretName())));
      return new AcxiomPremiumPublisherIdEncryptorImpl(
          secretMap.get(EuSecretNames.PREMIUM_PUBLISHER_ENCRYPTION_KEY.getSecretName()));
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException(e);
    }
  }
}