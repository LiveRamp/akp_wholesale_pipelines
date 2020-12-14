package com.liveramp.dataflow.akp.steps.setup;

import java.io.Serializable;
import java.util.function.Supplier;

import com.liveramp.dataflow.common.SecretManagerProvider;
import com.liveramp.translation_zone_hashing.CustomIdToArlTranslator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ArlTranslatorSupplier implements Supplier<CustomIdToArlTranslator>, Serializable {
    private static SecretManagerProvider secretProvider;
    private static boolean test;

    private Logger LOG = LoggerFactory.getLogger(ArlTranslatorSupplier.class);


    // the key name in EU Central prod instead of EuSecretNames
    // https://console.cloud.google.com/security/secret-manager?folder=&project=eu-central-prod
    private static final String ARL_KEY_NAME = "projects/467137229199/secrets/arl-key/versions/1";
    private static final String TEST_SECRET_VAL = "ABCD";

    public ArlTranslatorSupplier(SecretManagerProvider secretProvider) {
        this.secretProvider = secretProvider;
    }

    public ArlTranslatorSupplier(boolean test) {
        this.test = test;
    }

    @Override
    public CustomIdToArlTranslator get() {
        if (!test){
            String arlSecretVal = secretProvider.get(ARL_KEY_NAME);
            if (arlSecretVal != null && arlSecretVal.length() > 0) {
                LOG.info("Succeded to obtain arlSecretVal. Length:{}", arlSecretVal.length());
            } else {
                LOG.error("Failed to obtain arlSecretVal!!!!");
            }
            return new CustomIdToArlTranslator(arlSecretVal);
        }else{
            LOG.info("In test mode. Returning dummy CustomIdToArlTranslator with " +
                    "dummy secret:{}",TEST_SECRET_VAL );
            return new CustomIdToArlTranslator(TEST_SECRET_VAL);
        }

    }
}
