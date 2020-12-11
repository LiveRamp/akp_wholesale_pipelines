package com.liveramp.dataflow.akp.steps.setup;

import com.liveramp.dataflow.common.SecretManagerProvider;
import com.liveramp.translation_zone_hashing.CustomIdToArlTranslator;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;


public class ArlTranslatorSupplierTest {
    @Test
    @Ignore
    //  Note: This succeeds locally where one has credential to access seccret mentioned.
    // Will not work on Jenkins since it does not have perm to secret
    public void testGetSecretProd() {
        SecretManagerProvider secretProvider = SecretManagerProvider.production();
        ArlTranslatorSupplier arlTranslatorSupplier = new ArlTranslatorSupplier(secretProvider);
        CustomIdToArlTranslator customIdToArlTranslator = arlTranslatorSupplier.get();
        Assert.assertNotNull(customIdToArlTranslator);
    }
}
