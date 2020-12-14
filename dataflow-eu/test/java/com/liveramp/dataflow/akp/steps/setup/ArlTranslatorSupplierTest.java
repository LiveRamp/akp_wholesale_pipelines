package com.liveramp.dataflow.akp.steps.setup;

import com.liveramp.translation_zone_hashing.CustomIdToArlTranslator;
import org.junit.Assert;
import org.junit.Test;


public class ArlTranslatorSupplierTest {
    @Test
    public void testGetSecretProd() {
        ArlTranslatorSupplier arlTranslatorSupplier = new ArlTranslatorSupplier(true);
        CustomIdToArlTranslator customIdToArlTranslator = arlTranslatorSupplier.get();
        Assert.assertNotNull(customIdToArlTranslator);
    }
}
