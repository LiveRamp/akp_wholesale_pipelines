package com.liveramp.dataflow.akp.steps.setup;

import com.liveramp.dataflow.common.SecretManagerProvider;
import org.junit.Test;


public class ArlTranslatorSupplierTest {
    @Test
    public void testGetSecretProd() {
        SecretManagerProvider secretProvider = SecretManagerProvider.production();
        ArlTranslatorSupplier stuff = new ArlTranslatorSupplier(secretProvider);
        stuff.get();
    }



}