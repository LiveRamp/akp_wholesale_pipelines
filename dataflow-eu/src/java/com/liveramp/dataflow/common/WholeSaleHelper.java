package com.liveramp.dataflow.common;

import com.google.cloud.bigtable.beam.CloudBigtableTableConfiguration;

public class WholeSaleHelper {

  public static final String KEY_VALUE_SEPARATOR = ",";
  private static final String PROJECT_ID = "international-gramercy";
  private static final String INSTANCE_ID = "com-liveramp-eu-wholesale-prod";
  private static final String WHOLE_SALE_TABLE = "uk_wholesale";
  public static final String WHOLE_SALE_COLUMN_FAMILY = "hashes";
  public static final String WHOLE_SALE_MD5 = "md5";
  public static final String WHOLE_SALE_SHA1 = "sha1";
  public static final String WHOLE_SALE_SHA256 = "sha256";
  public static final String DELIMITER = "|";
  public static CloudBigtableTableConfiguration WHOOLE_SALE_CONFIG = new CloudBigtableTableConfiguration.Builder()
      .withProjectId(PROJECT_ID)
      .withInstanceId(INSTANCE_ID)
      .withTableId(WHOLE_SALE_TABLE)
      .build();
}