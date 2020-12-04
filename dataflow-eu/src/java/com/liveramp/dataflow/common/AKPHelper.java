package com.liveramp.dataflow.common;

import com.google.cloud.bigtable.beam.CloudBigtableTableConfiguration;

public class AKPHelper {

  public static final String KEY_VALUE_SEPARATOR = "=";
  public static final String BIGTABLE_SEPARATOR = "#";
  public static final String PREFERRED_PEL_KEY = "LRI_DER_LRI_SHA256_EMAIL1";

  private static final String INSTANCE_ID = "pixel-serving";
  private static final String PROJECT_ID = "eu-central-prod";
  public static final String COLUMN_FAMILY = "pel";
  public static final String COLUMN_QUALIFIER = "pel";
  private static final String ARL_DIFF_TABLE = "arl_diff";
  private static final String ARL_PEL_TABLE = "arl_to_pel";
  public static CloudBigtableTableConfiguration arlDiffConfig = new CloudBigtableTableConfiguration.Builder()
      .withProjectId(PROJECT_ID)
      .withInstanceId(INSTANCE_ID)
      .withTableId(ARL_DIFF_TABLE)
      .build();
  public static CloudBigtableTableConfiguration arlPelConfig = new CloudBigtableTableConfiguration.Builder()
      .withProjectId(PROJECT_ID)
      .withInstanceId(INSTANCE_ID)
      .withTableId(ARL_PEL_TABLE)
      .build();
}