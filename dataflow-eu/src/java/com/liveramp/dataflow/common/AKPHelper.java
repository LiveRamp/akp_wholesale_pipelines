package com.liveramp.dataflow.common;

import com.google.cloud.bigtable.beam.CloudBigtableTableConfiguration;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

import com.liveramp.dataflow.akp.AkpLoadingOptions;

public class AKPHelper {

  public static final String KEY_VALUE_SEPARATOR = "=";
  public static final String BIGTABLE_SEPARATOR = "#";
  public static final String PREFERRED_PEL_KEY = "LRI_DER_LRI_SHA256_EMAIL1";

  public static final String INSTANCE_ID = "pixel-serving";
  public static final String PROJECT_ID = "eu-central-prod";
  public static final String COLUMN_FAMILY = "pel";
  public static final String COLUMN_QUALIFIER = "pel";
  public static final String ARL_DIFF_TABLE = "arl_diff";
  public static final String ARL_PEL_TABLE = "arl_to_pel";

  public static CloudBigtableTableConfiguration getArlPelBigtableConfig(AkpLoadingOptions akpLoadingOptions) {
    GcpOptions gcpOptions = PipelineOptionsFactory.as(GcpOptions.class);
    return new CloudBigtableTableConfiguration.Builder()
        .withProjectId(akpLoadingOptions.getProjectId())
        .withInstanceId(akpLoadingOptions.getBigtableInstance())
        .withTableId(akpLoadingOptions.getArlPelTable())
        .build();
  }

  public static CloudBigtableTableConfiguration getArlDiffBigtableConfig(AkpLoadingOptions akpLoadingOptions) {
    GcpOptions gcpOptions = PipelineOptionsFactory.as(GcpOptions.class);
    return new CloudBigtableTableConfiguration.Builder()
        .withProjectId(akpLoadingOptions.getProjectId())
        .withInstanceId(akpLoadingOptions.getBigtableInstance())
        .withTableId(akpLoadingOptions.getArlDiffTable())
        .build();
  }
}
