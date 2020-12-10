package com.liveramp.dataflow.akp;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;

import com.liveramp.dataflow.common.AKPHelper;

public interface AkpLoadingOptions extends DataflowPipelineOptions {

  @Validation.Required
  @Description("GCS path of audience key publishing input data")
  ValueProvider<String> getInputFile();

  void setInputFile(ValueProvider<String> value);

  @Validation.Required
  @Description("Adnetwork account to load data")
  ValueProvider<String> getAnaId();

  void setAnaId(ValueProvider<String> value);

  @Validation.Required
  @Description("CID header key")
  ValueProvider<String> getCidKey();

  void setCidKey(ValueProvider<String> value);

  @Description("Optional PEL header to prefer")
  @Default.String(AKPHelper.PREFERRED_PEL_KEY)
  ValueProvider<String> getPreferredPelKey();

  void setPreferredPelKey(ValueProvider<String> value);

  @Validation.Required
  @Description("BigTable instance where data will be loaded into table")
  @Default.String(AKPHelper.INSTANCE_ID)
  ValueProvider<String> getBigtableInstance();

  void setBigtableInstance(ValueProvider<String> value);

  @Validation.Required
  @Description("Table name for arl_diff")
  @Default.String(AKPHelper.ARL_DIFF_TABLE)
  ValueProvider<String> getArlDiffTable();

  void setArlDiffTable(ValueProvider<String> value);

  @Validation.Required
  @Description("Table name for arl_pel")
  @Default.String(AKPHelper.ARL_PEL_TABLE)
  ValueProvider<String> getArlPelTable();

  void setArlPelTable(ValueProvider<String> value);


  @Validation.Required
  @Description("Project name to run job")
  @Default.String(AKPHelper.PROJECT_ID)
  ValueProvider<String> getProjectId();

  void setProjectId(ValueProvider<String> value);
}
