package com.liveramp.dataflow.akp;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;

import com.liveramp.dataflow.common.AKPHelper;

public interface AkpLoadingOptions extends PipelineOptions {

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
}
