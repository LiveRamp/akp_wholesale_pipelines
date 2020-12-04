package com.liveramp.dataflow.wholesale;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;

import com.liveramp.dataflow.common.WholeSaleHelper;

public interface WholeSaleOptions extends PipelineOptions {

  ValueProvider<String> getInputFile();

  void setInputFile(ValueProvider<String> value);

  ValueProvider<String> getOutputFile();

  void setOutputFile(ValueProvider<String> value);

  ValueProvider<String> getProjectId();

  void setProjectId(ValueProvider<String> value);

  @Default.String(WholeSaleHelper.DELIMITER)
  ValueProvider<String> getDelimiter();

  void setDelimiter(ValueProvider<String> value);

}
