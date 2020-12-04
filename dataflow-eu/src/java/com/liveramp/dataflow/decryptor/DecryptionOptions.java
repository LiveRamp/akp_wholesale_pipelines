package com.liveramp.dataflow.decryptor;

import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;

public interface DecryptionOptions extends PipelineOptions {

  ValueProvider<String> getInputPath();

  void setInputPath(ValueProvider<String> value);

  ValueProvider<String> getOutputPath();

  void setOutputPath(ValueProvider<String> value);

  ValueProvider<String> getMode();

  void setMode(ValueProvider<String> value);

  ValueProvider<String> getProjectId();

  void setProjectId(ValueProvider<String> value);

}
