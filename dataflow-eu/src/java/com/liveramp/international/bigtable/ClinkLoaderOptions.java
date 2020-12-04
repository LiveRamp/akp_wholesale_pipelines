package com.liveramp.international.bigtable;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;

public interface ClinkLoaderOptions extends PipelineOptions {

  ValueProvider<String> getInputFile();

  void setInputFile(ValueProvider<String> value);

  ValueProvider<String> getProjectId();

  void setProjectId(ValueProvider<String> projectId);

  ValueProvider<String> getBigtableInstanceId();

  void setBigtableInstanceId(ValueProvider<String> bigtableInstanceId);

  ValueProvider<String> getClinkAsRowkeyTable();

  void setClinkAsRowkeyTable(ValueProvider<String> clinkAsRowkeyTable);

  ValueProvider<String> getClinkAsValueTable();

  void setClinkAsValueTable(ValueProvider<String> clinkAsValueTable);

  ValueProvider<String> getColumnFamily();

  void setColumnFamily(ValueProvider<String> columnFamily);

  @Default.String("|")
  ValueProvider<String> getDelimiter();

  void setDelimiter(ValueProvider<String> delimiter);
}
