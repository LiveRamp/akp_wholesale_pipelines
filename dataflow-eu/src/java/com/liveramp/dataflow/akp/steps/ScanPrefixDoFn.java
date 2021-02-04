package com.liveramp.dataflow.akp.steps;

import java.io.IOException;

import com.google.cloud.bigtable.beam.CloudBigtableTableConfiguration;
import com.google.cloud.bigtable.hbase.BigtableConfiguration;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;

public class ScanPrefixDoFn extends DoFn<String, byte[]> {

  private final CloudBigtableTableConfiguration cidTableConfiguration;
  private ValueProvider<String> ana;

  public ScanPrefixDoFn(ValueProvider<String> ana, CloudBigtableTableConfiguration cidTableConfiguration) {
    this.ana = ana;
    this.cidTableConfiguration = cidTableConfiguration;
  }

  @ProcessElement
  public void processElement(ProcessContext c) throws IOException {
    Connection connection = BigtableConfiguration.connect(cidTableConfiguration.toHBaseConfig());
    Scan scan = new Scan()
        .setRowPrefixFilter(this.ana.get().getBytes())
        .setTimeStamp(0)
        .setFilter(new KeyOnlyFilter());
    Table table = connection.getTable(TableName.valueOf(cidTableConfiguration.getTableId()));
    for (Result result : table.getScanner(scan)) {
      c.output(result.getRow());
    }
  }
}