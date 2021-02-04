package com.liveramp.dataflow.akp.steps;


import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;

import com.liveramp.dataflow.common.AKPHelper;

public class WriteToArlDiffTableFn extends DoFn<KV<String, String>, Mutation> {

  private final ValueProvider<String> ana;

  public WriteToArlDiffTableFn(ValueProvider<String> ana) {
    this.ana = ana;
  }

  @ProcessElement
  public void processElement(ProcessContext c) {
    String pel = this.ana + AKPHelper.BIGTABLE_SEPARATOR + c.element().getKey();
    c.output(new Put(pel.getBytes())
        .addColumn(AKPHelper.COLUMN_FAMILY.getBytes(), AKPHelper.COLUMN_QUALIFIER.getBytes(),
            c.element().getValue().getBytes()));
  }
}
