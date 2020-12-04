package com.liveramp.dataflow.akp.steps;


import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;

import com.liveramp.dataflow.common.AKPHelper;

public class WriteToArlDiffTableFn extends DoFn<KV<String, String>, Mutation> {

  private ValueProvider<String> Ana;

  public WriteToArlDiffTableFn(ValueProvider<String> Ana) {
    this.Ana = Ana;
  }

  @ProcessElement
  public void processElement(ProcessContext c) {
    String pel = Ana + AKPHelper.BIGTABLE_SEPARATOR + c.element().getKey();
    c.output(new Put(pel.getBytes())
        .addColumn(AKPHelper.COLUMN_FAMILY.getBytes(), AKPHelper.COLUMN_QUALIFIER.getBytes(),
            c.element().getValue().getBytes()));
  }
}
