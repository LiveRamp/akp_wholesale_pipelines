package com.liveramp.dataflow.akp.steps;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;

public class GenerateMutationForArlDiffTableDoFn extends DoFn<byte[], Mutation> {

  @ProcessElement
  public void processElement(ProcessContext c) {
    Delete deletionForCID = new Delete(c.element());
    c.output(deletionForCID);
  }
}
