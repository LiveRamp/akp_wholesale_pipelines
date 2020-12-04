package com.liveramp.dataflow.common;

import java.util.Map;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;

import com.liveramp.java_support.functional.Fn;

public class MapToPutFn<InputT> extends DoFn<InputT, Mutation> {

  private final Fn<InputT, byte[]> rowKeyFn;
  private final Fn<InputT, byte[]> columnFamilyFn;
  private final Fn<InputT, Map<byte[], byte[]>> columnQualifierToValueFn;

  public MapToPutFn(
      Fn<InputT, byte[]> rowKeyFn, Fn<InputT, byte[]> columnFamilyFn,
      Fn<InputT, Map<byte[], byte[]>> columnQualifierToValueFn) {
    this.rowKeyFn = rowKeyFn;
    this.columnFamilyFn = columnFamilyFn;
    this.columnQualifierToValueFn = columnQualifierToValueFn;
  }

  @ProcessElement
  public void processElement(@Element InputT input, OutputReceiver<Mutation> out) {
    byte[] rowKey = rowKeyFn.apply(input);
    byte[] columnFamily = columnFamilyFn.apply(input);
    Map<byte[], byte[]> columnQualifierToValue = columnQualifierToValueFn.apply(input);

    if (!columnQualifierToValue.isEmpty()) {
      out.output(buildPut(rowKey, columnFamily, columnQualifierToValue));
    }
  }

  private static Put buildPut(byte[] rowKey, byte[] columnFamily, Map<byte[], byte[]> columnQualifierToValue) {
    Put put = new Put(rowKey);
    columnQualifierToValue.forEach(
        (cq, v) -> put.addColumn(columnFamily, cq, v)
    );

    return put;
  }
}
