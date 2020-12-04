package com.liveramp.dataflow.common;

import java.io.Serializable;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

import com.liveramp.java_support.functional.Fn;

public class MapToKVFn<InputT, Key extends Serializable, OutputT extends Serializable> extends
    DoFn<InputT, KV<Key, OutputT>> {

  private final Fn<InputT, Key> keyFn;
  private final Fn<InputT, OutputT> valueFn;

  public MapToKVFn(Fn<InputT, Key> keyFn, Fn<InputT, OutputT> valueFn) {
    this.keyFn = keyFn;
    this.valueFn = valueFn;
  }

  @ProcessElement
  public void processElement(@Element InputT input, OutputReceiver<KV<Key, OutputT>> out) {
    Key key = keyFn.apply(input);
    OutputT value = valueFn.apply(input);
    out.output(KV.of(key, value));
  }
}
