package com.liveramp.dataflow.wholesale.steps;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import com.google.common.base.Splitter;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

import com.liveramp.dataflow.common.WholeSaleHelper;

public class SeparatedForKVFn extends DoFn<String, KV<String, String>> {

  @ProcessElement
  public void processElement(@Element String line, OutputReceiver<KV<String, String>> receiver) {
    List<String> separatedLine = Arrays.stream(line.split(WholeSaleHelper.KEY_VALUE_SEPARATOR, -1)).map(s -> s.trim())
        .collect(Collectors.toList());
    if (separatedLine.size() == 2) {
      receiver.output(KV.of(separatedLine.get(0), separatedLine.get(1)));
    }
  }
}