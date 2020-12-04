package com.liveramp.dataflow.common;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.commons.lang3.StringUtils;

import com.liveramp.java_support.functional.Fn;

public class ParseToColumnMapFn extends DoFn<String, Map<Integer, String>> {

  private final Fn<String, Map<Integer, String>> parseFn;
  private List<Integer> requiredColumns;
  private ValueProvider<Integer>[] indices;

  public ParseToColumnMapFn(Fn<String, Map<Integer, String>> parseFn, ValueProvider<Integer>... indices) {
    this.parseFn = parseFn;
    this.indices = indices;
  }

  public ParseToColumnMapFn(Fn<String, Map<Integer, String>> parseFn) {
    this.parseFn = parseFn;
    this.requiredColumns = new ArrayList<>();
  }

  @ProcessElement
  public void processElement(@Element String input, OutputReceiver<Map<Integer, String>> out) {
    if (requiredColumns == null) {
      requiredColumns = Arrays.stream(indices).map(index -> index.get()).collect(Collectors.toList());
    }
    Map<Integer, String> columnIndexToValue = parseFn.apply(input);
    for (int requiredColumn : requiredColumns) {
      if (StringUtils.isBlank(columnIndexToValue.getOrDefault(requiredColumn, ""))) {
        return;
      }
    }

    out.output(columnIndexToValue);
  }

  public static ParseToColumnMapFn csv(ValueProvider<Integer>... indices) {
    return new ParseToColumnMapFn(new ParseCsvFn(), indices);
  }
}
