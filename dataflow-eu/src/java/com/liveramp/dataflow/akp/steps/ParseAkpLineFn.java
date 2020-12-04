package com.liveramp.dataflow.akp.steps;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import com.google.common.base.Splitter;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

import com.liveramp.dataflow.common.AKPHelper;
import com.liveramp.file_decompression_service.generated.DecompressionType;
import com.liveramp.ingestion.config.EuFileConfigurationUtils;

public class ParseAkpLineFn extends DoFn<String, KV<String, String>> {

  private ValueProvider<String> cidKey;
  private ValueProvider<String> preferredPelKey;
  private ValueProvider<String> filePath;

  private Splitter.MapSplitter keyValueSplitter;

  public ParseAkpLineFn(
      ValueProvider<String> cidKey, ValueProvider<String> preferredPelKey, ValueProvider<String> filePath) {
    this.cidKey = cidKey;
    this.preferredPelKey = preferredPelKey;
    this.filePath = filePath;
  }

  @Setup
  public void setup() {
    String inferredDelimiter = EuFileConfigurationUtils.inferDelimiter(filePath.get(), DecompressionType.NONE);
    keyValueSplitter = Splitter.on(inferredDelimiter)
        .withKeyValueSeparator(AKPHelper.KEY_VALUE_SEPARATOR);
  }

  @ProcessElement
  public void processElement(@Element String line, OutputReceiver<KV<String, String>> receiver) {
    line = line.trim();
    if (!line.isEmpty()) {
      Map<String, String> lineMap = keyValueSplitter.split(line);
      getCidToPel(lineMap, cidKey.get(), preferredPelKey.get())
          .ifPresent(receiver::output);
    }
  }

  private static Optional<KV<String, String>> getCidToPel(
      Map<String, String> lineKeyValuePairs, String cidKey, String preferredPelKey) {
    String cidValue = removeQuotes(
        lineKeyValuePairs.getOrDefault(cidKey, "")
    );

    String pelValue = "";
    if (lineKeyValuePairs.containsKey(preferredPelKey)) {
      pelValue = removeQuotes(lineKeyValuePairs.get(preferredPelKey));
    } else {
      List<String> sortedPels = sortPels(lineKeyValuePairs);
      if (!sortedPels.isEmpty()) {
        pelValue = sortedPels.get(0);
      }
    }

    return (!cidValue.isEmpty() && !pelValue.isEmpty())
        ? Optional.of(KV.of(cidValue, pelValue))
        : Optional.empty();
  }

  private static List<String> sortPels(Map<String, String> lineKeyValuePairs) {
    return lineKeyValuePairs.values().stream()
        .map(ParseAkpLineFn::removeQuotes)
        .filter(str -> str.startsWith("XY") || str.startsWith("Xi"))
        .sorted(Comparator.reverseOrder())
        .collect(Collectors.toList());
  }

  private static String removeQuotes(String str) {
    return str.replaceAll("^\"|\"$", "");
  }
}

