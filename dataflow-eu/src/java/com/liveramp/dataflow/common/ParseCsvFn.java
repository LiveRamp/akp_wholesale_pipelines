package com.liveramp.dataflow.common;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;

import com.opencsv.CSVParser;

import com.liveramp.international.wholesale.WholesaleConstants;
import com.liveramp.java_support.functional.Fn;

import static java.util.stream.Collectors.toMap;

public class ParseCsvFn implements Fn<String, Map<Integer, String>> {

  private transient CSVParser csvParser = new CSVParser(WholesaleConstants.INPUT_FILE_DELIMITER);

  @Nullable
  @Override
  public Map<Integer, String> apply(@Nullable String s) {
    if (csvParser == null) {
      csvParser = new CSVParser(WholesaleConstants.INPUT_FILE_DELIMITER);
    }
    try {
      String[] parsedLine = csvParser.parseLine(s);
      return IntStream.range(0, parsedLine.length)
          .boxed()
          .collect(toMap(
              Fn.identity(),
              i -> parsedLine[i]
          ));
    } catch (IOException e) {
      return new HashMap<>();
    }
  }
}
