package com.liveramp.international.wholesale.functions;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.opencsv.CSVParser;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.liveramp.international.wholesale.FRWholesaleLoader;
import com.liveramp.international.wholesale.models.ClinkEmailHashes;

import static com.liveramp.international.wholesale.WholesaleConstants.INPUT_FILE_DELIMITER;
import static com.liveramp.international.wholesale.WholesaleConstants.MD5_COLUMN_QUALIFIER;
import static com.liveramp.international.wholesale.WholesaleConstants.SHA1_COLUMN_QUALIFIER;
import static com.liveramp.international.wholesale.WholesaleConstants.SHA256_COLUMN_QUALIFIER;

public class ClinkToEmailHashDoFn extends DoFn<String, ClinkEmailHashes> {

  private Logger LOG = LoggerFactory.getLogger(ClinkToEmailHashDoFn.class);

  @ProcessElement
  public void processElement(@Element String line, ProcessContext c) throws Exception {
    CSVParser csvParser = new CSVParser(INPUT_FILE_DELIMITER);
    try {
      String[] parsedLine = csvParser.parseLine(line);
      if (parsedLine.length > 1) {
        Map<String, String> map = new HashMap<>();
        map.put("clink", parsedLine[0]); //assumption clink will always be at 0 index in record

        List<String> records = IntStream.range(1, parsedLine.length - 1)
            .mapToObj(value -> parsedLine[value].trim()).collect(Collectors.toList());

        for (String hash : records) {
          if (map.size() == 4) {
            break;
          }
          String hashType = checkRecordHash(hash);
          if (!map.containsKey(SHA1_COLUMN_QUALIFIER) && hashType.equals(SHA1_COLUMN_QUALIFIER)) {
            map.put(SHA1_COLUMN_QUALIFIER, hash);
            continue;
          }

          if (!map.containsKey(SHA256_COLUMN_QUALIFIER) && hashType.equals(SHA256_COLUMN_QUALIFIER)) {
            map.put(SHA256_COLUMN_QUALIFIER, hash);
            continue;
          }

          if (!map.containsKey(MD5_COLUMN_QUALIFIER) && hashType.equals(MD5_COLUMN_QUALIFIER)) {
            map.put(MD5_COLUMN_QUALIFIER, hash);
            continue;
          }
        }
        c.output(new ClinkEmailHashes(map.get("clink"), map.get(MD5_COLUMN_QUALIFIER), map.get(SHA1_COLUMN_QUALIFIER),
            map.get(SHA256_COLUMN_QUALIFIER)));
      }
    } catch (IOException e) {
      LOG.warn("Exception while parsing the record", e);
      c.output(null);
    }
  }

  private static String checkRecordHash(String hash) {
    switch (hash.length()) {
      case 64:
        return SHA256_COLUMN_QUALIFIER;
      case 40:
        return SHA1_COLUMN_QUALIFIER;
      case 32:
        return MD5_COLUMN_QUALIFIER;
    }
    return StringUtils.EMPTY;
  }
}
