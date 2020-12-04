package com.liveramp.dataflow.arlpel;

import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

public class GenHashedEmailClinkMapping extends DoFn<String, KV<String, KV<String, String>>> {

  public static final String MD5_TYPE = "MD5";
  public static final String SHA1_TYPE = "SHA1";
  public static final String SHA256_TYPE = "SHA256";

  private int clinkIndex;
  private int md5Index;
  private int sha1Index;
  private int sha256Index;

  private final Counter invalidRecord = Metrics.counter(GenHashedEmailClinkMapping.class, "invalid record: config " +
      "index greater than number of cols");

  public GenHashedEmailClinkMapping(int clinkIndex, int md5Index, int sha1Index, int sha256Index) {
    StringBuilder validationMsg = new StringBuilder();

    if (!validateIndex(clinkIndex)) {
      validationMsg.append("Invalid index " + clinkIndex + " for clink\n");
    }
    if (!validateIndex(md5Index)) {
      validationMsg.append("Invalid index " + md5Index + " for MD5\n");
    }
    if (!validateIndex(sha1Index)) {
      validationMsg.append("Invalid index " + sha1Index + " for SHA1\n");
    }
    if (!validateIndex(sha256Index)) {
      validationMsg.append("Invalid index " + sha256Index + " for SHA256\n");
    }

    if (validationMsg.length() > 0) {
      throw new IllegalArgumentException(validationMsg.toString());
    } else {
      this.clinkIndex = clinkIndex;
      this.md5Index = md5Index;
      this.sha1Index = sha1Index;
      this.sha256Index = sha256Index;
    }
  }

  private boolean validateIndex(int index) {
    return index >= 0 ? true : false;
  }

  @ProcessElement
  public void processElement(@Element String element, OutputReceiver<KV<String, KV<String, String>>> receiver) {
    String[] split = element.split("\\|");
    if (clinkIndex >= split.length
        || sha256Index >= split.length
        || sha1Index >= split.length
        || md5Index >= split.length) {
      invalidRecord.inc();
      return; //skip record if any index greater than number of cols in input record
    }
    String clink = split[0];
    String sha1 = split[1];
    String md5 = split[2];
    String sha256 = split[3];

    if (clink != null) {

      if (md5 != null) {
        receiver.output(KV.of(md5, KV.of(clink, MD5_TYPE)));
      }

      if (sha1 != null) {
        receiver.output(KV.of(sha1, KV.of(clink, SHA1_TYPE)));
      }

      if (sha256 != null) {
        receiver.output(KV.of(sha256, KV.of(clink, SHA256_TYPE)));
      }
    }
  }

}
