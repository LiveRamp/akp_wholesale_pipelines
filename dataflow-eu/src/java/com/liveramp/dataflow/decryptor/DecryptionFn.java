package com.liveramp.dataflow.decryptor;

import java.io.BufferedReader;
import java.nio.charset.Charset;

import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.liveramp.dataflow.common.CommonFunctions;
import com.liveramp.ingestion.eu.generated.CloudBucketPath;
import com.liveramp.ingestion.logging.failure.DecryptionFileFailure;

public class DecryptionFn extends DoFn<String, String> {

  private ValueProvider<String> mode;

  private String charset;

  public static final TupleTag<DecryptionFileFailure> decryptionFileFailureTupleTag = new TupleTag<DecryptionFileFailure>() {
  };

  private static final Logger LOG = LoggerFactory.getLogger(DecryptionFn.class);

  public DecryptionFn(ValueProvider<String> mode, String charset) {
    this.mode = mode;
    this.charset = charset;
  }

  @ProcessElement
  public void processElement(@Element String inputPath, OutputReceiver<String> outputReceiver) {
    String pathWithouPrefix = removeGcsPrefix(inputPath);
    LOG.info("inputPath: " + inputPath);
    String key = getCloudObjectKey(pathWithouPrefix);
    LOG.info("key: " + key);
    String bucket = getCloudBucketName(pathWithouPrefix);
    LOG.info("bucket: " + bucket);
    CloudBucketPath pathToFile = new CloudBucketPath().set_bucket(bucket).set_key(key);
    try (BufferedReader eb = CommonFunctions
        .getBufferedReader(pathToFile, mode, Charset.forName(charset), pathToFile.get_key())) {
      String line;
      while ((line = eb.readLine()) != null) {
        outputReceiver.output(line);
      }
    } catch (Throwable t) {
      DecryptionFileFailure failure = key != null
          ? new DecryptionFileFailure(key, t)
          : new DecryptionFileFailure("", t);
      LOG.error(failure.scrubbed());
    }
  }

  private static String getCloudBucketName(String cloudLocation) {
    return StringUtils.substringBefore(cloudLocation, "/");
  }

  private static String getCloudObjectKey(String cloudLocation) {
    return StringUtils.substringAfter(cloudLocation, "/");
  }

  private static String removeGcsPrefix(String cloudBucketPath) {
    return StringUtils.stripStart(cloudBucketPath, "gs://");
  }

}




