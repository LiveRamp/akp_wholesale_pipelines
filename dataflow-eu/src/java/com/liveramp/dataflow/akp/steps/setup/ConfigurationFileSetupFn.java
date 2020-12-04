package com.liveramp.dataflow.akp.steps.setup;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Serializable;
import java.nio.channels.Channels;
import java.util.function.Function;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.liveramp.dataflow.akp.FullRefreshArlPelOptions;

public class ConfigurationFileSetupFn implements Serializable,
    Function<ValueProvider<String>, FullRefreshArlPelOptions> {

  private Logger LOG = LoggerFactory.getLogger(ConfigurationFileSetupFn.class);

  @Override
  public FullRefreshArlPelOptions apply(ValueProvider<String> configFile) {
    FullRefreshArlPelOptions fullRefreshArlPelOptions;
    Storage gcsClient = StorageOptions.getDefaultInstance().getService();
    String bucket = getBucket(configFile.get());
    String object = getObject(configFile.get());
    LOG.info("--> Config file:" + bucket + "/" + object);
    Blob v = gcsClient.get(BlobId.of(bucket, object));
    ObjectMapper objectMapper = new ObjectMapper();
    try {
      fullRefreshArlPelOptions = objectMapper
          .readValue(new BufferedReader(Channels.newReader(v.reader(), "UTF-8")), FullRefreshArlPelOptions.class);
      return fullRefreshArlPelOptions;
    } catch (IOException e) {
      LOG.error("Get Config file error : {}", e.getMessage());
    }
    return null;
  }

  private static String getBucket(String fullObjectPath) {
    String prefixRemoved = fullObjectPath.substring(5);
    return prefixRemoved.split("/")[0];
  }

  private static String getObject(String fullObjectPath) {
    String prefixRemoved = fullObjectPath.substring(5);
    return prefixRemoved.substring(prefixRemoved.indexOf("/") + 1);
  }
}
