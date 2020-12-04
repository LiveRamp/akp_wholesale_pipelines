package com.liveramp.dataflow.wholesale.steps;

import java.io.IOException;
import java.util.Objects;

import com.google.cloud.bigtable.hbase.BigtableConfiguration;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;

import com.liveramp.dataflow.common.WholeSaleHelper;

public class ScanWholeSaleFn extends DoFn<KV<String, String>, String> {

  private static final int MD5_HASH_LENGTH = 32;
  private static final int SHA1_HASH_LENGTH = 40;
  private static final int SHA256_HASH_LENGTH = 64;
  private static final String FIXED_OUTPUT_DELIMITER = "|";

  private transient Connection connection;

  private ValueProvider<String> delimiter;

  public ScanWholeSaleFn(ValueProvider<String> delimiter) {
    this.delimiter = delimiter;
  }

  @ProcessElement
  public synchronized void processElement(@Element KV<String, String> cookieAndPel, OutputReceiver<String> receiver)
      throws IOException {
    if (connection == null) {
      connection = BigtableConfiguration.connect(WholeSaleHelper.WHOOLE_SALE_CONFIG.toHBaseConfig());
    }

    Scan scan = new Scan().setRowPrefixFilter(cookieAndPel.getValue().getBytes());
    Table table = connection.getTable(TableName.valueOf(WholeSaleHelper.WHOOLE_SALE_CONFIG.getTableId()));

    String md5Str = "";
    String sha1Str = "";
    String sha256Str = "";

    for (Result result : table.getScanner(scan)) {
      String md5FromDatabase = getValueFromColumn(result, WholeSaleHelper.WHOLE_SALE_MD5.getBytes());
      String sha1FromDatabase = getValueFromColumn(result, WholeSaleHelper.WHOLE_SALE_SHA1.getBytes());
      String sha256FromDatabase = getValueFromColumn(result, WholeSaleHelper.WHOLE_SALE_SHA256.getBytes());

      // From ticket OC-19121 it was discovered that some of the wholesale data is in the wrong column family.
      // This results in MD5 values appearing in the SHA1 output and vice versa.
      // The workaround is to check the length of the hashes and set the 32 length one to be the MD5 hash,
      // 40 length one to be the SHA1 hash and 64 length one to be the SHA256 hash.
      // See: https://liveramp.atlassian.net/browse/OC-19121
      md5Str = getHashByLength(md5FromDatabase, sha1FromDatabase, sha256FromDatabase, MD5_HASH_LENGTH);
      sha1Str = getHashByLength(md5FromDatabase, sha1FromDatabase, sha256FromDatabase, SHA1_HASH_LENGTH);
      sha256Str = getHashByLength(md5FromDatabase, sha1FromDatabase, sha256FromDatabase, SHA256_HASH_LENGTH);
    }

    receiver.output(String.join(FIXED_OUTPUT_DELIMITER, cookieAndPel.getKey(), md5Str, sha1Str, sha256Str));
  }

  private String getValueFromColumn(Result result, byte[] qualifier) {
    Cell latestCell = result.getColumnLatestCell(WholeSaleHelper.WHOLE_SALE_COLUMN_FAMILY.getBytes(), qualifier);
    return new String(latestCell.getValueArray());
  }

  // Helper method. We don't know which of the 3 Strings contains the hash of the required length so we check them all
  private static String getHashByLength(
      final String str1, final String str2, final String str3,
      final int requiredLength) {
    if (Objects.nonNull(str1) && (str1.length() == requiredLength)) {
      return str1;
    } else if (Objects.nonNull(str2) && (str2.length() == requiredLength)) {
      return str2;
    } else if (Objects.nonNull(str3) && (str3.length() == requiredLength)) {
      return str3;
    } else {
      return "";
    }
  }
}