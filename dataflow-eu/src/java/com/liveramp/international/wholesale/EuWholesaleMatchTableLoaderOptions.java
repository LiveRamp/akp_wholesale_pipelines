package com.liveramp.international.wholesale;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Hidden;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;

import static com.liveramp.international.wholesale.WholesaleConstants.COLUMN_FAMILY;
import static com.liveramp.international.wholesale.WholesaleConstants.INSTANCE_ID;
import static com.liveramp.international.wholesale.WholesaleConstants.CLINK_COLUMN_INDEX;
import static com.liveramp.international.wholesale.WholesaleConstants.MD5_COLUMN_QUALIFIER;
import static com.liveramp.international.wholesale.WholesaleConstants.MD5_EMAIL_COLUMN_INDEX;
import static com.liveramp.international.wholesale.WholesaleConstants.PROJECT_ID;
import static com.liveramp.international.wholesale.WholesaleConstants.SHA1_COLUMN_QUALIFIER;
import static com.liveramp.international.wholesale.WholesaleConstants.SHA1_EMAIL_COLUMN_INDEX;
import static com.liveramp.international.wholesale.WholesaleConstants.SHA256_COLUMN_QUALIFIER;
import static com.liveramp.international.wholesale.WholesaleConstants.SHA256_EMAIL_COLUMN_INDEX;
import static com.liveramp.international.wholesale.WholesaleConstants.SHA_256_COLUMN_INDEX;
import static com.liveramp.international.wholesale.WholesaleConstants.TABLE_ID;

public interface EuWholesaleMatchTableLoaderOptions extends PipelineOptions {

  // Experian data
  @Validation.Required
  @Description("GCS path to sha256 file, e.g. gs://bucket/some/path")
  ValueProvider<String> getSha256Inputpath();

  void setSha256Inputpath(ValueProvider<String> value);

  @Description("Header column index for sha256 column in sha256 file")
  @Default.Integer(SHA_256_COLUMN_INDEX)
  ValueProvider<Integer> getSha256ColumnIndex();

  void setSha256ColumnIndex(ValueProvider<Integer> value);


  // KnowledgeBase data
  @Validation.Required
  @Description("GCS path to raw email &a  pel file, e.g. gs://bucket/some/path")
  ValueProvider<String> getRawEmailToPelInputpath();

  void setRawEmailToPelInputpath(ValueProvider<String> value);

  @Description("Header column index for raw email column in raw email & pel file")
  @Default.Integer(MD5_EMAIL_COLUMN_INDEX)
  ValueProvider<Integer> getMd5EmailColumnIndex();

  void setMd5EmailColumnIndex(ValueProvider<Integer> value);

  @Description("Header column index for raw email column in raw email & pel file")
  @Default.Integer(SHA1_EMAIL_COLUMN_INDEX)
  ValueProvider<Integer> getSha1EmailColumnIndex();

  void setSha1EmailColumnIndex(ValueProvider<Integer> value);

  @Description("Header column index for raw email column in raw email & pel file")
  @Default.Integer(SHA256_EMAIL_COLUMN_INDEX)
  ValueProvider<Integer> getSha256EmailColumnIndex();

  void setSha256EmailColumnIndex(ValueProvider<Integer> value);

  @Description("Header column index for pel column in raw email & pel file")
  @Default.Integer(CLINK_COLUMN_INDEX)
  ValueProvider<Integer> getPelColumnIndex();

  void setPelColumnIndex(ValueProvider<Integer> value);


  // Bigtable Configuration
  @Hidden
  @Description("GCP project id")
  @Default.String(PROJECT_ID)
  String getProjectId();

  void setProjectId(String value);

  @Hidden
  @Description("Bigtable instance id")
  @Default.String(INSTANCE_ID)
  String getInstanceId();

  void setInstanceId(String value);

  @Hidden
  @Description("Bigtable table id")
  @Default.String(TABLE_ID)
  String getTableId();

  void setTableId(String value);

  @Hidden
  @Description("Column family to load pel/hash match data into")
  @Default.String(COLUMN_FAMILY)
  String getColumnFamily();

  void setColumnFamily(String value);

  @Hidden
  @Description("sha256 column qualifier")
  @Default.String(SHA256_COLUMN_QUALIFIER)
  String getSha256ColumnQualifier();

  void setSha256ColumnQualifier(String value);

  @Hidden
  @Description("sha1 column qualifier")
  @Default.String(SHA1_COLUMN_QUALIFIER)
  String getSha1ColumnQualifier();

  void setSha1ColumnQualifier(String value);

  @Hidden
  @Description("md5 column qualifier")
  @Default.String(MD5_COLUMN_QUALIFIER)
  String getMd5ColumnQualifier();

  void setMd5ColumnQualifier(String value);
}

