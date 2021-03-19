package com.liveramp.international.bigtable;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.google.bigtable.repackaged.com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.bigtable.repackaged.com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.bigtable.repackaged.com.google.cloud.bigtable.data.v2.models.RowMutation;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PiiExpansionDataLoadBigtable {

  public static class ConvertRawInputFn extends DoFn<String, KV<String, String>> {

    private static final Logger LOG = LoggerFactory.getLogger(ConvertRawInputFn.class);
    private static final String WHITESPACE_REGEX = "\\s+";

    @ProcessElement
    public void processElement(ProcessContext c) {
      String line = c.element();
      String[] splitLine = line.trim().split(WHITESPACE_REGEX);

      if (splitLine.length == 2) {
        String customerId = splitLine[0];
        String hashedPii = splitLine[1];

        c.output(KV.of(customerId, hashedPii));
      } else {
        LOG.error("A Line has failed to parse");
        LOG.debug("Erroneous Line: " + line);
      }
    }
  }

  public static class PiiExpansionMappingFn extends DoFn<KV<String, CoGbkResult>, KV<String, Map<String, String>>> {

    private static final Logger LOG = LoggerFactory.getLogger(PiiExpansionMappingFn.class);

    final TupleTag<String> phoneTag;
    final TupleTag<String> emailTag;
    final ValueProvider<String> region;

    public PiiExpansionMappingFn(TupleTag<String> phoneTag, TupleTag<String> emailTag, ValueProvider<String> region) {
      this.phoneTag = phoneTag;
      this.emailTag = emailTag;
      this.region = region;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      Iterable<String> emailsIter = c.element().getValue().getAll(emailTag);
      Iterable<String> phonesIter = c.element().getValue().getAll(phoneTag);

      String emailCF = "email";
      String phoneCF = "phone";

      //Add all entries when a email is the rowKey
      emailsIter.forEach(hashForRowKey ->
          createExpansionMappings(c, emailsIter, phonesIter, emailCF, phoneCF, hashForRowKey));

      //Add all entries when a phone is the rowKey
      phonesIter.forEach(hashForRowKey ->
          createExpansionMappings(c, phonesIter, emailsIter, phoneCF, emailCF, hashForRowKey));
    }

    private void createExpansionMappings(
        ProcessContext c, Iterable<String> mainHashIter, Iterable<String> secondaryHashIter,
        String mainColumnFamily, String secondaryColumnFamily, String hashForRowKey) {

      String rowKeyPrefix = region.get() + "#" + mainColumnFamily + "#";
      Map<String, String> piiExpansions = new HashMap<>();
      String rowKey = rowKeyPrefix + hashForRowKey;

      //Add all hashes for secondary iterable
      // e.g. if Main iter is email hashes and secondary is Phone hashes this will add all the phone hashes
      secondaryHashIter.forEach(hashForQualifier -> piiExpansions.put(hashForQualifier, secondaryColumnFamily));

      //Add other hashes from mainIter
      // e.g. If Main iter is email hashes add all the OTHER email hashes (excluding the rowKeyHash)
      mainHashIter.forEach(
          hashForQualifier -> {
            // Need to skip the hashForRowKey as it is the from the Main Iter and we don't want to refer to itself
            if (!hashForQualifier.equalsIgnoreCase(hashForRowKey)) {
              piiExpansions.put(hashForQualifier, mainColumnFamily);
            }
          });

      c.output(KV.of(rowKey, piiExpansions));
    }
  }

  public static class BigTableWriterFn extends DoFn<KV<String, Map<String, String>>, Void> {

    private static final Logger LOG = LoggerFactory.getLogger(BigTableWriterFn.class);

    private final ValueProvider<String> projectId;
    private final ValueProvider<String> instanceId;
    private final boolean isTest;
    final ValueProvider<String> tableId;
    private final long jobTimestamp;

    BigtableDataClient dataClient;

    public static BigTableWriterFn testBigTableWriterFn(
        ValueProvider<String> projectId, ValueProvider<String> instanceId, ValueProvider<String> tableId, long jobTimestamp) {
      return new BigTableWriterFn(projectId, instanceId, tableId, jobTimestamp, true);
    }

    static BigTableWriterFn prodBigTableWriterFn(
        ValueProvider<String> projectId, ValueProvider<String> instanceId, ValueProvider<String> tableId, long jobTimestamp) {
      return new BigTableWriterFn(projectId, instanceId, tableId, jobTimestamp, false);
    }

    private BigTableWriterFn(ValueProvider<String> projectId, ValueProvider<String> instanceId, ValueProvider<String> tableId, long jobTimestamp, boolean isTest) {
      this.projectId = projectId;
      this.instanceId = instanceId;
      this.tableId = tableId;
      this.jobTimestamp = jobTimestamp;
      this.isTest = isTest;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      KV<String, Map<String, String>> inputData = c.element();
      String rowKey = inputData.getKey();
      Map<String, String> hashesToAdd = inputData.getValue();

      LOG.debug("Number of hashes to ADD: " + hashesToAdd.size());
      if (hashesToAdd.size() > 0) {
        RowMutation rowMutation = RowMutation.create(this.tableId.get(), rowKey);

        hashesToAdd.forEach((hashValue, hashType) -> {
          LOG.debug("RK: " + rowKey + " HT: " + hashType + " HV: " + hashValue + " Timestamp: " + jobTimestamp + "");
          rowMutation.setCell(hashType, hashValue, jobTimestamp, "");
        });

        this.dataClient.mutateRow(rowMutation);
      }
    }

    @Setup
    public void setup() throws IOException {

      if (isTest) {
        LOG.info("Creating Test Data client");
        this.dataClient = BigtableDataClient.create(BigtableDataSettings.newBuilderForEmulator("127.0.0.1", 8086)
            .setProjectId(projectId.get())
            .setInstanceId(instanceId.get())
            .build());
      } else {
        LOG.info("Creating BigTable Data client for: " + projectId + " : " + instanceId + " : " + tableId);
        this.dataClient = BigtableDataClient.create(BigtableDataSettings.newBuilder()
            .setProjectId(projectId.get())
            .setInstanceId(instanceId.get())
            .build());
      }
    }
  }

  //options
  public interface LoadPiiExpansionOptions extends PipelineOptions {

    ValueProvider<String> getEmailInputFile();

    void setEmailInputFile(ValueProvider<String> value);

    ValueProvider<String> getPhoneInputFile();

    void setPhoneInputFile(ValueProvider<String> value);

    ValueProvider<String> getProjectId();

    void setProjectId(ValueProvider<String> projectId);

    ValueProvider<String> getBigTableInstanceId();

    void setBigTableInstanceId(ValueProvider<String> bigtableInstanceId);

    ValueProvider<String> getTableId();

    void setTableId(ValueProvider<String> tableId);

    ValueProvider<String> getLrRegion();

    void setLrRegion(ValueProvider<String> region);
  }

  public static void main(String[] args) {

    LoadPiiExpansionOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
        .as(LoadPiiExpansionOptions.class);
    Pipeline pipeline = Pipeline.create(options);

    final TupleTag<String> phoneTag = new TupleTag<>();
    final TupleTag<String> emailTag = new TupleTag<>();

    /*final String projectId = options.getProjectId();
    final String instanceId = options.getBigTableInstanceId().toString();
    final String tableId = options.getTableId().toString();
    final String region = options.getLrRegion().toString();*/

    long jobTimestamp = System.currentTimeMillis() * 1000;

    //Read Data
    PCollection<KV<String, String>> emails = pipeline
        .apply("Read Email Lines", TextIO.read().from(options.getEmailInputFile()))
        .apply("Parse Email Lines", ParDo.of(new ConvertRawInputFn()));

    PCollection<KV<String, String>> phones = pipeline
        .apply("Read Phone Lines", TextIO.read().from(options.getPhoneInputFile()))
        .apply("Parse Phone Lines", ParDo.of(new ConvertRawInputFn()));

    KeyedPCollectionTuple.of(emailTag, emails).and(phoneTag, phones)
        .apply("Combine Phone and Email data", CoGroupByKey.create())
        .apply("PiiExpansion Mapping", ParDo.of(new PiiExpansionMappingFn(phoneTag, emailTag, options.getLrRegion())))
        .apply(
            "Write to BigTable",
            ParDo.of(BigTableWriterFn.prodBigTableWriterFn(options.getProjectId(), options.getBigTableInstanceId(), options.getTableId(), jobTimestamp)));

    pipeline.run();
  }
}

