package com.liveramp.international.bigtable;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import com.google.bigtable.admin.v2.ColumnFamily;
import com.google.bigtable.admin.v2.CreateTableRequest;
import com.google.bigtable.admin.v2.DeleteTableRequest;
import com.google.bigtable.admin.v2.ListTablesRequest;
import com.google.bigtable.admin.v2.Table;
import com.google.bigtable.v2.ReadRowsRequest;
import com.google.bigtable.v2.Row;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.config.CredentialOptions;
import com.google.cloud.bigtable.grpc.BigtableDataClient;
import com.google.cloud.bigtable.grpc.BigtableSession;
import com.google.cloud.bigtable.grpc.BigtableTableAdminClient;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.EnvironmentVariables;
import org.junit.experimental.categories.Category;

import com.liveramp.international.bigtable.PiiExpansionDataLoadBigtable.PiiExpansionMappingFn;
import com.liveramp.testing.categories.IntegrationTest;

import static com.liveramp.international.bigtable.PiiExpansionDataLoadBigtable.BigTableWriterFn.testBigTableWriterFn;

@Category(IntegrationTest.class)
public class PiiExpansionDataLoaderTest {

  @Rule
  public final transient TestPipeline pipeline = TestPipeline.create();

  @ClassRule
  public static final EnvironmentVariables environmentVariables
      = new EnvironmentVariables();

  private static String emulatorContainerName = "bigtable-emulator" + System.currentTimeMillis();

  @BeforeClass
  public static void setupBigTableEmulator() throws Exception {
    environmentVariables.set("BIGTABLE_EMULATOR_HOST", "127.0.0.1:8086");
    new ProcessBuilder("docker", "run", "-d", "-p", "8086:8080", "--name", emulatorContainerName,
        "spotify/bigtable" +
            "-emulator" +
            ":latest")
        .redirectErrorStream(true)
        .redirectOutput(new File("emulator.log"))
        .start()
        .waitFor();
  }

  @AfterClass
  public static void terminateBigTableEmulator() throws Exception {
    new ProcessBuilder("docker", "kill", emulatorContainerName)
        .redirectErrorStream(true)
        .start()
        .waitFor();

    new ProcessBuilder("docker", "rm", emulatorContainerName)
        .redirectErrorStream(true)
        .start()
        .waitFor();
  }

  private String projectId = "test-proj";
  private String instanceId = "com-liveramp-bigtable";
  private String tableId = "pii_expansion";
  private int port = 8086;

  String instanceName = "projects/" + projectId + "/instances/" + instanceId;
  String tableName = instanceName + "/tables/" + tableId;


  @Test
  public void fullPipelineTest() throws Exception {

    BigtableSession session = initBigTableEmulator();
    //given: I have two input files (mobile and email data)
    final TupleTag<String> emailTag = new TupleTag<>();
    final TupleTag<String> phoneTag = new TupleTag<>();
    final String region = "DE";

    List<String> emailLines = new ArrayList<>();
    emailLines.add("0 E1");
    emailLines.add("0 E2");
    emailLines.add("1 E3");
    emailLines.add("1 E4");

    List<String> phoneLines = new ArrayList<>();
    phoneLines.add("0 P1");
    phoneLines.add("1 P2");
    phoneLines.add("1 P3");

    long jobTimestamp = System.currentTimeMillis() * 1000;

    PCollection<KV<String, String>> emails = pipeline
        .apply("Read Email Lines", Create.of(emailLines))
        .apply("Parse Email Lines", ParDo.of(new PiiExpansionDataLoadBigtable.ConvertRawInputFn()));

    PCollection<KV<String, String>> phones = pipeline
        .apply("Read Phone Lines", Create.of(phoneLines))
        .apply("Parse Phone Lines", ParDo.of(new PiiExpansionDataLoadBigtable.ConvertRawInputFn()));

    KeyedPCollectionTuple.of(emailTag, emails).and(phoneTag, phones)
        .apply("Combine Phone and Email data", CoGroupByKey.create())
        .apply("PiiExpansion Mapping", ParDo.of(new PiiExpansionMappingFn(phoneTag, emailTag,
            ValueProvider.StaticValueProvider.of(region))))
        .apply("Write to BigTable", ParDo.of(testBigTableWriterFn(ValueProvider.StaticValueProvider.of(projectId), ValueProvider.StaticValueProvider.of(instanceId), ValueProvider.StaticValueProvider.of(tableId), jobTimestamp)));

    pipeline.run().waitUntilFinish();

    //Get row form bigtable which created with BeforeClass
    BigtableDataClient dataClient = session.getDataClient();
    List<Row> results = dataClient.readRowsAsync(ReadRowsRequest.newBuilder()
        .setTableName(tableName)
        .build()
    ).get();

    Assert.assertNotNull(results);
    Assert.assertEquals(7, results.size());
    System.out.println(results);
  }

  private BigtableSession initBigTableEmulator() throws IOException {

    BigtableOptions opts = new BigtableOptions.Builder()
        .setUserAgent("fake")
        .setProjectId(projectId)
        .setCredentialOptions(CredentialOptions.nullCredential())
        .setInstanceId(instanceId)
        .build();

    //Get table list
    BigtableSession session = new BigtableSession(opts);
    List<String> tables =
        session
            .getTableAdminClient()
            .listTables(ListTablesRequest.newBuilder().setParent(instanceName).build())
            .getTablesList()
            .stream().map(table -> {
              String[] nameComponents = table.getName().split("/");
              return nameComponents[nameComponents.length - 1];
            }
        ).collect(Collectors.toList());

    BigtableTableAdminClient tableAdminClient = session.getTableAdminClient();

    //Delete table
    if (tables.contains(tableId)) {
      tableAdminClient.deleteTable(DeleteTableRequest.newBuilder().setName(tableName).build());
    }

    //Create table
    tableAdminClient.createTable(
        CreateTableRequest.newBuilder()
            .setParent(instanceName)
            .setTableId(tableId)
            .setTable(
                Table.newBuilder()
                    .putColumnFamilies("phone", ColumnFamily.getDefaultInstance())
                    .putColumnFamilies("email", ColumnFamily.getDefaultInstance())
            )
            .build()
    );
    return session;
  }
}
