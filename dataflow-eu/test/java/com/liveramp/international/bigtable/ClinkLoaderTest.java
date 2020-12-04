package com.liveramp.international.bigtable;

import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;


import com.google.bigtable.admin.v2.ColumnFamily;
import com.google.bigtable.admin.v2.CreateTableRequest;
import com.google.bigtable.admin.v2.ListTablesRequest;
import com.google.bigtable.admin.v2.Table;
import com.google.bigtable.v2.ReadRowsRequest;
import com.google.bigtable.v2.Row;
import com.google.bigtable.v2.RowSet;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.config.CredentialOptions;
import com.google.cloud.bigtable.grpc.BigtableDataClient;
import com.google.cloud.bigtable.grpc.BigtableSession;
import com.google.cloud.bigtable.grpc.BigtableTableAdminClient;
import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import com.liveramp.international.bigtable.ClinkLoader.SplitRowToKV;
import com.liveramp.testing.categories.IntegrationTest;
import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.ValidatesRunner;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.lang.StringUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.EnvironmentVariables;
import org.junit.experimental.categories.Category;

@Category(IntegrationTest.class)
public class ClinkLoaderTest {

  private static final String EMULATOR_CONTAINER = "bigtable-emulator" + System.currentTimeMillis();
  private static final String PROJECT_ID = "test";
  private static final String INSTANCE_ID = "test";
  private static final String INSTANCE_DESCRIPTOR = "projects/" + PROJECT_ID + "/instances/" + INSTANCE_ID;
  private static final BigtableOptions BIGTABLE_OPTIONS = new BigtableOptions.Builder()
      .setUserAgent("fake")
      .setProjectId(PROJECT_ID)
      .setInstanceId(INSTANCE_ID)
      .setCredentialOptions(CredentialOptions.nullCredential())
      .setUsePlaintextNegotiation(true)
      .setAdminHost("localhost")
      .setDataHost("localhost")
      .setPort(8080)
      .build();

  @ClassRule
  public static final EnvironmentVariables environmentVariables = new EnvironmentVariables();

  @Rule
  public TestPipeline p = TestPipeline.create();

  @BeforeClass
  public static void setupBigTableEmulator() throws Exception {
    environmentVariables.set("BIGTABLE_EMULATOR_HOST", "localhost:8080");
    new ProcessBuilder(
        "docker", "run", "-d", "-p", "8080:8080", "--name", EMULATOR_CONTAINER, "spotify/bigtable-emulator:latest")
        .redirectErrorStream(true)
        .redirectOutput(new File("emulator.log"))
        .start()
        .waitFor();
  }

  @AfterClass
  public static void terminateBigTableEmulator() throws Exception {
    new ProcessBuilder("docker", "kill", EMULATOR_CONTAINER)
        .redirectErrorStream(true)
        .start()
        .waitFor();

    new ProcessBuilder("docker", "rm", EMULATOR_CONTAINER)
        .redirectErrorStream(true)
        .start()
        .waitFor();
  }

  private static final List<String> RAW_INPUT_DATA = Lists.newArrayList(
      "clink_0|value_0",
      "clink_1|value_1|",
      "|",
      "",
      "clink_2|",
      "|value_3"
  );


  @Test
  public void test_row_parse_function_keeps_full_rows_and_drops_insufficient_rows() {
    PCollection<KV<String, String>> clinksMapping = p.apply(Create.of(RAW_INPUT_DATA).withCoder(StringUtf8Coder.of()))
        .apply(ParDo.of(new SplitRowToKV(StaticValueProvider.of("|"))));
    PAssert.that(clinksMapping).containsInAnyOrder(
        KV.of("clink_0", "value_0"),
        KV.of("clink_1", "value_1")
    );

    //Checking that rows with missing values are filtered appropriately
    PAssert.that(clinksMapping).satisfies(itr -> {
          itr.forEach(kv -> {
            assertNotEquals(kv.getKey(), "clink_2");
            assertTrue(StringUtils.isNotBlank(kv.getKey()));

            assertNotEquals(kv.getValue(), "value_3");
            assertTrue(StringUtils.isNotBlank(kv.getValue()));
          });
          return null;
        }
    );

    p.run();
  }

  @Test
  @Category(ValidatesRunner.class)
  public void test_clink_written_as_rowkey_properly() throws Exception {
    String tableId = "clinks_key" + System.currentTimeMillis();
    String family = "cf";

    String tableDescriptor = INSTANCE_DESCRIPTOR + "/tables/" + tableId;

    createTable(tableId, family);

    PCollection<KV<String, String>> clinkMapping0 = p.apply(Create.of(RAW_INPUT_DATA).withCoder(StringUtf8Coder.of()))
        .apply(ParDo.of(new SplitRowToKV(StaticValueProvider.of("|"))));

    BigtableLoaderHelper.writeKeyAsRowkey(
        clinkMapping0,
        0,
        ValueProvider.StaticValueProvider.of(tableId),
        ValueProvider.StaticValueProvider.of(family),
        ValueProvider.StaticValueProvider.of(PROJECT_ID),
        ValueProvider.StaticValueProvider.of(INSTANCE_ID));

    p.run().waitUntilFinish();

    try (BigtableSession session = new BigtableSession(BIGTABLE_OPTIONS)) {
      //Get row form bigtable which created wiht BeforeClass
      BigtableDataClient dataClient = session.getDataClient();
      List<Row> results = dataClient.readRowsAsync(ReadRowsRequest.newBuilder()
          .setTableName(tableDescriptor)
          .setRows(
              RowSet.newBuilder()
                  .addRowKeys(ByteString.copyFromUtf8("clink_0"))
                  .addRowKeys(ByteString.copyFromUtf8("clink_1"))
          )
          .build()
      ).get();

      Assert.assertEquals(results.size(), 2);
      checkRowClinkAsKey(results.get(0));
      checkRowClinkAsKey(results.get(1));
    }
  }

  @Test
  @Category(ValidatesRunner.class)
  public void test_clink_written_as_value_properly() throws Exception {
    String tableId = "clinks_value" + System.currentTimeMillis();
    String family = "cf";
    String tableDescriptor = INSTANCE_DESCRIPTOR + "/tables/" + tableId;
    createTable(tableId, family);

    PCollection<KV<String, String>> clinkMapping1 = p.apply(Create.of(RAW_INPUT_DATA).withCoder(StringUtf8Coder.of()))
        .apply(ParDo.of(new SplitRowToKV(StaticValueProvider.of("|"))));

    BigtableLoaderHelper.writeValueAsRowkey(
        clinkMapping1,
        0,
        ValueProvider.StaticValueProvider.of(tableId),
        ValueProvider.StaticValueProvider.of(family),
        ValueProvider.StaticValueProvider.of(PROJECT_ID),
        ValueProvider.StaticValueProvider.of(INSTANCE_ID)
    );

    p.run().waitUntilFinish();

    try (BigtableSession session = new BigtableSession(BIGTABLE_OPTIONS)) {
      BigtableDataClient dataClient = session.getDataClient();
      List<Row> results = dataClient.readRowsAsync(ReadRowsRequest.newBuilder()
          .setTableName(tableDescriptor)
          .setRows(
              RowSet.newBuilder()
                  .addRowKeys(ByteString.copyFromUtf8("value_0"))
                  .addRowKeys(ByteString.copyFromUtf8("value_1"))
          )
          .build()
      ).get();

      Assert.assertEquals(results.size(), 2);
      checkRowClinkAsValue(results.get(0));
      checkRowClinkAsValue(results.get(1));
    }
  }

  private void checkRowClinkAsKey(Row row) throws Exception {
    String rowKey = row.getKey().toString("UTF8");
    String expectedColunm = rowKey.equals("clink_0") ? "value_0" : "value_1";

    Assert.assertEquals(row.getFamiliesList().size(), 1);
    Assert.assertEquals(row.getFamilies(0).getName(), "cf");
    Assert.assertEquals(row.getFamilies(0).getColumnsList().size(), 1);
    Assert.assertEquals(row.getFamilies(0).getColumns(0).getQualifier().toString("UTF8"), expectedColunm);
    Assert.assertEquals(row.getFamilies(0).getColumns(0).getCellsCount(), 1);
  }

  private void checkRowClinkAsValue(Row row) throws Exception {
    String rowKey = row.getKey().toString("UTF8");
    String expectedColunm = rowKey.equals("value_0") ? "clink_0" : "clink_1";

    Assert.assertEquals(row.getFamiliesList().size(), 1);
    Assert.assertEquals(row.getFamilies(0).getName(), "cf");
    Assert.assertEquals(row.getFamilies(0).getColumnsList().size(), 1);
    Assert.assertEquals(row.getFamilies(0).getColumns(0).getQualifier().toString("UTF8"), expectedColunm);
    Assert.assertEquals(row.getFamilies(0).getColumns(0).getCellsCount(), 1);
  }

  private void createTable(String table, String... columnFamilies) throws Exception {
    Map<String, ColumnFamily> columnFamilyMap = Arrays.stream(columnFamilies)
        .collect(Collectors.toMap(
            cf -> cf,
            cf -> ColumnFamily.getDefaultInstance()
        ));

    //Get table list
    try (BigtableSession session = new BigtableSession(BIGTABLE_OPTIONS)) {
      BigtableTableAdminClient tableAdminClient = session.getTableAdminClient();
      List<String> tables = session
          .getTableAdminClient()
          .listTables(ListTablesRequest.newBuilder().setParent(INSTANCE_DESCRIPTOR).build())
          .getTablesList()
          .stream().map(tableResult -> {
                String[] nameComponents = tableResult.getName().split("/");
                return nameComponents[nameComponents.length - 1];
              }
          ).collect(Collectors.toList());

      if (!tables.contains(table)) {
        tableAdminClient.createTable(
            CreateTableRequest.newBuilder()
                .setParent(INSTANCE_DESCRIPTOR)
                .setTableId(table)
                .setTable(Table.newBuilder().putAllColumnFamilies(columnFamilyMap))
                .build()
        );
      }
    }
  }
}
