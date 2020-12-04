package com.liveramp.dataflow.arlpel;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

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
import com.google.protobuf.ByteString;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.ValidatesRunner;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.codec.binary.Hex;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.EnvironmentVariables;
import org.junit.experimental.categories.Category;

import com.liveramp.testing.categories.IntegrationTest;

import static org.junit.Assert.assertFalse;


@Category(IntegrationTest.class)
public class LoadArlPelBigtableTest {

  @ClassRule
  public static final EnvironmentVariables environmentVariables
      = new EnvironmentVariables();

  private static String emulatorContainerName = "bigtable-emulator" + System.currentTimeMillis();

  @Rule
  public TestPipeline p = TestPipeline.create();

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

  @Test
  @Ignore //Works locally, can't get thbis to work with Jenkins
  public void testGenArlPelMappingFn() {
    PCollection<KV<String, String>> arlToPel = p.apply(Create.of(RAW_INPUT_DATA).withCoder(StringUtf8Coder.of()))
        .apply(ParDo.of(new LoadArlPelBigtable.GenArlPelMappingFn()));
    PAssert.that(arlToPel)
        .containsInAnyOrder(KV.of("a024d50fdb101d339512dacd52fdb05162c601fecfe69924fd4969e8d5a66e3b", "pel1"),
            KV.of("e98b625bc8aa46648c881d1e8fe641c61c9e33d882ed1928220def19b12c196a", "pel2"));

    PAssert.that(arlToPel).satisfies(itr -> {
          itr.forEach(kv -> {
            assertFalse(kv.getKey().equals("387fe669a802319fc96f4b76db5efd98afef1a0866f50825306b426d086b6ddb"));
            assertFalse(kv.getKey().equals("3243045a48885666ddbf7b06bfd849d706a04f77cd03ddb3ad27cc7662a722ce"));
            assertFalse(kv.getKey().isEmpty());

            assertFalse(kv.getValue().equals("pel3"));
            assertFalse(kv.getValue().equals("pel5"));
            assertFalse(kv.getValue().isEmpty());
          });
          return null;
        }
    );

    p.run().waitUntilFinish();

  }

  static final String[] RAW_INPUT_DATA_ARR =
      new String[]{
          "a024d50fdb101d339512dacd52fdb05162c601fecfe69924fd4969e8d5a66e3b|pel1",
          "e98b625bc8aa46648c881d1e8fe641c61c9e33d882ed1928220def19b12c196a|pel2", "|",
          "387fe669a802319fc96f4b76db5efd98afef1a0866f50825306b426d086b6ddb,pel3", "",
          "3243045a48885666ddbf7b06bfd849d706a04f77cd03ddb3ad27cc7662a722ce|", "|pel5"
      };

  static final List<String> RAW_INPUT_DATA = Arrays.asList(RAW_INPUT_DATA_ARR);

  @Test
  @Ignore //Works locally, can't get this to work with Jenkins
  @Category(ValidatesRunner.class)
  public void testBigTableWrite() throws Exception {

    String projectId = "test";
    String instanceId = "test";
    String tableId = "arl-pel" + System.currentTimeMillis();
    String family = "uk";

    String instanceName = "projects/" + projectId + "/instances/" + instanceId;
    String tableName = instanceName + "/tables/" + tableId;

    BigtableOptions opts = new BigtableOptions.Builder()
        .setUserAgent("fake")
        .setProjectId(projectId)
        .setCredentialOptions(CredentialOptions.nullCredential())
        .setInstanceId(instanceId)
        .build();

    try (BigtableSession session = new BigtableSession(opts)) {
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
      if (!tables.contains(tableId)) {
        BigtableTableAdminClient tableAdminClient = session.getTableAdminClient();
        tableAdminClient.createTable(
            CreateTableRequest.newBuilder()
                .setParent(instanceName)
                .setTableId(tableId)
                .setTable(
                    Table.newBuilder()
                        .putColumnFamilies(family, ColumnFamily.getDefaultInstance())
                )
                .build()
        );
      }

      String[] args = {"--bigTableTableName=" + tableId, "--bigtableColumnFamily=" + family,
          "--projectId" +
              "=" + projectId,
          "--bigTableInstanceId=" + instanceId};

      LoadArlPelBigtable.LoadArlPelOptions options =
          PipelineOptionsFactory.fromArgs(args).
              withValidation().as(LoadArlPelBigtable.LoadArlPelOptions.class);

      PCollection<KV<String, String>> arlToPel = p.apply(Create.of(RAW_INPUT_DATA).withCoder(StringUtf8Coder.of()))
          .apply(ParDo.of(new LoadArlPelBigtable.GenArlPelMappingFn()));

      LoadArlPelBigtable.writeArlPelToBigtable(
          arlToPel,
          ValueProvider.StaticValueProvider.of(tableId),
          ValueProvider.StaticValueProvider.of(family),
          ValueProvider.StaticValueProvider.of(projectId),
          ValueProvider.StaticValueProvider.of(instanceId));

      p.run().waitUntilFinish();

      BigtableDataClient dataClient = session.getDataClient();
      List<Row> results = dataClient.readRowsAsync(ReadRowsRequest.newBuilder()
          .setTableName(tableName)
          .setRows(
              RowSet.newBuilder()
                  .addRowKeys(ByteString.copyFrom(
                      Hex.decodeHex("a024d50fdb101d339512dacd52fdb05162c601fecfe69924fd4969e8d5a66e3b".toCharArray())))
                  .addRowKeys(ByteString.copyFrom(
                      Hex.decodeHex("e98b625bc8aa46648c881d1e8fe641c61c9e33d882ed1928220def19b12c196a".toCharArray())))
          )
          .build()
      ).get();

      Assert.assertEquals(results.size(), 2);
      checkRow(results.get(0));
      checkRow(results.get(1));
    }


  }

  private void checkRow(Row row) throws Exception {
//    String arlKey = row.getKey().toString("UTF8");
    String arlKey = Hex.encodeHexString(row.getKey().toByteArray());

    String expectedPel =
        arlKey.equals("a024d50fdb101d339512dacd52fdb05162c601fecfe69924fd4969e8d5a66e3b") ? "pel1" : "pel2";

    Assert.assertEquals(row.getFamiliesList().size(), 1);
    Assert.assertEquals(row.getFamilies(0).getName(), "uk");
    Assert.assertEquals(row.getFamilies(0).getColumnsList().size(), 1);
    Assert.assertEquals(row.getFamilies(0).getColumns(0).getQualifier().toString("UTF8"), "pel");
    Assert.assertEquals(row.getFamilies(0).getColumns(0).getCellsCount(), 1);
    Assert.assertEquals(row.getFamilies(0).getColumns(0).getCells(0).getValue().toString("UTF8"), expectedPel);
  }

}
