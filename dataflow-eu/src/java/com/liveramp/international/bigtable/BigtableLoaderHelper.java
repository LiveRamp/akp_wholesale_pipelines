package com.liveramp.international.bigtable;

import com.google.bigtable.v2.Mutation;
import com.google.bigtable.v2.Mutation.SetCell;
import com.google.protobuf.ByteString;
import java.util.Collections;
import java.util.Objects;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableIO;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

public class BigtableLoaderHelper {

  private BigtableLoaderHelper() {
  }

  public static void writeKeyAsRowkey(
      PCollection<KV<String, String>> inputData,
      long loadTimestamp,
      ValueProvider<String> tableName,
      ValueProvider<String> columnFamily,
      ValueProvider<String> projectId,
      ValueProvider<String> instanceId
  ) {
    inputData
        .apply(ParDo.of(new GenClinkAsKeyDoFn(columnFamily, loadTimestamp)))
        .apply(BigtableIO.write()
            .withProjectId(projectId)
            .withInstanceId(instanceId)
            .withTableId(tableName)
        );
  }

  public static void writeValueAsRowkey(
      PCollection<KV<String, String>> inputData,
      long loadTimestamp,
      ValueProvider<String> tableName,
      ValueProvider<String> columnFamily,
      ValueProvider<String> projectId,
      ValueProvider<String> instanceId
  ) {
    inputData
        .apply(ParDo.of(new GenClinkAsValueDoFn(columnFamily, loadTimestamp)))
        .apply(BigtableIO.write()
            .withProjectId(projectId)
            .withInstanceId(instanceId)
            .withTableId(tableName)
        );
  }

  private static class GenClinkAsKeyDoFn extends DoFn<KV<String, String>, KV<ByteString, Iterable<Mutation>>> {

    ValueProvider<String> columnFamily;
    long loadMicrosTimestamp;

    GenClinkAsKeyDoFn(ValueProvider<String> columnFamily, long loadMicrosTimestamp) {
      this.columnFamily = columnFamily;
      this.loadMicrosTimestamp = loadMicrosTimestamp;
    }

    @ProcessElement
    public void processElement(
        @Element KV<String, String> element,
        OutputReceiver<KV<ByteString, Iterable<Mutation>>> receiver) {
      Mutation valueWrite = Mutation.newBuilder()
          .setSetCell(SetCell.newBuilder()
              .setTimestampMicros(loadMicrosTimestamp)
              .setFamilyName(columnFamily.get())
              .setColumnQualifier(ByteString.copyFromUtf8(element.getValue()))
              .setValue(ByteString.copyFromUtf8("")).build()).build();
      receiver.output(KV.of(
          ByteString.copyFromUtf8(Objects.requireNonNull(element.getKey())),
          Collections.singletonList(valueWrite)));
    }
  }

  private static class GenClinkAsValueDoFn extends DoFn<KV<String, String>, KV<ByteString, Iterable<Mutation>>> {

    ValueProvider<String> columnFamily;
    long loadMicrosTimestamp;

    GenClinkAsValueDoFn(ValueProvider<String> columnFamily, long loadMicrosTimestamp) {
      this.columnFamily = columnFamily;
      this.loadMicrosTimestamp = loadMicrosTimestamp;
    }

    @ProcessElement
    public void processElement(
        @Element KV<String, String> element,
        OutputReceiver<KV<ByteString, Iterable<Mutation>>> receiver) {
      Mutation valueWrite = Mutation.newBuilder()
          .setSetCell(SetCell.newBuilder()
              .setTimestampMicros(loadMicrosTimestamp)
              .setFamilyName(columnFamily.get())
              .setColumnQualifier(ByteString.copyFromUtf8(Objects.requireNonNull(element.getKey())))
              .setValue(ByteString.copyFromUtf8("")).build()).build();
      receiver.output(KV.of(ByteString.copyFromUtf8(element.getValue()), Collections.singletonList(valueWrite)));
    }
  }
}
