package com.liveramp.international.wholesale.functions;

import java.util.Arrays;
import java.util.Optional;

import com.google.bigtable.v2.Mutation;
import com.google.protobuf.ByteString;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.commons.lang.StringUtils;

import com.liveramp.international.wholesale.PelAndEmailHashes;
import com.liveramp.international.wholesale.WholesaleConstants;

public class BigtableMutationsFn extends DoFn<PelAndEmailHashes, KV<ByteString, Iterable<Mutation>>> {

  @ProcessElement
  public void processElement(ProcessContext processingContext) {
    PelAndEmailHashes pelAndEmailHashes = processingContext.element();
    if (!StringUtils.isBlank(pelAndEmailHashes.getPel())) {
      Mutation md5hashedEmailWrite = MutationGenerator(
          WholesaleConstants.MD5_COLUMN_QUALIFIER, pelAndEmailHashes.getMd5(), WholesaleConstants.COLUMN_FAMILY);
      Mutation sha1hashedEmailWrite = MutationGenerator(
          WholesaleConstants.SHA1_COLUMN_QUALIFIER, pelAndEmailHashes.getSha1(), WholesaleConstants.COLUMN_FAMILY);
      Mutation sha256hashedEmailWrite = MutationGenerator(
          WholesaleConstants.SHA256_COLUMN_QUALIFIER, pelAndEmailHashes.getSha256(), WholesaleConstants.COLUMN_FAMILY);

      processingContext
          .output(KV.of(ByteString.copyFromUtf8(pelAndEmailHashes.getPel()), Arrays.asList(md5hashedEmailWrite)));
      processingContext
          .output(KV.of(ByteString.copyFromUtf8(pelAndEmailHashes.getPel()), Arrays.asList(sha1hashedEmailWrite)));
      processingContext
          .output(KV.of(ByteString.copyFromUtf8(pelAndEmailHashes.getPel()), Arrays.asList(sha256hashedEmailWrite)));
    }
  }


  static Mutation MutationGenerator(String columnQualifier, String columnValue, String columnFamily) {
    return Mutation.newBuilder()
        .setSetCell(Mutation.SetCell.newBuilder()
            //.setTimestampMicros(0)
            .setFamilyName(columnFamily)
            .setColumnQualifier(ByteString.copyFromUtf8(columnQualifier))
            .setValue(ByteString.copyFromUtf8(Optional.ofNullable(columnValue).orElse(""))).build()).build();
  }
}
