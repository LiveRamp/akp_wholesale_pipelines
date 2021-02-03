package com.liveramp.dataflow.wholesale.functions;

import java.io.Serializable;

import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;

import com.liveramp.international.wholesale.functions.ClinkToEmailHashDoFn;
import com.liveramp.international.wholesale.models.ClinkEmailHashes;

public class ClinkToEmailHashDoFnTest implements Serializable {

  @Rule
  public transient final TestPipeline pipeline = TestPipeline.create();

  @Test
  public void clink_to_hash_all_present() {

    String clink = "clink";
    String md5 = "fe823d82a3f6e8175818f6da2a088878";
    String sha1 = "8d21016ba8bfabbadf7425fd14c49cd79aeced91";
    String sha256 = "c76f8d5fd5ee236b74c51126efb3671326269adb2fdd42fdaa4ba9c5dc24677d";
    PCollection<ClinkEmailHashes> line = pipeline
        .apply(Create.of(String.join("|", clink, md5, sha256, sha1)))
        .apply(ParDo.of(new ClinkToEmailHashDoFn()));
    PAssert.that(line).containsInAnyOrder(new ClinkEmailHashes(clink, md5, sha1, sha256));
    pipeline.run();
  }

}
