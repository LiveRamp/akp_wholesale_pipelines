package com.liveramp.international.wholesale;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ParDo;

import com.liveramp.ingestion.secret.EuSecretGroups;
import com.liveramp.international.wholesale.functions.BigtableMutationsFn;
import com.liveramp.international.wholesale.functions.ClinkToEmailHashDoFn;
import com.liveramp.international.wholesale.functions.GeneratePelToEmailHashDoFn;

public class FRWholesaleLoader {

  public static void main(String[] args) {
    EuWholesaleMatchTableLoaderOptions options = PipelineOptionsFactory.fromArgs(args)
        .as(EuWholesaleMatchTableLoaderOptions.class);
    Date date = new Date(System.currentTimeMillis());
    options.setJobName("FRWholesale" + System.currentTimeMillis());
    Pipeline pipeline = Pipeline.create(options);
    Map<String, String> secrets = EuSecretGroups.buildSecretMap(EuSecretGroups.TRANSLATOR_SECRETS);
    pipeline.apply("ReadLines", TextIO.read()
        .from(options.getSha256Inputpath()))
        .apply("ParseToClinkEmailHashes", ParDo.of(new ClinkToEmailHashDoFn()))
        .apply("GeneratePelAndEmailHashes", ParDo.of(new GeneratePelToEmailHashDoFn(secrets)))
        .apply("BigtableMutation", ParDo.of(new BigtableMutationsFn()))
        .apply(
            BigtableIO.write()
                .withProjectId(WholesaleConstants.PROJECT_ID)
                .withInstanceId(WholesaleConstants.INSTANCE_ID)
                .withTableId(WholesaleConstants.TABLE_ID));

    pipeline.run();
  }
}
