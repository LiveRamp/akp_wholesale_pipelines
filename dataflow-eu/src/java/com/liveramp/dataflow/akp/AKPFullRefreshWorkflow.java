package com.liveramp.dataflow.akp;

import com.google.cloud.bigtable.beam.CloudBigtableIO;
import com.liveramp.dataflow.common.SecretManagerProvider;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Wait;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import com.liveramp.dataflow.akp.steps.BigTableInsertFlow;
import com.liveramp.dataflow.akp.steps.GenerateMutationForArlDiffTableDoFn;
import com.liveramp.dataflow.akp.steps.GenerateMutationForArlToPelTableDoFn;
import com.liveramp.dataflow.akp.steps.ParseAkpLineFn;
import com.liveramp.dataflow.akp.steps.ScanPrefixDoFn;
import com.liveramp.dataflow.akp.steps.setup.ArlTranslatorSupplier;
import com.liveramp.dataflow.common.AKPHelper;

public class AKPFullRefreshWorkflow {
    private static final SecretManagerProvider secretProvider = SecretManagerProvider.production();
    private static final ArlTranslatorSupplier arlTranslatorSupplier = new ArlTranslatorSupplier(secretProvider);

    public static void main(String[] args) {

        AkpLoadingOptions options = PipelineOptionsFactory.fromArgs(args)
                .withValidation()
                .as(AkpLoadingOptions.class);
        Pipeline pipeline = Pipeline.create(options);

        //Scan with prefix(ANA)
        PCollection<byte[]> rowKey = pipeline.apply(Create.of("Start"))
                .apply("Scan Prefix", ParDo.of(new ScanPrefixDoFn(options.getAnaId(), AKPHelper.getArlDiffBigtableConfig(options))));
        //Delete from interface table
        rowKey.apply("Generate ARL Mutation", ParDo.of(new GenerateMutationForArlToPelTableDoFn(arlTranslatorSupplier)))
                .apply("Delete ARL Row", CloudBigtableIO.writeToTable(AKPHelper.getArlPelBigtableConfig(options)));
        //Delete from cid table
        rowKey.apply("Generate CID Mutation", ParDo.of(new GenerateMutationForArlDiffTableDoFn()))
                .apply("Delete CID Row", CloudBigtableIO.writeToTable(AKPHelper.getArlDiffBigtableConfig(options)));

        PCollection<String> lines = pipeline.apply("Read Lines", TextIO.read().from(options.getInputFile()));
        PCollection<KV<String, String>> processData = lines.apply("Wait Delete", Wait.on(rowKey))
                .apply(
                        "File Filter",
                        ParDo.of(new ParseAkpLineFn(options.getCidKey(), options.getPreferredPelKey(), options.getInputFile())));
        BigTableInsertFlow.insert(arlTranslatorSupplier, pipeline, processData, options);
    }
}
