package com.liveramp.dataflow.akp.steps;

import java.util.function.Supplier;

import com.google.cloud.bigtable.beam.CloudBigtableIO;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import com.liveramp.dataflow.akp.AkpLoadingOptions;
import com.liveramp.dataflow.common.AKPHelper;
import com.liveramp.translation_zone_hashing.CustomIdToArlTranslator;

public class BigTableInsertFlow {

  public static void insert(
      Supplier<CustomIdToArlTranslator> arlTranslatorSupplier, Pipeline pipeline,
      PCollection<KV<String, String>> processData, AkpLoadingOptions options) {
    processData.apply("Generate CID Mutation", ParDo.of(new WriteToArlDiffTableFn(options.getAnaId())))
        .apply("Write To CID BigTable", CloudBigtableIO.writeToTable(AKPHelper.getArlDiffBigtableConfig(options)));
    processData
        .apply("Generate ARL Mutation", ParDo.of(new WriteToArlToPelTableFn(arlTranslatorSupplier, options.getAnaId())))
        .apply("Write To ARL BigTable", CloudBigtableIO.writeToTable(AKPHelper.getArlPelBigtableConfig(options)));
    pipeline.run();
  }
}
