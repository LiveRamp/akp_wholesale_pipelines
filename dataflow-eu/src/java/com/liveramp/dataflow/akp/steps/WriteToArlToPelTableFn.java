package com.liveramp.dataflow.akp.steps;

import java.util.function.Supplier;

import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;

import com.liveramp.abilitec.generated.Arl;
import com.liveramp.dataflow.common.AKPHelper;
import com.liveramp.translation_zone_hashing.CustomIdToArlTranslator;
import com.liveramp.types.custom_id.CustomId;

public class WriteToArlToPelTableFn extends DoFn<KV<String, String>, Mutation> {

  private Supplier<CustomIdToArlTranslator> arlTranslatorSupplier;
  private ValueProvider<String> Ana;

  private CustomIdToArlTranslator translator;

  public WriteToArlToPelTableFn(Supplier<CustomIdToArlTranslator> arlTranslatorSupplier, ValueProvider<String> ana) {
    this.arlTranslatorSupplier = arlTranslatorSupplier;
    Ana = ana;
  }

  @Setup
  public void setup() {
    translator = arlTranslatorSupplier.get();
  }

  @ProcessElement
  public void processElement(ProcessContext c) {
    Arl arl = translator.apply(new CustomId(Integer.parseInt(Ana.get()), c.element().getKey()));
    c.output(new Put(arl.get_arl()).addColumn(AKPHelper.COLUMN_FAMILY.getBytes(), AKPHelper.COLUMN_QUALIFIER.getBytes(),
        c.element().getValue().getBytes()));
  }
}
