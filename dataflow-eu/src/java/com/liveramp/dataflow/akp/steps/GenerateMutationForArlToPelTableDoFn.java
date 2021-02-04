package com.liveramp.dataflow.akp.steps;

import com.liveramp.dataflow.akp.steps.setup.ArlTranslatorSupplier;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.util.Bytes;

import com.liveramp.abilitec.generated.Arl;
import com.liveramp.translation_zone_hashing.CustomIdToArlTranslator;
import com.liveramp.types.custom_id.CustomId;

public class GenerateMutationForArlToPelTableDoFn extends DoFn<byte[], Mutation> {

  private final ArlTranslatorSupplier arlTranslatorSupplier;

  private CustomIdToArlTranslator translator;

  public GenerateMutationForArlToPelTableDoFn(ArlTranslatorSupplier arlTranslatorSupplier) {
    this.arlTranslatorSupplier = arlTranslatorSupplier;
  }

  @Setup
  public void setup() {
    translator = arlTranslatorSupplier.get();
  }

  @ProcessElement
  public void processElement(ProcessContext c) {
    String rowKey = Bytes.toString(c.element());
    String[] rowKeyArray = rowKey.split("#");
    if (rowKeyArray.length == 2) {
      Arl arl = translator.apply(new CustomId(Integer.parseInt(rowKeyArray[0]), rowKeyArray[1]));
      Delete deletionForInterface = new Delete(arl.get_arl());
      c.output(deletionForInterface);
    }
  }


}
