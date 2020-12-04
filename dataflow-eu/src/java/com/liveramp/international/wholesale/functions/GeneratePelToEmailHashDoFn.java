package com.liveramp.international.wholesale.functions;

import java.util.Map;

import org.apache.beam.sdk.transforms.DoFn;

import com.liveramp.ingestion.abilitec.ProductionEuTranslators;
import com.liveramp.international.wholesale.PelAndEmailHashes;
import com.liveramp.international.wholesale.models.ClinkEmailHashes;
import com.rapleaf.types.new_person_data.AbiliTecId;

public class GeneratePelToEmailHashDoFn extends DoFn<ClinkEmailHashes, PelAndEmailHashes> {

  private Map<String, String> secrets;

  public GeneratePelToEmailHashDoFn(Map<String, String> secrets) {
    this.secrets = secrets;
  }

  @ProcessElement
  public void processElement(ProcessContext processingContext) {
    ClinkEmailHashes emailHashes = processingContext.element();
    System.out.println(emailHashes);
    PelAndEmailHashes pelAndEmailHashes = new PelAndEmailHashes(
        ProductionEuTranslators.getClinkToMaintainedIndividualPelTranslator(secrets)
            .apply(new AbiliTecId(emailHashes.getKey())).get_maintained_individual_pel().get_partner_id(),
        emailHashes.getMd5(),
        emailHashes.getSha1(),
        emailHashes.getSha256());
    processingContext.output(pelAndEmailHashes);
  }
}
