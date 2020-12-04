package com.liveramp.dataflow.arlpel;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.junit.Rule;
import org.junit.Test;

import com.liveramp.abilitec.generated.Arl;
import com.liveramp.abilitec.generated.PEL;
import com.liveramp.translation_zone_hashing.PINToARLTranslator;
import com.liveramp.types.custom_id.CustomId;
import com.rapleaf.types.new_person_data.AbiliTecId;
import com.rapleaf.types.new_person_data.HashedEmailPIN;
import com.rapleaf.types.new_person_data.PIN;

public class ConvertRawToArlPelTest implements Serializable {

  @Rule
  public transient TestPipeline p = TestPipeline.create();

  @Test
  public void testGenHashedEmailClinkMapping() throws Exception {

    Function<AbiliTecId, PEL> clinkToMaintedPelFn = (Function<AbiliTecId, PEL> & Serializable) (clink) -> {
      return new PEL(PEL._Fields.MAINTAINED_INDIVIDUAL_PEL, new CustomId(10936, clink.get_id()));
    };

    PINToARLTranslator pinToArlTranslator = new MockPinToArlTranslator();
    PCollection<KV<String, String>> arlPelMapping =
        ConvertRawToArlPel.generateArlPelMapping(
            p.apply(TextIO.read().from("test/resources/raw.csv")),
            ValueProvider.StaticValueProvider.of(0),
            ValueProvider.StaticValueProvider.of(2),
            ValueProvider.StaticValueProvider.of(1),
            ValueProvider.StaticValueProvider.of(3),
            pinToArlTranslator,
            (BiFunction<DoFn.ProcessContext, PCollectionView<Map<String, String>>, Function<AbiliTecId, PEL>> & Serializable)
                (ctx, view) -> clinkToMaintedPelFn);

    PAssert.that(arlPelMapping.apply(Count.globally())).containsInAnyOrder(7l);
    PAssert.that(arlPelMapping
        .apply(ParDo.of(new DoFn<KV<String, String>, String>() {
          @ProcessElement
          public void processElement(
              @Element KV<String, String> element,
              OutputReceiver<String> receiver) {
            receiver.output(element.getValue());
          }
        }))
        .apply(Count.perElement())
    ).containsInAnyOrder(
        KV.of("074657374406c69766572616d702e636f6d74657374406c69766572616d702e6", 2l),
        KV.of("76d61696c2e636f6d626f6e6f626f476d61696c2e636f6d626f6e6f626f40015", 3l),
        KV.of("d61696c2e636f6d626f6e6f6d61696c2e636f6d626f6e6f6d61696c2e636f6d6", 1l),
        KV.of("861696c2e636f6d626f6e6f6d61696c2e636f6d626f6e6f6d61696c2e636f6d6", 1l));

    p.run().waitUntilFinish();
  }

  static class MockPinToArlTranslator implements PINToARLTranslator {

    @Nullable
    @Override
    public Arl apply(@Nullable PIN pin) {
      HashedEmailPIN hashedEmail = pin.get_hashed_email();
      if (hashedEmail.is_set_sha256()) {
        return new Arl(ByteBuffer.wrap(hashedEmail.get_sha256()));
      } else if (hashedEmail.is_set_md5()) {
        return new Arl(ByteBuffer.wrap(hashedEmail.get_md5()));
      } else if (hashedEmail.is_set_sha1()) {
        return new Arl(ByteBuffer.wrap(hashedEmail.get_sha1()));
      } else {
        throw new RuntimeException("Unknown hash type");
      }
    }
  }

}
