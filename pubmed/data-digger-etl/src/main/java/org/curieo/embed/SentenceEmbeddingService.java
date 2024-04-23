package org.curieo.embed;

import java.util.function.Function;
import lombok.Generated;
import org.apache.commons.collections4.CollectionUtils;
import org.curieo.model.StandardRecord;
import org.curieo.model.StandardRecordWithEmbeddings;

@Generated
public record SentenceEmbeddingService(EmbeddingService embeddingsService)
    implements Function<StandardRecord, StandardRecordWithEmbeddings> {
  @Override
  public StandardRecordWithEmbeddings apply(StandardRecord sr) {
    StringBuilder text = new StringBuilder();
    if (!CollectionUtils.isEmpty(sr.getTitles())) {
      text.append(sr.getTitles().getFirst().getString());
    }
    if (!CollectionUtils.isEmpty(sr.getAbstractText())) {
      text.append(' ');
      text.append(sr.getAbstractText().getFirst().getString());
    }
    return StandardRecordWithEmbeddings.copy(sr, embeddingsService.embed(text.toString()));
  }
}
