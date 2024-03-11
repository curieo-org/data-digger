package org.curieo.embed;

import java.util.function.Function;

import org.apache.commons.collections4.CollectionUtils;
import org.curieo.model.StandardRecord;
import org.curieo.model.StandardRecordWithEmbeddings;
import lombok.Generated;
import lombok.Value;

@Generated @Value
public class SentenceEmbeddingService implements Function<StandardRecord, StandardRecordWithEmbeddings> {
	EmbeddingService embeddingsService;
	
	@Override
	public StandardRecordWithEmbeddings apply(StandardRecord sr) {
		StringBuilder text = new StringBuilder();
		if (!CollectionUtils.isEmpty(sr.getTitles())) {
			text.append(sr.getTitles().get(0).getString());
		}
		if (!CollectionUtils.isEmpty(sr.getAbstractText())) {
			text.append(' ');
			text.append(sr.getAbstractText().get(0).getString());
		}
		return StandardRecordWithEmbeddings.copy(sr, embeddingsService.embed(text.toString()));
	}
}
