package org.curieo.model;

import java.text.ParseException;
import java.util.Date;
import java.util.List;

import lombok.Builder;
import lombok.Data;
import lombok.Generated;

/**
 * This is the standardized record as we file it in third party stores (e.g. Elastic Search.)
 * Use this record, because JSON serialization works on actual fields and not interfaces.
 * (I am sure that this can be tweaked but this seems a straightforward way).
 */
@Generated @Data @Builder
public class StandardRecordWithEmbeddings implements Record {
	List<Text> abstractText;
	List<Text> titles;
	List<String> authors;
	List<Reference> references;
	List<Metadata> metadata;
	String identifier;
	String publicationDate;
	double[] embeddings;
	
	public static StandardRecordWithEmbeddings copy(Record record, double[] embeddings) {
		return new StandardRecordWithEmbeddings.StandardRecordWithEmbeddingsBuilder()
				.abstractText(record.getAbstractText())
				.titles(record.getTitles())
				.authors(record.getAuthors())
				.metadata(record.getMetadata())
				.publicationDate(StandardRecord.formatDate(record.getPublicationDate()))
				.references(record.getReferences())
				.identifier(record.getIdentifier())
				.embeddings(embeddings)
				.build();
	}

	@Override
	public Date getPublicationDate() {
		if (publicationDate == null) {
			return null;
		}
		try {
			return StandardRecord.FORMATTER.parse(publicationDate);
		} catch (ParseException e) {
			throw new RuntimeException(e);
		}
	}
}
