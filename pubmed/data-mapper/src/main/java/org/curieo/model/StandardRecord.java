package org.curieo.model;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Locale;

import lombok.Builder;
import lombok.Data;
import lombok.Generated;

/**
 * This is the standardized record as we file it in third party stores (e.g. Elastic Search.)
 * Use this record, because JSON serialization works on actual fields and not interfaces.
 * (I am sure that this can be tweaked but this seems a straightforward way).
 */
@Generated @Data @Builder
public class StandardRecord implements Record {
	static final SimpleDateFormat FORMATTER = new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH);
	List<Text> abstractText;
	List<Text> titles;
	List<String> authors;
	List<String> references;
	List<Metadata> metadata;
	String identifier;
	String publicationDate;
	
	public static StandardRecord copy(Record record) {
		return new StandardRecord.StandardRecordBuilder()
				.abstractText(record.getAbstractText())
				.titles(record.getTitles())
				.authors(record.getAuthors())
				.metadata(record.getMetadata())
				.publicationDate(record.getPublicationDate() == null ? null : formatDate(record.getPublicationDate()))
				.references(record.getReferences())
				.identifier(record.getIdentifier())
				.build();
	}
	
	@Override
	public Date getPublicationDate() {
		if (publicationDate == null) {
			return null;
		}
		try {
			return FORMATTER.parse(publicationDate);
		} catch (ParseException e) {
			throw new RuntimeException(e);
		}
	}
	
	public static String formatDate(Date date) {
		return FORMATTER.format(date);
	}
}
