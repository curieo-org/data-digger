package org.curieo.model;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Locale;

import org.apache.commons.collections4.ListUtils;

public interface Record {
	static final SimpleDateFormat FORMATTER = new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH);
	List<Text> getAbstractText();

	List<Text> getTitles();

	List<String> getAuthors();

	List<Reference> getReferences();

	List<Metadata> getMetadata();

	/**
	 * @return publication date formatted as YYYY-MM-DD
	 */
	String getPublicationDate();

	/**
	 * This identifier must be unique *across* sources
	 */
	String getIdentifier();
	List<Metadata> getIdentifiers();

	default List<LinkedField<Authorship>> toAuthorships() {
		throw new UnsupportedOperationException("Not defined for standard records.");
	}

	default Integer getYear() {
		if (getPublicationDate() == null) {
			return null;
		}
		return Integer.parseInt(getPublicationDate().substring(0, 4));
	}

	default List<LinkedField<Reference>> toReferences() {
		List<LinkedField<Reference>> list = new ArrayList<>();
		
		for (int ordinal = 0; ordinal < ListUtils.emptyIfNull(getReferences()).size(); ordinal++) {
			list.add(new LinkedField<>(ordinal, getIdentifier(), getReferences().get(ordinal)));
		}
		return list;
	}
	
	static String formatDate(Date date) {
		return FORMATTER.format(date);
	}
}