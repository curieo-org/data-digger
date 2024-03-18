package org.curieo.model;

import java.util.Calendar;
import java.util.Date;
import java.util.List;

public interface Record {
	List<Text> getAbstractText();

	List<Text> getTitles();

	List<String> getAuthors();

	List<String> getReferences();

	List<Metadata> getMetadata();

	Date getPublicationDate();

	/**
	 * This identifier must be unique *across* sources
	 */
	String getIdentifier();

	default List<Authorship> toAuthorships() {
		throw new UnsupportedOperationException("Not defined for standard records.");
	}

	default Integer getYear() {
		if (getPublicationDate() == null) {
			return null;
		}
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(getPublicationDate());
		return calendar.get(Calendar.YEAR);
	}
}