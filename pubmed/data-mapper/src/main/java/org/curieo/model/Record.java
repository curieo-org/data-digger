package org.curieo.model;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import org.apache.commons.collections4.ListUtils;

public interface Record {
	List<Text> getAbstractText();

	List<Text> getTitles();

	List<String> getAuthors();

	List<Reference> getReferences();

	List<Metadata> getMetadata();

	Date getPublicationDate();

	/**
	 * This identifier must be unique *across* sources
	 */
	String getIdentifier();

	default List<LinkedField<Authorship>> toAuthorships() {
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

	default List<LinkedField<Reference>> toReferences() {
		List<LinkedField<Reference>> list = new ArrayList<>();
		
		for (int ordinal = 0; ordinal < ListUtils.emptyIfNull(getReferences()).size(); ordinal++) {
			list.add(new LinkedField<>(ordinal, getIdentifier(), getReferences().get(ordinal)));
		}
		return list;
	}
}