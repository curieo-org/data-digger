package org.curieo.model;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.collections4.ListUtils;
import org.apache.commons.lang3.StringUtils;

import lombok.Builder;
import lombok.Data;
import lombok.Generated;
import lombok.Singular;

/**
 * An AuthorShip is a recorded instance of a person with a name as an author 
 * of a publication.
 */
@Generated @Data @Builder
public class Authorship {
	// position in sequence of authors of publication
	int ordinal; 
	String foreName;
	String lastName;
	String initials;
	
	@Singular
	List<String> affiliations;
	
	int yearActive;
	String emailAddress;
	String publicationId;
	
	public Authorship copy() {
		Authorship authorship = Authorship.builder()
				.ordinal(ordinal)
				.foreName(foreName)
				.initials(initials)
				.lastName(lastName)
				.yearActive(yearActive)
				.emailAddress(emailAddress)
				.publicationId(publicationId)
				.build();
		authorship.setAffiliations(affiliations == null ? null : new ArrayList<>(affiliations));
		return authorship;
	}
	
	@Override
	public String toString() {
		return String.format("[%d] %s (%s) %s; [%d] %s %s\n  %s",
					ordinal, StringUtils.defaultIfEmpty(foreName, ""),
					StringUtils.defaultIfEmpty(initials, ""),
					StringUtils.defaultIfEmpty(lastName, ""),
					yearActive,
					StringUtils.defaultIfEmpty(emailAddress, ""),
					StringUtils.defaultIfEmpty(publicationId, ""),
					String.join(";\n  ", ListUtils.emptyIfNull(affiliations))
					);
	}
}
