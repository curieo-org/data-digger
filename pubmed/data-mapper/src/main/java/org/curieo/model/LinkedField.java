package org.curieo.model;

import lombok.Generated;
import lombok.Value;

/**
 * For extracting multiple fields from a single record
 * we wrap the fields and link them to the original through the 
 * publication id
 * @param <T> field to be wrapped
 */
@Generated @Value
public class LinkedField<T> {
	int ordinal;
	String publicationId;
	T field;
}
