package org.curieo.model;

import java.util.List;

import lombok.Builder;
import lombok.Data;
import lombok.Generated;
import lombok.Singular;

@Generated @Data @Builder
public class Reference {
	String citation;
	@Singular
	List<Metadata> identifiers;
}
