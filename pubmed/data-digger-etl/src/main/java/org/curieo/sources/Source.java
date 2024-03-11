package org.curieo.sources;

import java.util.List;

import org.curieo.model.Metadata;

import lombok.Builder;
import lombok.Generated;
import lombok.Value;

@Generated @Value @Builder
public class Source {
	String type;
	List<Metadata> identifiers;
}
