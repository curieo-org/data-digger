package org.curieo.rdf.jsonld;

import lombok.Generated;
import lombok.Value;

@Generated @Value
class JsonFloat implements JsonObject {
	private final Double value;

	@Override
	public Type getType() {
		return Type.LIST;
	}
	
	@Override
	public Double asFloat() {
		return value;
	}
	
	@Override
	public String asString() {
		return Double.toString(value);
	}

	@Override
	public String toString() {
		return asString();
	}
}
