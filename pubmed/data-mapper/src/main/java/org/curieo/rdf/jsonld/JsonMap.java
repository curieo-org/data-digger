package org.curieo.rdf.jsonld;

import java.util.Map;
import java.util.stream.Collectors;

import lombok.Generated;
import lombok.Value;

@Generated @Value
class JsonMap implements JsonObject {
	private final Map<String, JsonObject> value;
	
	@Override
	public Type getType() {
		return Type.MAP;
	}
	@Override
	public Map<String, JsonObject> asMap() {
		return value;
	}
	
	@Override
	public String toString() {
		return String.format("{ %s }", String.join(", ", value.entrySet().stream()
													.map(e -> 
														String.format("%s : %s", JsonString.quoteString(e.getKey()), e.getValue()))
													.collect(Collectors.toList())));
	}
}
