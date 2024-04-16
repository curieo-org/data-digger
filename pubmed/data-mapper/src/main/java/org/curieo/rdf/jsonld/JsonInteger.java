package org.curieo.rdf.jsonld;

import lombok.Generated;
import lombok.Value;

@Generated
@Value
class JsonInteger implements JsonObject {
  private final Integer value;

  @Override
  public Type getType() {
    return Type.INTEGER;
  }

  @Override
  public Integer asInteger() {
    return value;
  }

  @Override
  public String asString() {
    return Integer.toString(value);
  }

  @Override
  public String toString() {
    return asString();
  }
}
