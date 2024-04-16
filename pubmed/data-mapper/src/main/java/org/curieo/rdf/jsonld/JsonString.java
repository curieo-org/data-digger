package org.curieo.rdf.jsonld;

import lombok.Generated;
import lombok.Value;

@Generated
@Value
class JsonString implements JsonObject {
  private final String value;

  @Override
  public Type getType() {
    return Type.STRING;
  }

  @Override
  public String asString() {
    return value;
  }

  @Override
  public String toString() {
    return quoteString(value);
  }

  static String quoteString(String key) {
    return String.format("\"%s\"", key.replace("\\", "\\\\").replace("\"", "\\\""));
  }
}
