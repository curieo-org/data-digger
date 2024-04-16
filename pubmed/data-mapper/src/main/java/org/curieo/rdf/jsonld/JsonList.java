package org.curieo.rdf.jsonld;

import java.util.List;
import java.util.stream.Collectors;
import lombok.Generated;
import lombok.Value;

@Generated
@Value
class JsonList implements JsonObject {
  private final List<JsonObject> value;

  @Override
  public Type getType() {
    return Type.LIST;
  }

  @Override
  public List<JsonObject> asList() {
    return value;
  }

  @Override
  public String toString() {
    return String.format(
        "[%s]",
        String.join(", ", value.stream().map(Object::toString).collect(Collectors.toList())));
  }
}
