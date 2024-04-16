package org.curieo.rdf;

import java.util.Objects;

class RdfVerbObject implements ImmutableVerbObject {
  private final String verb;
  private final String object;

  RdfVerbObject(String v, String o) {
    verb = v;
    object = o;
  }

  @Override
  public String getObject() {
    return object;
  }

  @Override
  public boolean isLiteral() {
    return false;
  }

  @Override
  public Literal getLiteralObject() {
    throw new UnsupportedOperationException("This is an object triple.");
  }

  @Override
  public String getVerb() {
    return verb;
  }

  public int hashCode() {
    return Objects.hash(verb, object);
  }

  public boolean equals(Object other) {
    if (other instanceof RdfVerbObject) {
      RdfVerbObject that = (RdfVerbObject) other;
      return that.getObject().equals(object) && that.getVerb().equals(verb);
    }
    return false;
  }
}
