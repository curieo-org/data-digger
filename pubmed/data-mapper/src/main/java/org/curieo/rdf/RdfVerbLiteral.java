package org.curieo.rdf;

import java.util.Objects;

public class RdfVerbLiteral implements ImmutableVerbObject {
  private final String verb;
  private final Literal object;

  public RdfVerbLiteral(String v, Literal o) {
    verb = v;
    object = o;
  }

  @Override
  public Literal getLiteralObject() {
    return object;
  }

  @Override
  public String getVerb() {
    return verb;
  }

  @Override
  public int hashCode() {
    return Objects.hash(verb, object);
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof RdfVerbLiteral) {
      RdfVerbLiteral that = (RdfVerbLiteral) other;
      return that.getLiteralObject().equals(object) && that.getVerb().equals(verb);
    }
    return false;
  }

  @Override
  public String getObject() {
    throw new UnsupportedOperationException("This is a literal triple.");
  }

  @Override
  public boolean isLiteral() {
    return true;
  }
}
