package org.curieo.rdf;

public interface ImmutableVerbObject {
  String getVerb();

  String getObject();

  /**
   * Get the object as a literal. This will not be implemented for all triple types.
   *
   * @return null if not defined, a Literal otherwise
   */
  Literal getLiteralObject();

  boolean isLiteral();
}
