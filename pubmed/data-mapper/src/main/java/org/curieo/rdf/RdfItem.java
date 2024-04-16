package org.curieo.rdf;

import lombok.Generated;
import lombok.Value;

@Generated
@Value
class RdfItem {
  private static final RdfItem A = new RdfItem(Constants.RDF_TYPE);
  String string;
  Literal literal;

  RdfItem(String s) {
    this.string = s;
    this.literal = null;
  }

  RdfItem(Literal l) {
    this.string = null;
    this.literal = l;
  }

  boolean isLiteral() {
    return literal != null;
  }

  RdfItem expandVerb() {
    if (isLiteral()) {
      throw new UnsupportedOperationException("This is an immutable Rdf Item.");
    }

    if (string.equals("a")) {
      // https://www.w3.org/TR/turtle/#sec-parsing-triples
      return A;
    }
    return this;
  }
}
