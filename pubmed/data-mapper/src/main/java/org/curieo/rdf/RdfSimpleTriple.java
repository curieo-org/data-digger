package org.curieo.rdf;

/**
 * Rdf Triple
 *
 * @author DoornenbalM
 */
class RdfSimpleTriple extends RdfAnonymousTriple implements RdfTriple {
  private String uri;

  RdfSimpleTriple(String u, String s, String v, String o) {
    super(s, v, o);
    uri = u;
  }

  @Override
  public String getUri() {
    return uri;
  }

  @Override
  public void setUri(String uri) {
    this.uri = uri;
  }

  /**
   * Implementations of RdfTriple *must* override hashcode() and equals().
   *
   * @return
   */
  @Override
  public int hashCode() {
    return java.util.Objects.hash(uri, getSubject(), getVerb(), getObject());
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof RdfTriple) {
      return this.equals((RdfTriple) o);
    }
    return false;
  }

  /**
   * toString() outputs simplified turtle.
   *
   * @return turtle representation of this triple.
   */
  @Override
  public String toString() {
    return String.format("%s %s %s .", getSubject(), getVerb(), getObject());
  }
}
