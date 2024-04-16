package org.curieo.rdf;

/**
 * Rdf Triple
 *
 * @author DoornenbalM
 */
public class RdfLiteralTriple extends RdfAnonymousLiteralTriple implements RdfTriple {
  private String uri;

  public RdfLiteralTriple(String u, String s, String v, Literal o) {
    super(s, v, o);
    this.uri = u;
  }

  @Override
  public void setUri(String u) {
    this.uri = u;
  }

  @Override
  public String getUri() {
    return this.uri;
  }

  /**
   * Implementations of RdfTriple *must* override hashcode() and equals().
   *
   * @return
   */
  @Override
  public int hashCode() {
    return java.util.Objects.hash(uri, getSubject(), getVerb(), getLiteralObject());
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof RdfTriple) {
      return super.equals((RdfTriple) o);
    }
    return false;
  }
}
