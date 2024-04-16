package org.curieo.rdf;

import org.apache.commons.text.StringEscapeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Rdf Triple
 *
 * @author DoornenbalM
 */
public interface RdfTriple {
  static final Logger LOGGER = LoggerFactory.getLogger(RdfTriple.class);

  void setSubject(String s);

  void setVerb(String v);

  void setObject(String o);

  void setLiteralObject(Literal o);

  /**
   * Set the URI of this triple. If this triple is read-only or anonymous this will throw an {@link
   * UnsupportedOperationException}.
   *
   * @param u
   */
  void setUri(String u);

  /**
   * Get the type of the object of this triple: either literal or uri
   *
   * @return either of {@code URI} or {@code LITERAL}.
   */
  boolean isLiteral();

  String getSubject();

  /**
   * Get the subject of the object of this triple: uri
   *
   * @return uri without an anchor at the end (if it contains an anchor otherwise the uri).
   */
  default String getSubjectNoAnchor() {
    if (getSubject().indexOf('#') >= 0) return getSubject().substring(0, getSubject().indexOf('#'));
    return getSubject();
  }

  String getVerb();

  String getObject();

  /**
   * Get the object as a literal. This will not be implemented for all triple types.
   *
   * @return null if not defined, a Literal otherwise
   */
  Literal getLiteralObject();

  String getUri();

  default boolean equals(RdfTriple o2) {
    // we are going to allow for anonymous triples by having their URI set to null
    // other fields *must not be null* or we throw a null pointer exception
    if (java.util.Objects.equals(this.getUri(), o2.getUri())
        && this.getSubject().equals(o2.getSubject())
        && this.getVerb().equals(o2.getVerb())
        && this.isLiteral() == o2.isLiteral()) {
      if (!this.isLiteral()) {
        return this.getObject().equals(o2.getObject());
      } else {
        return this.getLiteralObject().equals(o2.getLiteralObject());
      }
    }
    return false;
  }

  default String asRdfXml(NamespaceService ns) {
    String verb = ns.encodeUri(this.getVerb());
    if (this.isLiteral()) {
      Literal l = this.getLiteralObject();
      String datatype = l.getDataType();
      if (datatype == null) {
        datatype = "";
      } else {
        String edt = ns.encodeUri(datatype);
        if (edt != null) {
          datatype = String.format(" rdf:datatype=\"%s\"", edt);
        } else {
          datatype = String.format(" rdf:datatype=\"%s\"", datatype);
        }
      }

      String language =
          l.getLanguage() == null ? "" : String.format(" xml:lang=\"%s\"", l.getLanguage());
      if (l.getValue() == null) {
        LOGGER.error("Value cannot be null in a Literal");
      }
      return "<"
          + verb
          + datatype
          + language
          + ">"
          + StringEscapeUtils.escapeXml10(l.getValue())
          + "</"
          + verb
          + ">";
    } else {
      return String.format("<%s rdf:resource=\"%s\"/>", verb, this.getObject());
    }
  }

  default boolean isSimple() {
    return !isLiteral();
  }
}
