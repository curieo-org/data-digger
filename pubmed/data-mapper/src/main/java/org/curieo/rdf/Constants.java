package org.curieo.rdf;

public class Constants {
  static final String IMMUTABLE = "This is an immutable triple.";
  static final String ANONYMOUS = "This is an anonymous triple.";

  /** Namespaces */
  public static final String RDF = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";

  public static final String RDFS = "http://www.w3.org/2000/01/rdf-schema#";
  public static final String MEMBER = RDF + "_"; // subclass of rdfs:member
  public static final String EVO_NS = "https://data.elsevier.com/schema/evo/";
  public static final String XS = "http://www.w3.org/2001/XMLSchema#";

  /** Specific predicates */
  public static final String RDFSLABEL = RDFS + "label";

  public static final String RDF_TYPE = RDF + "type";
  public static final String EVO_CATEGORY = EVO_NS + "category";
  public static final String RDF_SEQ = RDF + "Seq";

  private Constants() {}
}
