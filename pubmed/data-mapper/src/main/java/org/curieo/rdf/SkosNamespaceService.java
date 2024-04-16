package org.curieo.rdf;

public class SkosNamespaceService {
  public static final String SKOS = "http://www.w3.org/2004/02/skos/core#";
  public static final String SKOSXL = "http://www.w3.org/2008/05/skos-xl#";
  public static final String DCT = "http://purl.org/dc/terms/";
  public static final String SKSME = "http://synaptica.net/skm/subElement/";
  public static final String EMTREE = "http://data.elsevier.com/vocabulary/Emtree/sample";
  public static final String PROP = "http://data.elsevier.com/properties#";
  public static final String EGV = "http://www.elsevier.com/xml/schema/rdf/EGVSatelliteTagging-1/";
  public static final String EGVS =
      "http://www.elsevier.com/xml/schema/rdf/ElsevierGenericVocabularySatellite-1/";
  public static final String SVF = "http://www.elsevier.com/xml/schema/grant/grant-1.2/";
  public static final String SEMREL = "http://data.elsevier.com/EMMeT/SemRelations/";
  public static final String ELS = "https://data.elsevier.com/vocabulary/schema/";
  public static final String GN = "http://www.geonames.org/ontology#";
  public static final String PROV = "http://www.w3.org/ns/prov#";
  public static final String SCHEMA = "https://none.schema.org/";
  public static final String ISOTHES =
      "http://purl.org/iso25964/skos-thes#"; // , which occurred 515 times.
  public static final String ZTHES = "http://synaptica.net/zthes/";
  public static final String ROS = "https://data.elsevier.com/research/omniscience/schema/";
  public static final String EVO = "https://data.elsevier.com/schema/evo/";
  public static final String KNOV =
      "https://data.elsevier.com/engineering/knovel/properties/schema/";
  public static final String OM = "http://www.ontology-of-units-of-measure.org/resource/om-2/";
  public static final String KUSCH = "https://data.elsevier.com/schema/unitsOfMeasure/";

  public static final String REPLACES = DCT + "replaces";

  // https://www.w3.org/TR/skos-reference/#xl-labels
  public static final String ALTLABEL = SKOSXL + "altLabel";
  public static final String PREFLABEL = SKOSXL + "prefLabel";
  public static final String HIDDENLABEL = SKOSXL + "hiddenLabel";

  public static final String[] SKOS_STANDARD_LABELTYPES =
      new String[] {ALTLABEL, PREFLABEL, HIDDENLABEL};
  static final String SKOSXL_LABEL = SKOSXL + "Label";
  public static final String SKOSXL_LITERALFORM = SKOSXL + "literalForm";

  public static final String SKOS_SCOPENOTE = SKOS + "scopeNote";
  public static final String SKOS_CONCEPT = SKOS + "Concept";
  public static final String SKOS_NOTATION = SKOS + "notation";
  public static final String SKOS_IN_SCHEME = SKOS + "inScheme";
  public static final String SKOS_CONCEPT_SCHEME = SKOS + "ConceptScheme";

  public static final String NARROWER = SKOS + "narrower";
  public static final String BROADER = SKOS + "broader";
  // https://www.w3.org/TR/skos-primer/
  public static final String IS_ACRONYM_OF = EGVS + "isAcronymOf";

  private static final ImmutableNamespaceService SHARED_NAMESPACE_SERVICE = getNamespaceService();

  /** There is no instance */
  private SkosNamespaceService() {}

  /**
   * Get a namespace service. This is a _modifiable_ object so each time a copy is asked for, create
   * a new one.
   *
   * @return a namespace service - modifiable
   */
  public static NamespaceService getNamespaceService() {
    NamespaceServiceImpl namespaces = new NamespaceServiceImpl();
    namespaces.put("skos", SKOS);
    namespaces.put("skosxl", SKOSXL);
    namespaces.put("rdf", Constants.RDF);
    namespaces.put("rdfs", Constants.RDFS);
    namespaces.put("emtree", EMTREE);
    namespaces.put("dct", DCT);
    namespaces.put("els", ELS);
    namespaces.put("sksme", SKSME);
    namespaces.put("prop", PROP);
    namespaces.put("prov", PROV);
    namespaces.put("egv", EGV);
    namespaces.put("egvs", EGVS);
    namespaces.put("semrel", SEMREL);
    namespaces.put("om", OM);
    namespaces.put("schema", SCHEMA);
    namespaces.put("svf", SVF);
    namespaces.put("gn", GN);
    namespaces.put("zthes", ZTHES);
    namespaces.put("isothes", ISOTHES);
    namespaces.put("evo", EVO);
    namespaces.put("ros", ROS);
    namespaces.put("kusch", KUSCH);
    namespaces.put("knov", KNOV);
    return namespaces;
  }

  public static ImmutableNamespaceService getSharedNamespaceService() {
    return SHARED_NAMESPACE_SERVICE;
  }

  /**
   * Create a term. Do not that adding this term *while* you're reading the RDF is probably not
   * going to work Iterating over the store while writing to it violates some sanity checks
   *
   * @param rdf
   * @param conceptUri
   * @param labelUri
   * @param labelText
   * @param prefLabel
   * @return the created label
   */
  public static Literal createTerm(
      Store rdf, String conceptUri, String labelUri, String labelText, boolean prefLabel) {
    return createTerm(rdf, conceptUri, labelUri, labelText, "en", prefLabel);
  }

  public static Literal createTerm(
      Store rdf,
      String conceptUri,
      String labelUri,
      String labelText,
      String languageId,
      boolean prefLabel) {
    rdf.assertTriple(conceptUri, Constants.RDF_TYPE, SKOS_CONCEPT);
    rdf.assertTriple(conceptUri, prefLabel ? PREFLABEL : ALTLABEL, labelUri);
    Literal label = new Literal(labelText, languageId, null);
    rdf.assertTriple(labelUri, SKOSXL_LITERALFORM, label);
    return label;
  }

  /**
   * Create an RDF store that is optimized (a bit) for Skos
   *
   * @return instantiated, empty store.
   */
  public static Store createStore() {
    return new Store(getNamespaceService())
        .withVerbImplyingImmutability(Constants.RDF_TYPE)
        .withVerbImplyingImmutability(SKOS_IN_SCHEME)
        .withVerbImplyingImmutability(DCT + "source")
        .withVerbImplyingImmutability(EGVS + "usageFlag");
  }
}
