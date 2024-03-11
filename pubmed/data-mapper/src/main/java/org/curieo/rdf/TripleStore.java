package org.curieo.rdf;

/**
 * Interface defining the functionality to stuff triples into some store.
 * Retrieval methods not defined here
 * 
 * @author doornenbalm
 */
public interface TripleStore {
	RdfTriple assertTriple(String uri, String subject, String verb, String object);
	RdfTriple assertTriple(String uri, String subject, String verb, Literal object);
	RdfTriple accept(RdfTriple triple);

	/**
	 * Assert a triple.
	 * If an unnamed version of the triple already exists, _name_ it.
	 * If a named version of the triple already exists, with another name, let them co-exist
	 * 
	 * @param uri
	 * @param subject
	 * @param verb
	 * @param object
	 * @return the asserted triple
	 */
	RdfTriple assertAndNameTriple(String uri, String subject, String verb, String object);
	
	default RdfTriple assertTriple(String subject, String verb, String object) {
		return assertTriple(null, subject, verb, object);
	}
	
	/**
	 * Assert a sequence in RDF (https://ontola.io/blog/ordered-data-in-rdf/)
	 * @param listIdentifier
	 * @param members
	 * @return the number of members.
	 */
	default int assertSeq(String listIdentifier, Iterable<String> members) {
		int i = 0;
		assertTriple(listIdentifier, Constants.RDF_TYPE, Constants.RDF_SEQ);
		for (String member : members) {
			String membership = String.format("%s%d", Constants.MEMBER, ++i);
			assertTriple(listIdentifier, membership, member);
		}
		return i;
	}
	
	default RdfTriple assertTriple(String subject, String verb, Literal object) {
		return assertTriple(null, subject, verb, object);
	}
}
