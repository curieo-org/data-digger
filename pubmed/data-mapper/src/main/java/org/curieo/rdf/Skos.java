package org.curieo.rdf;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;


import static org.curieo.rdf.SkosNamespaceService.*;
import static java.util.Arrays.asList;

import org.apache.commons.collections4.IterableUtils;
import org.apache.commons.collections4.SetUtils;
import org.apache.commons.text.StringEscapeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Skos Thesaurus
 * Essentially, a wrapper around an RDF store with utility methods that
 * 'know' the semantics of Skos RDF verbs.
 */
public class Skos {
	/*
	private static final Logger LOGGER = LoggerFactory.getLogger(Skos.class);
	private static final Map<String, String> KNOWN_TYPES;
	public static final int RECURSION_LIMIT = 50;

	public static final String[] PREFLABEL_VERBS = new String[] { SKOS + "prefLabel", PREFLABEL };
	public static final String[] ALTLABEL_VERBS  = new String[] { SKOS + "altLabel", ALTLABEL };
	
	private final Set<String> prefixes = new HashSet<>();

	private final RdfStore store;
	private String xmlbase = null;
	
	private boolean owl;

	public static final int MAXIMUM_ERROR_REPORT = 10;

	static {
		KNOWN_TYPES = new HashMap<>();
		KNOWN_TYPES.put(ALTLABEL, SkosNamespaceService.SKOSXL_LABEL);
		KNOWN_TYPES.put(PREFLABEL, SkosNamespaceService.SKOSXL_LABEL);
	}
	
	
	static final Set<String> EMBEDDED_TYPES = SetUtils.unmodifiableSet(Constants.RDF_SEQ, SkosNamespaceService.SKOSXL_LABEL);

	static final Collection<String> TYPE = Collections.singleton(Constants.RDF_TYPE);

	private static final Collection<String> IGNORE_ON_RECURSION = asList(Constants.RDF_TYPE, EGV + "derivedFrom");

	private String getType(String object) {
		return store.getBySubjectAndVerb(object, Constants.RDF_TYPE).findFirst()
				.filter(RdfTriple::isSimple).map(RdfTriple::getObject).orElse(null);
	}

	private void writeTriple(RdfTriple triple, WriteContext pr, String indent) {
		writeTriple(triple, pr, indent, new ArrayDeque<>());
	}
	private void writeTriple(RdfTriple triple, WriteContext pr, String indent, Deque<String> seenAlready) {
		String prefix = store.getNamespaceService().getPrefix(triple.getVerb());
		
		if (prefix != null) {
			String type = triple.isLiteral() ? null : getType(triple.getObject());
			if (type == null) {
				type = KNOWN_TYPES.get(triple.getVerb());
			}
			if (EMBEDDED_TYPES.contains(type)) {
				type = store.getNamespaceService().encodeUri(type);
				String verb = store.getNamespaceService().encodeUri(triple.getVerb());
				pr.printf("%s<%s>%n", indent, verb);
				String object = triple.getObject();
				pr.printf("%s  <%s rdf:about=\"%s\">%n", indent, type, object);
				if (seenAlready.size() == 0) {
					seenAlready.push(object);
					writeTriples(store.getBySubject(object), pr, indent + "    ", IGNORE_ON_RECURSION, seenAlready);
					seenAlready.pop();
				} else if (seenAlready.peek().equals("STOP")) {
					if (seenAlready.size() == RECURSION_LIMIT) {
						LOGGER.warn("Reached {}} deep on stack({}), stopping the recursion here.", RECURSION_LIMIT, String.join("->", seenAlready));
					} else {
						seenAlready.push("STOP");
						writeTriples(store.getBySubject(object), pr, indent + "    ", IGNORE_ON_RECURSION, seenAlready);
						seenAlready.pop();
					}
				} else if (seenAlready.contains(object)) {
					LOGGER.warn("'{}' seen before, stack({}})", object, String.join("->", seenAlready));
					seenAlready.push("STOP");
					writeTriples(store.getBySubject(object), pr, indent + "    ", IGNORE_ON_RECURSION, seenAlready);
					seenAlready.pop();
				} else {
					if (seenAlready.size() == RECURSION_LIMIT) {
						LOGGER.warn("Reached {} deep on stack({}), stopping the recursion here.", RECURSION_LIMIT, String.join("->", seenAlready));
					} else {
						seenAlready.push(object);
						writeTriples(store.getBySubject(object), pr, indent + "    ", IGNORE_ON_RECURSION, seenAlready);
						seenAlready.pop();
					}
				}
//				writeTriples(store.getBySubject(object), pr, indent + "    ", TYPE, seenAlready);
				pr.printf("%s  </%s>%n", indent, type);
				pr.printf("%s</%s>%n", indent, verb);
			}
			else {
				pr.println(indent + triple.asRdfXml(store.getNamespaceService()));
			}
		}
		else {
			prefix = SkosHandler.getUriPath(triple.getVerb());
			if (prefix != null) {
				pr.getUnknownPrefixes().merge(prefix, 1, (a, b) -> a + b);
			}
		}
	}

	private void writeTriples(Iterable<RdfTriple> triples, WriteContext pr, String indent, Collection<String> ignore) {
		writeTriples(store, pr, indent, ignore, null);
	}
	
	private void writeTriples(Iterable<RdfTriple> triples, WriteContext pr, String indent, Collection<String> ignore, Deque<String> seenAlready) {
		for (RdfTriple triple : triples) {
			if (!ignore.contains(triple.getVerb())) {
				writeTriple(triple, pr, indent, seenAlready);
			}
		}
	}
*/
	/**
	 * Writes the specified Concepts to SKOS RDF/XML file.
	 *
	 * @param path    Path to the file
	 * @param flags   flags to control the way we write; if XL set to true, will write Skos XL
	 * @throws IOException
	 */
	public void writeSKOS(Store store, String path) throws IOException {
		try (FileOutputStream pr = new FileOutputStream(path)) {
			//writeSKOS(new WriteContextStream(pr), null);
			pr.flush();
			pr.close();
		}
	}

	/**
	 * Writes the specified Concepts to SKOS RDF/XML file.
	 *
	 * @param pr    writer to which the SKOS content is written
	 * @param flags flags to control the way we write; if XL set to true, will write Skos XL
	 */
	public void writeSKOS(PrintWriter pr) {
		//writeSKOS(new WriteContextImpl(pr), null);
	}

	/**
	 * Writes the specified Concepts to SKOS RDF/XML file for concepts that match the predicate.
	 *
	 * @param pr               writer to which the SKOS content is written
	 * @param flags            flags to control the way we write; if XL set to true, will write Skos XL
	 * @param conceptPredicate filter output concepts to only those matching the predicate
	 */
	/*
	public void writeSKOS(WriteContext wc) {
		NamespaceService nse = store.getNamespaceService();
		wc.println("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
		wc.println("<rdf:RDF");
		
		// Collect the name space abbreviations - the standard ones are always in
		Set<String> ns = SetUtils.hashSet("rdf", "rdfs", "skos", "skosxl");
		for (String verb : store.getVerbs()) {
			String prefix = nse.getPrefix(verb);
			if (prefix != null) {
				ns.add(prefix);
			}
		}
		for (String n : ns) {
			wc.println(String.format("xmlns:%s=\"%s\"", n, nse.getFullForm(n)));
		}
		wc.println(">");	
		// write schemes and flag sets
		for (String top : new String[] { SKOS_CONCEPT_SCHEME, Model.DEFAULT_MATCHPROPERTIES_TYPE, Constants.RDFS + "Class" }) {
			for (RdfTriple scheme : this.store.getByVerbAndObject(Constants.RDF_TYPE, top)) {
				wc.println(String.format("<%s rdf:about=\"%s\">", nse.encodeUri(top), scheme.getSubject()));
				for (RdfTriple triple : store.getBySubject(scheme.getSubject())) {
					if (!triple.getVerb().equals(Constants.RDF_TYPE)) {
						writeTriple(triple, wc, "  ");
					}
				}
				wc.printf("</%s>%n", store.getNamespaceService().encodeUri(top));
			}
		}

		// write concepts
		int skippedConcepts=0;
		Set<String> concepts = this.store
				.getByVerbAndObject(Constants.RDF_TYPE, SKOS_CONCEPT).stream().map(RdfTriple::getSubject).collect(Collectors.toSet());
	
		for (String scheme : getSchemes()) {
			List<String> conceptsInScheme = this
					.store.getByVerbAndObject(SKOS_IN_SCHEME, scheme).stream().map(RdfTriple::getSubject)
					.collect(Collectors.toList());
			for (String c : conceptsInScheme) {
				// only export once, even if concept appears in multiple schemes
				if (concepts.contains(c)) {
					writeConcept(wc, c);
				}
			}
			// avoid exporting concept twice
			concepts.removeAll(conceptsInScheme);
		}
		
		wc.println("</rdf:RDF>");

		LOGGER.info("{} concepts processed, {} written, {} skipped", size(), size()-skippedConcepts, skippedConcepts);
		for (Map.Entry<String, Integer> unknownPrefix : wc.getUnknownPrefixes().entrySet()) {
			LOGGER.info("No prefix was defined for \"{}\", which occurred {} times.", unknownPrefix.getKey(), unknownPrefix.getValue());
		}
	}

	public void writeConcept(WriteContext wc, String concept) {
		
		boolean asSkosVerb = store.findFirst(concept, Constants.RDF_TYPE, SKOS_CONCEPT).isPresent();
		if (asSkosVerb) {
			wc.println("<skos:Concept rdf:about=\"" + concept + "\">");
		}
		else {
			wc.println("<rdf:Description rdf:about=\"" + concept + "\">");
		}
		
		for (RdfTriple triple : store.getBySubject(concept)) {
			if (!(asSkosVerb && triple.getVerb().equals(Constants.RDF_TYPE) && triple.getObject().equals(SKOS_CONCEPT))) {
				writeTriple(triple, wc, "  ");
			}
		}
		
		if (asSkosVerb) {
			wc.println("</skos:Concept>");
		}
		else {
			wc.println("</rdf:Description>");
		}
	}
	
	public interface WriteContext {
		void printf(String format, Object...args);
		void println(String line);
		Map<String, Integer> getUnknownPrefixes();
	}
	public static class WriteContextImpl implements WriteContext {
		private final Map<String, Integer> unknownPrefixes = new HashMap<>();
		private final PrintWriter ps;
		public WriteContextImpl(PrintWriter ps) {
			this.ps = ps;
		}
		public void printf(String format, Object...args) {
			ps.printf(format, args);
		}
		public void println(String line) {
			ps.println(line);
		}
		public Map<String, Integer> getUnknownPrefixes() {
			return unknownPrefixes;
		}
	}

	public static class WriteContextStream implements WriteContext {
		
		private final Map<String, Integer> unknownPrefixes = new HashMap<>();
		private final OutputStream ps;
		public WriteContextStream(OutputStream ps) {
			this.ps = ps;
		}
		public void printf(String format, Object...args) {
			try {
				ps.write(String.format(format, args).getBytes(UTF_8));
			} catch (IOException e) {
				throw new RuntimeException("Cannot write", e);
			}
		}
		public void println(String line) {
			try {
				ps.write(line.getBytes(UTF_8));
				ps.write('\n');
			} catch (IOException e) {
				throw new RuntimeException("Cannot write", e);
			}
		}
		public Map<String, Integer> getUnknownPrefixes() {
			return unknownPrefixes;
		}
	}
	*/
}
