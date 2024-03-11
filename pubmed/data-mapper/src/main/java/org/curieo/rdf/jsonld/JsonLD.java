package org.curieo.rdf.jsonld;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import org.apache.commons.collections4.SetUtils;

import static org.curieo.rdf.Constants.*;
import org.curieo.rdf.Literal;
import org.curieo.rdf.NamespaceService;
import org.curieo.rdf.NamespaceServiceImpl;
import org.curieo.rdf.Store;
import org.curieo.rdf.RdfTriple;
import org.curieo.utils.ParseParameters;
import org.curieo.utils.ParseParametersFile;
import com.github.jsonldjava.core.JsonLdProcessor;
import com.github.jsonldjava.core.RDFDataset;
import com.github.jsonldjava.core.RDFDataset.Quad;
import com.github.jsonldjava.utils.JsonUtils;

public class JsonLD {
	/**
	 * Json-ld has a '@context' map defining namespaces, data types and global variables
	 */
	public static final String CONTEXT = "@context";
	/**
	 * Json-ld has a content map named '@default' map 
	 */
	public static final String DEFAULT = "@default";
	
	/**
	 * Special token corresponding to rdf:type
	 */
	public static final String TYPE    = "@type";
	/**
	 * Special token corresponding to rdf:subject
	 */
	public static final String ID      = "@id";

	public static final String URI_DATA_TYPE = XS + "URI";
	private static final String RDF_VALUE = RDF + "Value";
	private static final Set<String> RESERVED = SetUtils.hashSet(CONTEXT, TYPE, ID);
	private static final String CANNOT_READ_JSONLD = "Cannot read json-ld";
	
	private JsonLD() {}	
	
	public static void readTheHardWay(File path, Map<String, Store> stores) {
		try {
			readTheHardWay(new ParseParametersFile(new FileInputStream(path), path.getName()), stores);
		} catch (FileNotFoundException e) {
			throw new RuntimeException(CANNOT_READ_JSONLD, e);
		}
	}
	
	public static Store createEmptyStore() {
		NamespaceServiceImpl ns = new NamespaceServiceImpl();
		ns.put("rdf", RDF);
		return new Store(ns);
	}
	
	private static void readTheHardWay(ParseParameters pp, Map<String, Store> stores) {
		// Open a valid json(-ld) input file
		JsonObject object = JsonObject.parse(pp);
		
		if (object != null && object.isMap()) {
			// find the context
			JsonObject context = object.asMap().get(CONTEXT);
			Store store = stores.computeIfAbsent(CONTEXT, c -> createEmptyStore());
			
			// this should be a list of maps
			if (context != null) {
				readContext(store, context);
			}
			
			// default map
			store = new Store(store.getNamespaceService());
			if (object.isMap()) {
				toRdf(stores.get(CONTEXT), store, object);
			}
			if (!store.isEmpty()) {
				stores.put(DEFAULT, store);
			}
			NamespaceService defaultNamespaceService = store.getNamespaceService();
			object.asMap().forEach((key, value) -> 
			{
				if (key.startsWith("@") && !RESERVED.contains(key)) {
					
					Store graph = new Store(copy(defaultNamespaceService));
					toRdf(stores.get(CONTEXT), graph, value);
					if (!graph.isEmpty()) {
						stores.put(key, graph);
					}
				}
			});
		}
	}
	
	private static NamespaceService copy(NamespaceService original) {
		NamespaceServiceImpl impl = new NamespaceServiceImpl();
		for (String prefix : original.knownPrefixes()) {
			impl.put(prefix, original.getFullForm(prefix));
		}
		return impl;
	}
	
	private static void readContext(Store store, JsonObject context) {
		NamespaceService ns = store.getNamespaceService();
		if (context.isMap()) {
			// first namespaces
			context.asMap().forEach((key, value) ->
				{
					if (key.indexOf(':') == -1 && !key.startsWith("@")) {
						ns.put(key, value.asString());
					}
				});
			// then data
			context.asMap().forEach((key, value) -> {
					if (key.indexOf(':') != -1) {
						// declaring the type of a field
						JsonObject type = value.asMap().get(TYPE);
						if (type != null) {
							String dtype = type.asString();
							if (dtype.equals(ID)) {
								dtype = URI_DATA_TYPE;
							}
							else {
								dtype = ns.decodeUri(dtype);
							}
							store.assertTriple(ns.decodeUri(key), RDF_TYPE, dtype);
						}
					}
					else if (key.startsWith("@")) {
						store.assertTriple(key, RDF_VALUE, new Literal(value.asString(), null, null));
					}
				});
		}
		else {
			context.asList().forEach(o -> readContext(store, o));
		}
	}
	
	private static String toRdf(Store context, Store store, JsonObject map) {
		// ID corresponds to the _subject_ of the predicates in this map
		JsonObject idobj = map.asMap().get(ID);
		if (idobj == null) {
			map.asList().forEach(obj -> toRdf(context, store, obj));
			return null;
		}
		else {
			map.asMap().forEach((key, value) -> {
				String predicate = store.getNamespaceService().decodeUri(key);
				handleValue(context, idobj.asString(), store, predicate, value);
			});
			return idobj.asString();
		}
	}

	private static void handleValue(Store context, String subject, Store store, String predicate, JsonObject value) {
		if (predicate.equals(TYPE)) {
			store.assertTriple(subject, RDF_TYPE, store.getNamespaceService().decodeUri(value.asString()));
		}
		else if (!predicate.equals(ID)) {
			switch (value.getType()) {
			case LIST:
				value.asList().forEach(lv -> handleValue(context, subject, store, predicate, lv));
				break;
			case INTEGER:
				store.assertTriple(subject, predicate, new Literal(value.asInteger()));
				break;
			case FLOAT:
				store.assertTriple(subject, predicate, new Literal(value.asFloat()));
				break;
			case MAP:
				String object = toRdf(context, store, value);
				if (object != null) {
					store.assertTriple(subject, predicate, store.getNamespaceService().decodeUri(object));
				}
				break;
			case STRING:
				String dataType = context.getBySubjectAndVerb(predicate, RDF_TYPE).findFirst()
					.map(RdfTriple::getObject)
					.orElse(guessDataType(value.asString()));
				
				if (dataType.equals(URI_DATA_TYPE)) {
					store.assertTriple(subject, predicate, store.getNamespaceService().decodeUri(value.asString()));
				}
				else {
					store.assertTriple(subject, predicate, new Literal(value.asString(), null, dataType));
				}
				break;
			}
		}
	}
	

	private static String guessDataType(String string) {
		if (string.startsWith("http://") || string.startsWith("https://")) {
			return URI_DATA_TYPE;
		}
		else {
			return XS + "string";
		}
	}

	public static Map<String, Store> read(File file) {
		// Open a valid json(-ld) input file
		try {
			return read(new FileInputStream(file));
		} catch (IOException e) {
			throw new RuntimeException(CANNOT_READ_JSONLD, e);
		}
	}

	@SuppressWarnings("unchecked")
	public static Map<String, Store> read(InputStream inputStream) {
		// Open a valid json(-ld) input file
		try {
			// Read the file into an Object (The type of this object will be a List, Map, String, Boolean,
			// Number or null depending on the root object in the file).
			Object jsonObject = JsonUtils.fromInputStream(inputStream);
			// Customise options...
			// Call whichever JSONLD function you want! (e.g. compact)
			return (Map<String, Store>) JsonLdProcessor.toRDF(jsonObject, JsonLD::store);
		} catch (IOException e) {
			throw new RuntimeException(CANNOT_READ_JSONLD, e);
		}
	}
	
	private static Map<String, Store> store(RDFDataset rdf) {
		return rdf.graphNames().stream().collect(Collectors.toMap(UnaryOperator.identity(), gn -> read(rdf.getNamespaces(), rdf.getQuads(gn))));
	}

	private static Store read(Map<String, String> namespaces, List<Quad> quads) {
		NamespaceServiceImpl nsi = new NamespaceServiceImpl();
		namespaces.forEach((ns, prefix) -> nsi.put(prefix, ns));
		Store store = new Store(nsi);
		for (Quad quad : quads) {
			String subject = quad.getSubject().getValue();
			String verb = quad.getPredicate().getValue();
			if (quad.getObject().isIRI()) {
				store.assertTriple(subject, verb, quad.getObject().getValue());
			}
			else {
				store.assertTriple(subject, verb, new Literal(quad.getObject().getValue(), quad.getObject().getLanguage(), quad.getObject().getDatatype()));
			}
		}
		return store;
	}
}
