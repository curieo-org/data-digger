package org.curieo.rdf;

import java.util.List;
import java.util.Set;
import java.util.function.BiPredicate;

import lombok.Generated;
import lombok.Value;

@FunctionalInterface
public interface RdfPredicate extends BiPredicate<Store, String> {
	public static final RdfPredicate PASS = (a, b) -> true;
	
    /**
     * Evaluates this predicate on the given uri argument.
     *
     * @param rdf the rdf store
     * @param uri the input uri
     * @return {@code true} if the uri argument matches the predicate,
     * otherwise {@code false}
     */
	@Override
	boolean test(Store rdf, String uri);
	
	@Generated @Value
	static class FilterByProperty implements RdfPredicate {
		String property;
		Set<String> values;
		
		@Override
		public boolean test(Store rdf, String uri) {
			for (String v : rdf.getBySubjectAndVerb(uri, property).objects()) {
				if (values.isEmpty() || values.contains(v)) {
					return true;
				}
			}
			return false;
		}
	}

	@Generated @Value
	static class And implements RdfPredicate {
		List<BiPredicate<Store, String>> predicates;
		@Override
		public boolean test(Store rdf, String uri) {
			return predicates.stream().allMatch(p -> p.test(rdf, uri));
		}
	}
	
	@Generated @Value
	static class Or implements RdfPredicate {
		List<BiPredicate<Store, String>> predicates;
		@Override
		public boolean test(Store rdf, String uri) {
			return predicates.stream().anyMatch(p -> p.test(rdf, uri));
		}
	}
}
