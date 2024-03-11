package org.curieo.utils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ListUtils {
	
	private ListUtils() {}
	
	
	/**
	 * Compute Cartesian product of multiple lists.
	 * Suppose you have three lists of ["a", "b"], ["1", "2", "3"] and ["*", "#"], 
	 * you will receive 12 lists containing:
	 *   ["a", "1", "*"],
	 *   ["a", "1", "#"],
	 *   ["a", "2", "*"],
	 *   ["a", "2", "#"],
	 *   ...
	 *   ["b", "3", "#"]
	 * In other words, all possible combinations.
	 * 
	 * @param <T>
	 * @param combination
	 * @return the Cartesian product of lists.
	 */
	public static <T> List<List<T>> cartesian(List<List<T>> combination) {
        return cartesian(combination, 0);
	}

	static <T> List<List<T>> cartesian(List<List<T>> combination, int i) {
		if (i == combination.size()) {
            return Collections.emptyList();
		}
		if (i + 1 == combination.size()) {
			return combination.get(i).stream().map(v -> Collections.singletonList(v)).toList();
		}

        List<List<T>> retval = new ArrayList<>();
        for (T v : combination.get(i)) {
            for (List<T> combine : cartesian(combination, i + 1)) {
                List<T> nl = new ArrayList<>();
                nl.add(v);
                nl.addAll(combine);
                retval.add(nl);
            }
        }
        return retval;
	}
}
