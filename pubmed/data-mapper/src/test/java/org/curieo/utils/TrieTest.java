package org.curieo.utils;

import java.util.function.Consumer;
import java.util.function.Function;
import java.util.Random;

import org.curieo.rdf.HashSet;
import org.junit.jupiter.api.Test;

class TrieTest {
	
	/**
	 * Trie takes 6 secs to store, 0.3 to retrieve, 1.8M memory
	 */
	@Test
	void testStorageSizeAndSpeedTrie() {
		Trie<Boolean> trie = new Trie<>();
		testStorageSizeAndSpeed("Trie", t -> trie.put(t, Boolean.TRUE), trie::containsKey);
	}

	/**
	 * HashSet takes 2 secs to store, 0.17 to retrieve, 0.7M memory
	 */
	@Test
	void testStorageSizeAndSpeedHashSet() {
		HashSet<String> hashSet = new HashSet<>();
		testStorageSizeAndSpeed("HashSet", hashSet::add, hashSet::contains);
	}

	/**
	 * System HashSet takes 2 secs to store, 0.13 to retrieve, 1.1M memory
	 */
	@Test
	void testStorageSizeAndSpeedSystemHashSet() {
		java.util.HashSet<String> hashSet = new java.util.HashSet<>();
		testStorageSizeAndSpeed("HashSet", hashSet::add, hashSet::contains);
	}
	
	void testStorageSizeAndSpeed(String key, Consumer<String> store, Function<String, Boolean> retrieve) {
		Random random = new Random(1000);
		System.gc();
		Runtime runtime = Runtime.getRuntime();
		long mem = runtime.totalMemory() - runtime.freeMemory();
		long start = System.currentTimeMillis();
		for (int i = 0; i < 10000000; i++) {
			store.accept(Integer.toString(random.nextInt()));
		}
		long end = System.currentTimeMillis();
		System.out.printf("%s took %.03f seconds to store 10M integers%n", key, (end - start)/(float)1000);
		int contained = 0;
		for (int i = 0; i < 1000000; i++) {
			if (retrieve.apply(Integer.toString(random.nextInt()))) {
				contained++;
			}
		}
		long last = System.currentTimeMillis();
		long memNow = runtime.totalMemory() - runtime.freeMemory();
		System.out.printf("%s took %.03f seconds to check 1M integers (%d found)%n", key, (last - end)/(float)1000, contained);
		System.out.printf("%s took %dK to store%n", key, (memNow-mem)/1024);
	}
}
