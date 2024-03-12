package org.curieo.embed;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

class SentenceEmbeddingServiceTests {
	
	/**
	 * Disabled as this needs a live service.
	 */
	@Test @Disabled
	void testOnce() {
		EmbeddingService ses = new EmbeddingService("http://127.0.0.1:3000/embed", 768);
		System.out.printf("%d\n", ses.embed("The quick brown fox jumped over the lazy dog").length);
	}
}
