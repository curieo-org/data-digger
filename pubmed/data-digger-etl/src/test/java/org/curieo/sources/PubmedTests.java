package org.curieo.sources;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import javax.xml.stream.XMLStreamException;

import org.curieo.sources.pubmed.Pubmed;
import org.curieo.sources.pubmed.PubmedRecord;
import org.junit.jupiter.api.Test;

class PubmedTests {
	
	@Test
	void testReader() throws IOException, XMLStreamException {
		String path = System.getenv("HOME") + "/Documents/corpora/pubmed/pubmed24n1223.xml";
		int count = 0;
		int haveAbstract = 0, haveTitle = 0, haveReferences = 0;
		Map<Integer, Integer> yearCount = new HashMap<>();
		for (PubmedRecord pr : Pubmed.read(new File(path))) {
			String title, pmid = pr.getIdentifier("pubmed");
			if (pr.getTitles().isEmpty()) {
				title = "NONE";
			}
			else {
				title = pr.getTitles().get(0).getString();
				haveTitle++;
			}
			if (!pr.getAbstractText().isEmpty()) {
				haveAbstract++;
			}
			if (pr.getPublicationDate() != null) {
				yearCount.merge(pr.getYear(), 1, (a, b) -> a + b);
			}
			if (count%1000 == 0) {
				System.out.printf("%s: %s\n", pmid, title);
			}
			if (pr.getPubmedReferences() != null) {
				haveReferences++;
			}
			count++;
		}
		System.out.printf("Found %d records, %d have title, %d have abstract, %d have references\n", 
				count, haveTitle, haveAbstract, haveReferences);
		Iterable<Map.Entry<Integer, Integer>> yearcounts = yearCount.entrySet().stream().sorted((a, b) -> a.getKey().compareTo(b.getKey()))::iterator;
		for (Map.Entry<Integer, Integer> year : yearcounts) {
			System.out.printf("Found %d records from year %d\n", 
					year.getValue(), year.getKey());
		}
	}
	

}