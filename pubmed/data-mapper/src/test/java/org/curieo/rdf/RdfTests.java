package org.curieo.rdf;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

class RdfTests {

  private void printStats(Store rdf) {
    Stream.of(rdf.statistics()).forEach(System.out::println);
    Map<String, Integer> subjectCount = new HashMap<>();
    Map<String, Integer> objectCount = new HashMap<>();
    rdf.getVerbs()
        .forEach(
            v -> {
              Set<String> subjects = new HashSet<>();
              Set<String> objects = new HashSet<>();
              rdf.getByVerb(v).subjects().forEach(subjects::add);
              rdf.getByVerb(v).objects().forEach(objects::add);
              subjectCount.put(v, subjects.size());
              objectCount.put(v, objects.size());
            });
    rdf.getVerbs()
        .forEach(
            v ->
                System.out.printf(
                    "verb: %s, with %d subjects and %d objects%n",
                    v, subjectCount.get(v), objectCount.get(v)));
  }

  @Test
  void testJournals() throws IOException {

    String path = "../corpora/pc_journal_000001.ttl.gz";
    Store rdf =
        new Turtle(SkosNamespaceService::createStore).read(new File(path)).get(Turtle.DEFAULT);
    printStats(rdf);
  }

  @Test
  void testReadMesh() throws IOException {
    String path = "../corpora/mesh-snippet.nt";
    Store rdf =
        new Turtle(SkosNamespaceService::createStore).read(new File(path)).get(Turtle.DEFAULT);
    printStats(rdf);
  }
}
