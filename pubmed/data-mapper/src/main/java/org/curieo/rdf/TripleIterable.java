package org.curieo.rdf;

import java.util.Iterator;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.commons.collections4.IteratorUtils;

/**
 * This interface brings the RdfStore more in sync with the API of rdf4j. [Rdf4j is slower but has a
 * nice API] Should we ever choose to swap out one for the other, this makes it easier.
 * https://rdf4j.org/documentation/programming/model/
 */
public interface TripleIterable extends Iterable<RdfTriple> {

  default Optional<RdfTriple> findFirst() {
    Iterator<RdfTriple> it = this.iterator();
    if (it.hasNext()) {
      return Optional.of(it.next());
    }
    return Optional.empty();
  }

  default Iterable<String> subjects() {
    return map(RdfTriple::getSubject);
  }

  default Iterable<String> objects() {
    return map(TripleIterable::getObject);
  }

  default <T> Iterable<T> map(Function<RdfTriple, T> m) {
    return new Iterable<T>() {
      private final Iterator<RdfTriple> triples = TripleIterable.this.iterator();

      @Override
      public Iterator<T> iterator() {
        return new Iterator<T>() {
          @Override
          public boolean hasNext() {
            return triples.hasNext();
          }

          @Override
          public T next() {
            return m.apply(triples.next());
          }
        };
      }
    };
  }

  static String getObject(RdfTriple triple) {
    return triple.isSimple() ? triple.getObject() : triple.getLiteralObject().getValue();
  }

  default TripleIterable filter(Predicate<RdfTriple> test) {
    return () -> IteratorUtils.filteredIterator(TripleIterable.this.iterator(), test::test);
  }

  default Stream<RdfTriple> stream() {
    return StreamSupport.stream(this.spliterator(), false);
  }
}
