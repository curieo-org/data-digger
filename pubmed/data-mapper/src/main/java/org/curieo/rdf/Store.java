package org.curieo.rdf;

import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static org.curieo.rdf.Constants.*;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.collections4.IterableUtils;
import org.apache.commons.collections4.SetUtils;
import org.apache.commons.lang3.StringUtils;
import org.curieo.utils.HashSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Store holding all Triples in Memory And yes, that is not a sustainable solution for any kind of
 * real RDF data set.
 */
public class Store implements Collection<RdfTriple>, TripleStore, TripleIterable {
  public static final String INDENT = "  ";

  /**
   * Indexed to the URI - that must always be unique. You *can* insert triples without URI -- those
   * are anonymous are silently de-duplicated if inserted multiple times.
   */
  private final Map<String, Set<Integer>> bySubject = new HashMap<>();

  private final Map<String, Set<Integer>> byVerb = new HashMap<>();
  private final Map<String, Set<Integer>> byObject = new HashMap<>();
  private final Map<String, Integer> byUri = new HashMap<>();
  private RdfTriple[] triples = new RdfTriple[100];
  private int size = 0;
  private int[] abandoned = new int[100];
  private int asize = 0;
  private final NamespaceService namespaceService;
  private static final Logger LOGGER = LoggerFactory.getLogger(Store.class);

  public Store(NamespaceService namespaceService) {
    this.namespaceService = namespaceService;
  }

  public Store() {
    this(new NamespaceServiceImpl());
  }

  /**
   * Compile a set of statistics. Each String is a printable line.
   *
   * @return string-formatted statistics.
   */
  public String[] statistics() {
    return new String[] {
      String.format("RDF store contains %d subjects", bySubject.size()),
      String.format("RDF store contains %d objects", byObject.size()),
      String.format("RDF store contains %d verbs", byVerb.size()),
      String.format("RDF store contains %d triples", size())
    };
  }

  /**
   * Assert a triple. If the triple is *anonymous* - that is, it has no name (URI) we will *not*
   * allow the triple to be duplicated. In this case the method returns the existing triple.
   * Otherwise the method will return the asserted triple.
   *
   * @param triple
   * @return the triple that was added, or another (non-anonymous) triple in the store
   */
  @Override
  public RdfTriple accept(RdfTriple triple) {
    String uri = triple.getUri();
    if (uri == null) {
      if (triple.isSimple()) {
        Optional<RdfTriple> first =
            getTriples(triple.getSubject(), triple.getVerb(), triple.getObject()).findFirst();
        if (first.isPresent()) {
          return first.get();
        }
      } else {
        for (RdfTriple c : getBySubjectAndVerb(triple.getSubject(), triple.getVerb())) {
          if (c.isSimple()) {
            LOGGER.info(
                "Triple [{} {}] once asserted with literal ({}) and once with URI object ({})",
                triple.getSubject(),
                triple.getVerb(),
                triple.getLiteralObject().getValue(),
                c.getObject());
          } else if (c.getLiteralObject().equals(triple.getLiteralObject())) {
            return c; // anonymous triples cannot be duplicated
          }
        }
      }
    }

    int r;
    if (asize != 0) { // if we abandoned triple slots earlier, we'll first fill those up.
      asize--;
      r = abandoned[asize];
    } else {
      r = size;
      if (size == triples.length) {
        triples = Arrays.copyOf(triples, size * 2);
      }
      size++;
    }

    add(bySubject, triple.getSubject(), r);

    // only index by object for non-literals
    if (triple.isSimple()) {
      add(byObject, triple.getObject(), r);
    }

    add(byVerb, triple.getVerb(), r);
    if (uri != null) {
      byUri.put(uri, r);
    }
    triples[r] = triple;
    return triple;
  }

  @Override
  public RdfTriple assertTriple(String uri, String subject, String verb, String object) {
    if (uri == null) {
      return accept(createTriple(subject, verb, object));
    } else {
      return accept(new RdfSimpleTriple(uri, intern(subject), intern(verb), intern(object)));
    }
  }

  @Override
  public RdfTriple assertAndNameTriple(String uri, String subject, String verb, String object) {
    if (uri == null) {
      throw new IllegalArgumentException("URI cannot be null");
    }

    for (RdfTriple triple : getTriples(subject, verb, object)) {
      if (triple.getUri() == null) {
        this.remove(triple);
        break;
      }
      if (triple.getUri().equals(uri)) {
        return triple;
      }
    }

    return accept(new RdfSimpleTriple(uri, intern(subject), intern(verb), intern(object)));
  }

  @Override
  public RdfTriple assertTriple(String uri, String subject, String verb, Literal object) {
    if (uri == null) {
      return accept(createTriple(subject, verb, object));
    } else {
      checkArguments(subject, verb, object);
      return accept(new RdfLiteralTriple(uri, intern(subject), intern(verb), object));
    }
  }

  public RdfTriple createTriple(String subject, String verb, String object) {
    checkArguments(subject, verb, object);
    RdfVerbObject rvo = getImmutableVerbObjectPair(intern(verb), intern(object));
    if (rvo != null) {
      return new RdfAnonymousImmutableTriple(intern(subject), rvo);
    }
    return new RdfAnonymousTriple(intern(subject), intern(verb), intern(object));
  }

  public RdfTriple createTriple(String subject, String verb, Literal object) {
    checkArguments(subject, verb, object);
    RdfVerbLiteral rvl = getImmutableVerbObjectPair(intern(verb), object);
    if (rvl != null) {
      return new RdfAnonymousImmutableTriple(intern(subject), rvl);
    }
    return new RdfAnonymousLiteralTriple(intern(subject), intern(verb), object);
  }

  public RdfTriple createTriple(String uri, String subject, String verb, String object) {
    if (uri == null) {
      return createTriple(subject, verb, object);
    } else {
      return new RdfSimpleTriple(uri, intern(subject), intern(verb), intern(object));
    }
  }

  private static void checkArguments(String subject, String verb, Object object) {
    if (subject == null) throw new IllegalArgumentException("subject");
    if (verb == null) throw new IllegalArgumentException("verb");
    if (object == null) throw new IllegalArgumentException("object");
  }

  /**
   * If a string has already been added to the dictionaries, do not add it again
   *
   * @param id
   * @return
   */
  private String intern(String id) {
    Set<Integer> existing = bySubject.get(id);
    if (existing != null && !existing.isEmpty()) {
      return triples[existing.iterator().next()].getSubject();
    }
    existing = byVerb.get(id);
    if (existing != null && !existing.isEmpty()) {
      return triples[existing.iterator().next()].getVerb();
    }
    existing = byObject.get(id);
    if (existing != null && !existing.isEmpty()) {
      return triples[existing.iterator().next()].getObject();
    }
    return id;
  }

  /**
   * Get all triples by subject
   *
   * @param s
   * @return
   */
  public TripleIterable getBySubject(String s) {
    return new RdfTripleIterable(bySubject.getOrDefault(s, emptySet()));
  }

  /**
   * Get the triples that contain this exact subject, verb and object.
   *
   * @param subject
   * @param verb
   * @param object
   * @return the triples
   */
  public TripleIterable getTriples(String subject, String verb, String object) {
    return new RdfTripleIterable(
        safeIntersection(bySubject.get(subject), byVerb.get(verb), byObject.get(object)));
  }

  /**
   * Get the first triple that contain this exact subject, verb and object.
   *
   * @param subject
   * @param verb
   * @param object
   * @return the first triple
   */
  public Optional<RdfTriple> findFirst(String subject, String verb, String object) {
    return getTriples(subject, verb, object).findFirst();
  }

  /**
   * Get the first triple that contain this exact subject and verb.
   *
   * @param subject
   * @param verb
   * @return the first triple
   */
  public Optional<RdfTriple> findFirst(String subject, String verb) {
    return getBySubjectAndVerb(subject, verb).findFirst();
  }

  /**
   * Get the verbs
   *
   * @return verbs
   */
  public Collection<String> getVerbs() {
    return byVerb.keySet();
  }

  /**
   * Get all triples by subject and verb
   *
   * @param s
   * @param v
   * @return
   */
  public TripleIterable getBySubjectAndVerb(String s, String v) {
    return new RdfTripleIterable(safeIntersection(bySubject.get(s), byVerb.get(v)));
  }

  /**
   * Get all object values - if one is a container, return these values as a list
   *
   * @param s
   * @param v
   * @return a list of values
   */
  public List<String> getContainer(String s, String v) {
    List<String[]> values = new ArrayList<>();
    for (String object :
        new RdfTripleIterable(safeIntersection(bySubject.get(s), byVerb.get(v))).objects()) {
      int size = values.size();
      getBySubject(object)
          .filter(Store::isMember)
          .forEach(t -> values.add(new String[] {t.getVerb(), t.getObject()}));
      if (values.size() == size) {
        values.add(new String[] {null, object});
      }
    }
    return values.stream()
        .sorted((a, b) -> StringUtils.compare(a[0], b[0]))
        .map(a -> a[1])
        .collect(Collectors.toList());
  }

  private static boolean isMember(RdfTriple triple) {
    return triple.getVerb().startsWith(MEMBER)
        && StringUtils.isNumeric(triple.getVerb().substring(MEMBER.length()));
  }

  /**
   * Count all triples by subject and verb
   *
   * @param s
   * @param verb
   * @return
   */
  public int countBySubjectAndVerb(String s, String verb) {
    return IterableUtils.size(safeIntersection(bySubject.get(s), byVerb.get(verb)));
  }

  /**
   * Count all triples by verb and object
   *
   * @param v verb
   * @param o object
   * @return count
   */
  public int countByVerbAndObject(String v, String o) {
    return IterableUtils.size(safeIntersection(byVerb.get(v), byObject.get(o)));
  }

  /**
   * Get all triples by verb
   *
   * @param verbs verb list
   * @return iterable
   */
  public TripleIterable getByVerb(String... verbs) {
    if (verbs.length == 0) {
      return new RdfTripleIterable(Collections.emptyList());
    } else if (verbs.length == 1) {
      return new RdfTripleIterable(byVerb.getOrDefault(verbs[0], emptySet()));
    } else {
      return Stream.of(verbs)
              .flatMap(v -> new RdfTripleIterable(byVerb.getOrDefault(v, emptySet())).stream())
          ::iterator;
    }
  }

  /**
   * Get the number of triples by this verb
   *
   * @param verb verb
   * @return the number of triples
   */
  public int countByVerb(String verb) {
    return byVerb.getOrDefault(verb, emptySet()).size();
  }

  /**
   * @return all triples by object
   */
  public TripleIterable getByObject(String o) {
    return new RdfTripleIterable(byObject.getOrDefault(o, emptySet()));
  }

  public TripleIterable getBySubjectAndObject(String subject, String object) {
    return new RdfTripleIterable(safeIntersection(bySubject.get(subject), byObject.get(object)));
  }

  public TripleIterable getByVerbAndObject(String verb, String object) {
    return new RdfTripleIterable(safeIntersection(byVerb.get(verb), byObject.get(object)));
  }

  /**
   * Does the *transitive* relation src-REL-trg exist in this graph? If not, does the relation
   * src-REL-?-REL-trg hold? If src=trg this method returns true
   *
   * @param source
   * @param relation
   * @param target
   * @return true if so
   */
  public boolean isRelated(String source, String relation, String target) {
    SetStack src = new SetStack().add(source);
    SetStack trg = new SetStack().add(target);
    do {
      for (String t : trg.getLast()) {
        if (src.contains(t)) {
          return true;
        }
      }
      Set<String> newSrc = new HashSet<>();
      for (String s : src.getLast()) {
        for (RdfTriple rt : this.getBySubjectAndVerb(s, relation)) {
          if (trg.contains(rt.getObject())) {
            return true; // match!
          }
          if (!src.contains(rt.getObject())) { // avoid cycles
            newSrc.add(rt.getObject());
          }
        }
      }
      if (newSrc.isEmpty()) {
        return false; // no further expansions possible- we're done
      }
      src.add(newSrc);

      Set<String> newTrg = new HashSet<>();
      for (String t : trg.getLast()) {
        for (RdfTriple rt : this.getByVerbAndObject(relation, t)) {
          if (newSrc.contains(rt.getSubject())) {
            return true; // match!
          }
          if (!trg.contains(rt.getSubject())) { // avoid cycles
            newTrg.add(rt.getSubject());
          }
        }
      }
      if (newTrg.isEmpty()) {
        return false; // no further expansions possible- we're done
      }
      trg.add(newTrg);
    } while (true);
  }

  /**
   * Get triple by URI
   *
   * @param u
   * @return
   */
  public RdfTriple getByUri(String u) {
    Integer offset = byUri.get(u);
    if (offset == null) return null;
    return triples[offset];
  }

  /**
   * Remove an evo:category when two concepts are the same and in the same category We find the all
   * the categories from the originalConcept and the conceptWithDuplicates abd remove the ones that
   * have in common from the conceptWithDuplicates
   *
   * @param conceptWithDuplicates - The string Uri that we will check its categories and remove the
   *     common ones
   * @param originalConcept - The string Uri that we are comparing to
   * @param preservedCategories - category Uris that should not be removed if duplicated
   * @return number of replacements - The number of categories that were removed from
   *     conceptWithDuplicates
   */
  public int removeDuplicateCategoriesFromConcept(
      final String conceptWithDuplicates,
      final String originalConcept,
      Set<String> preservedCategories) {
    int repl = 0;

    List<RdfTriple> categoryConceptWithDuplicates = new ArrayList<>();
    List<String> categoryOriginalConcept = new ArrayList<>();
    preservedCategories = SetUtils.emptyIfNull(preservedCategories);

    this.getBySubjectAndVerb(originalConcept, Constants.EVO_CATEGORY)
        .objects()
        .forEach(categoryOriginalConcept::add);
    this.getBySubjectAndVerb(conceptWithDuplicates, Constants.EVO_CATEGORY)
        .forEach(categoryConceptWithDuplicates::add);

    for (RdfTriple categoryTriple : categoryConceptWithDuplicates) {
      String candidateCategory = categoryTriple.getObject();
      if (categoryOriginalConcept.contains(candidateCategory)
          && !preservedCategories.contains(candidateCategory)) {
        remove(categoryTriple);
        repl += 1;
      }
    }
    return repl;
  }

  /**
   * Remove an evo:category when two concepts are the same and in the same category We find the all
   * the categories from the originalConcept and the conceptWithDuplicates abd remove the ones that
   * have in common from the conceptWithDuplicates
   *
   * @param conceptWithDuplicates - The string Uri that we will check its categories and remove the
   *     common ones
   * @param originalConcept - The string Uri that we are comparing to
   * @return number of replacements - The number of categories that were removed from
   *     conceptWithDuplicates
   */
  public int removeDuplicateCategoriesFromConcept(
      final String conceptWithDuplicates, final String originalConcept) {
    return removeDuplicateCategoriesFromConcept(conceptWithDuplicates, originalConcept, emptySet());
  }

  /**
   * Replace a URI
   *
   * @param olduri
   * @param newuri
   * @return number of replacements
   */
  public int replaceUri(final String olduri, final String newuri) {
    int repl = 0;
    Set<Integer> set = bySubject.remove(olduri);
    if (set != null) {
      List<RdfTriple> removed = remove(set);
      for (RdfTriple rt : removed) {
        if (rt.isSimple()) assertTriple(rt.getUri(), newuri, rt.getVerb(), rt.getObject());
        else assertTriple(rt.getUri(), newuri, rt.getVerb(), rt.getLiteralObject());
      }
      repl += removed.size();
    }

    set = byObject.remove(olduri);
    if (set != null) {
      List<RdfTriple> removed = remove(set);
      for (RdfTriple rt : removed) {
        this.assertTriple(rt.getUri(), rt.getSubject(), rt.getVerb(), newuri);
      }
      repl += removed.size();
    }

    set = byVerb.remove(olduri);
    if (set != null) {
      List<RdfTriple> removed = remove(set);
      for (RdfTriple rt : removed) {
        if (rt.isSimple()) assertTriple(rt.getUri(), rt.getSubject(), newuri, rt.getObject());
        else assertTriple(rt.getUri(), rt.getSubject(), newuri, rt.getLiteralObject());
      }
      repl += removed.size();
    }

    Integer offset = byUri.remove(olduri);
    if (offset != null) {
      triples[offset].setUri(newuri);
      byUri.put(newuri, offset);
      repl++;
    }

    return repl;
  }

  private List<RdfTriple> remove(Set<Integer> set) {
    return set.stream().map(this::removeTriple).collect(Collectors.toList());
  }

  /**
   * Add a key-value pair to a 1-many map.
   *
   * @param to map to add to
   * @param key
   * @param value
   */
  private static void add(Map<String, Set<Integer>> to, String key, Integer value) {
    to.computeIfAbsent(key, k -> new HashSet<>()).add(value);
  }

  /**
   * Intersection that does not crash on nulls
   *
   * @param a
   * @param b
   * @return
   */
  private static Iterable<Integer> safeIntersection(Set<Integer> a, Set<Integer> b) {
    if (a == null || b == null) return emptySet();
    // smallest first
    if (a.size() > b.size()) {
      return new IntersectIterable(b, a);
    } else {
      return new IntersectIterable(a, b);
    }
  }

  private static Iterable<Integer> safeIntersection(
      Set<Integer> a, Set<Integer> b, Set<Integer> c) {
    if (a == null || b == null || c == null) return emptySet();

    // abc, acb, bac, bca, cab, cba
    // order by size
    if (a.size() > b.size()) {
      // b <= a
      if (c.size() > b.size()) {
        // b < c, b <= a
        if (c.size() > a.size()) {
          // b < a <= c
          return new IntersectIterable(b, a, c);
        }
        // b < c <= a
        return new IntersectIterable(b, c, a);
      } else if (c.size() > a.size()) {
        // c <= a, c <= b, b < a
        return new IntersectIterable(c, b, a);
      } else {
        // a <= c, c <= b, b < a
        return new IntersectIterable(b, c, a);
      }
    } else {
      // a <= b
      if (c.size() > b.size()) {
        // a <= b, b < c
        return new IntersectIterable(a, b, c);
      } else {
        // a <= b, c <= b
        if (a.size() > c.size()) {
          // c < a, a <= b
          return new IntersectIterable(c, a, b);
        } else {
          // a <= c, a <= b, c <= b
          return new IntersectIterable(a, c, b);
        }
      }
    }
  }

  private static class IntersectIterable implements Iterable<Integer> {
    private final Iterable<Integer> a;
    private final Set<Integer> b;

    IntersectIterable(Iterable<Integer> a, Set<Integer> b) {
      this.a = a;
      this.b = b;
    }

    IntersectIterable(Iterable<Integer> a, Set<Integer> b, Set<Integer> c) {
      this(new IntersectIterable(a, b), c);
    }

    @Override
    public Iterator<Integer> iterator() {
      return new IntersectIterator();
    }

    class IntersectIterator implements Iterator<Integer> {
      private final Iterator<Integer> ait;
      private Integer current;

      IntersectIterator() {
        ait = a.iterator();
        moveNext();
      }

      private void moveNext() {
        while (ait.hasNext()) {
          current = ait.next();
          if (b.contains(current)) {
            return;
          }
        }
        current = null;
      }

      @Override
      public boolean hasNext() {
        return current != null;
      }

      @Override
      public Integer next() {
        if (current == null) {
          throw new NoSuchElementException();
        }
        Integer n = current;
        moveNext();
        return n;
      }
    }
  }

  @Override
  public int size() {
    return size - asize; // the number filled minus the abandoned ones
  }

  @Override
  public boolean isEmpty() {
    return size - asize == 0;
  }

  @Override
  public boolean contains(Object o) {
    if (o instanceof RdfTriple) {
      RdfTriple rt = (RdfTriple) o;
      if (rt.isSimple()) {
        return contains(rt.getSubject(), rt.getVerb(), rt.getObject());
      }
      return contains(rt.getSubject(), rt.getVerb(), rt.getLiteralObject());
    }
    return false;
  }

  public boolean contains(String subject, String verb, Literal object) {
    for (RdfTriple rt : this.getBySubjectAndVerb(subject, verb)) {
      if (rt.getLiteralObject().equals(object)) {
        return true;
      }
    }
    return false;
  }

  public boolean contains(String subject, String verb, String object) {
    return safeIntersection(bySubject.get(subject), byVerb.get(verb), byObject.get(object))
        .iterator()
        .hasNext();
  }

  @Override
  public Iterator<RdfTriple> iterator() {
    return new RdfIterator();
  }

  @Override
  public Object[] toArray() {
    if (asize == 0) {
      return Arrays.copyOf(triples, size);
    } else {
      Object[] retval = new Object[size - asize];
      int o = 0;
      for (int i = 0; i < size; i++) {
        if (triples[i] != null) {
          retval[o++] = triples[i];
        }
      }
      return retval;
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> T[] toArray(T[] a) {
    Object[] array = toArray();
    if (a.length < array.length) {
      return (T[]) array;
    } else {
      for (int i = 0; i < array.length; i++) {
        a[i] = (T) array[i];
      }
      return a;
    }
  }

  @Override
  public boolean add(RdfTriple e) {
    RdfTriple old = accept(e);
    // return true if changed... in the situation where there was a triple that blocks this one from
    // being add
    // the collection remains *unchanged* and there false is returned;
    return old == e;
  }

  private RdfTriple removeTriple(Integer t) {
    RdfTriple rt = triples[t];
    triples[t] = null; // remove
    Set<Integer> set;
    set = bySubject.get(rt.getSubject());
    if (set != null && set.remove(t) && set.isEmpty()) {
      bySubject.remove(rt.getSubject());
    }
    set = byVerb.get(rt.getVerb());
    if (set != null && set.remove(t) && set.isEmpty()) {
      byVerb.remove(rt.getVerb());
    }

    if (rt.isSimple()) {
      set = byObject.get(rt.getObject());
      if (set != null && set.remove(t) && set.isEmpty()) {
        byObject.remove(rt.getObject());
      }
    }

    if (rt.getUri() != null) {
      byUri.remove(rt.getUri());
    }
    int r = asize;
    if (asize == abandoned.length) {
      abandoned = Arrays.copyOf(abandoned, asize * 2);
    }
    abandoned[r] = t;
    asize++;
    return rt;
  }

  @Override
  public boolean remove(Object o) {
    if (o instanceof RdfTriple) {
      return remove((RdfTriple) o);
    } else {
      return false;
    }
  }

  private boolean remove(RdfTriple o) {
    if (o.getUri() != null) {
      Integer i = byUri.get(o.getUri());
      if (i != null) {
        removeTriple(i);
      }
      return i != null;
    }
    List<Integer> remove = new ArrayList<>();
    for (Integer i : safeIntersection(bySubject.get(o.getSubject()), byVerb.get(o.getVerb()))) {
      if (triples[i].equals(o)) {
        remove.add(i);
      }
    }
    remove.forEach(this::removeTriple);
    return remove.size() != 0;
  }

  public boolean removeByUri(String uri) {
    Set<Integer> triplesToRemove = new HashSet<>();
    triplesToRemove.addAll(SetUtils.emptyIfNull(bySubject.get(uri)));
    triplesToRemove.addAll(SetUtils.emptyIfNull(byObject.get(uri)));
    triplesToRemove.addAll(SetUtils.emptyIfNull(byVerb.get(uri)));
    Integer i = byUri.get(uri);
    if (i != null) {
      triplesToRemove.add(i);
    }
    triplesToRemove.forEach(this::removeTriple);
    return !triplesToRemove.isEmpty();
  }

  @Override
  public boolean containsAll(Collection<?> c) {
    for (Object o : c) {
      if (!contains(o)) {
        return false;
      }
    }
    return true;
  }

  @Override
  public boolean addAll(Collection<? extends RdfTriple> c) {
    boolean any = false;
    for (RdfTriple t : c) {
      if (add(t)) {
        any = true;
      }
    }
    return any;
  }

  @Override
  public boolean removeAll(Collection<?> c) {
    boolean done = false;
    for (Object o : c) {
      done = remove(o) || done;
    }
    return done;
  }

  @Override
  public boolean retainAll(Collection<?> c) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void clear() {
    bySubject.clear();
    byVerb.clear();
    byObject.clear();
    byUri.clear();
    size = 0;
    asize = 0;
  }

  private class RdfIterator implements Iterator<RdfTriple> {
    private int offset = 0;

    @Override
    public boolean hasNext() {
      while (offset != size) {
        if (triples[offset] != null) return true;
        offset++;
      }
      return false;
    }

    @Override
    public RdfTriple next() {
      if (offset == size || triples[offset] == null) {
        throw new NoSuchElementException();
      }
      return triples[offset++];
    }
  }

  private class RdfTripleIterable implements TripleIterable {
    private final Iterable<Integer> offsets;

    private RdfTripleIterable(Iterable<Integer> offsets) {
      this.offsets = offsets;
    }

    @Override
    public Iterator<RdfTriple> iterator() {
      return new RdfTripleIterator();
    }

    private class RdfTripleIterator implements Iterator<RdfTriple> {
      private final Iterator<Integer> iterator;

      RdfTripleIterator() {
        this.iterator = offsets.iterator();
      }

      @Override
      public boolean hasNext() {
        return iterator.hasNext();
      }

      @Override
      public RdfTriple next() {
        return triples[iterator.next()];
      }
    }
  }

  /*
   * A memory optimization - allow for immutable verb-object pairs
   */
  private final Map<String, Map<String, RdfVerbObject>> immutableVerbObjectPairs = new HashMap<>();
  private final Map<String, Map<Literal, RdfVerbLiteral>> immutableVerbLiteralPairs =
      new HashMap<>();
  private final Set<String> verbsImplyingImmutability = new HashSet<>();

  /**
   * As a memory and speed optimization, you can define specific RDF verbs that causes triples with
   * this verb to be stored as read-only.
   *
   * @param verb
   * @return this
   */
  public Store withVerbImplyingImmutability(String verb) {
    verbsImplyingImmutability.add(verb);
    return this;
  }

  /**
   * Add a verb-object pair that is always stored together, and as an immutable pair. This saves
   * both on memory and speed.
   *
   * @param v
   * @param o
   * @return rdf verb and object combination added
   */
  private RdfVerbObject addImmutableVerbObjectPair(String v, String o) {
    RdfVerbObject rvo = new RdfVerbObject(v, o);
    immutableVerbObjectPairs.computeIfAbsent(v, verb -> new HashMap<>()).put(o, rvo);
    return rvo;
  }

  private RdfVerbObject getImmutableVerbObjectPair(String v, String o) {
    Map<String, RdfVerbObject> objectsPerVerb = immutableVerbObjectPairs.get(v);
    if (objectsPerVerb == null) {
      if (verbsImplyingImmutability.contains(v)) {
        return addImmutableVerbObjectPair(v, o);
      }
    } else {
      return objectsPerVerb.get(o);
    }
    return null;
  }

  private RdfVerbLiteral getImmutableVerbObjectPair(String v, Literal o) {
    Map<Literal, RdfVerbLiteral> literalsPerVerb = immutableVerbLiteralPairs.get(v);
    if (literalsPerVerb == null) {
      if (!verbsImplyingImmutability.contains(v)) {
        return null;
      }
      literalsPerVerb = new HashMap<>();
      immutableVerbLiteralPairs.put(v, literalsPerVerb);
    }
    return literalsPerVerb.computeIfAbsent(o, k -> new RdfVerbLiteral(v, o));
  }

  /**
   * For a given relationship (transitive, e.g. broader/narrower skos relation) find the set of URIs
   * that relate to this subject n this relationship. The original subject URI is also included!
   *
   * @param subject
   * @param verbs
   * @return set of relations with minimally one member
   */
  public Set<String> getTransitiveRelationObjects(String subject, String... verbs) {
    Set<String> set = new HashSet<>();
    computeTransitiveRelationObjects(subject, set, verbs);
    return set;
  }

  public void write(PrintWriter pr, String... comments) {
    pr.println("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
    pr.println("<rdf:RDF");

    // Collect the name space abbreviations - the standard ones are always in
    for (String n : namespaceService.knownPrefixes()) {
      pr.println(String.format("xmlns:%s=\"%s\"", n, namespaceService.getFullForm(n)));
    }
    pr.println(">");
    for (String comment : comments) {
      pr.println(INDENT + "<!-- " + comment + "-->");
    }

    for (Entry<String, Set<Integer>> bs : bySubject.entrySet()) {
      pr.println(INDENT + "<rdf:Description rdf:about=\"" + bs.getKey() + "\">");
      for (Integer i : bs.getValue()) {
        pr.println(INDENT + INDENT + this.triples[i].asRdfXml(namespaceService));
      }
      pr.println(INDENT + "</rdf:Description>");
    }
    pr.println("</rdf:RDF>");
  }

  public NamespaceService getNamespaceService() {
    return this.namespaceService;
  }

  public boolean hasProperty(String uri, String property) {
    return this.getBySubjectAndVerb(uri, property).iterator().hasNext();
  }

  public boolean hasAnyProperty(String uri, Collection<String> properties) {
    return properties.stream()
        .anyMatch(property -> this.getBySubjectAndVerb(uri, property).iterator().hasNext());
  }

  private void computeTransitiveRelationObjects(String subject, Set<String> set, String... verbs) {
    // Avoid getting into a loop here.
    if (set.contains(subject)) {
      return; // bomb out
    }

    set.add(subject);
    for (String verb : verbs) {
      for (String object : getBySubjectAndVerb(subject, verb).objects()) {
        computeTransitiveRelationObjects(object, set, verbs);
      }
    }
  }

  @Override
  public Stream<RdfTriple> stream() {
    return Collection.super.stream();
  }

  private class SetStack {
    private final List<Set<String>> data = new ArrayList<>();

    public boolean contains(String s) {
      for (Set<String> d : data) {
        if (d.contains(s)) {
          return true;
        }
      }
      return false;
    }

    public SetStack add(String d) {
      data.add(singleton(d));
      return this;
    }

    public void add(Set<String> d) {
      data.add(d);
    }

    public Set<String> getLast() {
      return data.get(data.size() - 1);
    }
  }
}
