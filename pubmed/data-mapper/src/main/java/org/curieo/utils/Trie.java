package org.curieo.utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;
import lombok.Generated;
import lombok.Value;

public class Trie<T> {
  private int lower = -1;
  private int upper = -1;
  private Trie<T>[] next;
  private T value;

  public T get(String s) {
    int pos = 0;
    Trie<T> current = this;
    while (pos != s.length()) {
      current = current.next(s.charAt(pos));
      if (current == null) {
        return null;
      }
      pos = pos + 1;
    }
    return current.value;
  }

  public boolean containsKey(String s) {
    return get(s) != null;
  }

  public TrieHit findLongestUpTo(String s, int pos, int length) {
    Trie<T> longest = this;
    int longueur = 0;
    int originalPos = pos;
    Trie<T> current = this;
    while (pos != length) {
      current = current.next(s.charAt(pos));
      if (current == null) {
        if (longest.value == null) {
          return null;
        }
        return new TrieHit(longest.value, longueur);
      }
      pos = pos + 1;
      if (current.value != null) {
        longest = current;
        longueur = pos - originalPos;
      }
    }
    if (longest.value == null) {
      return null;
    }
    return new TrieHit(longest.value, longueur);
  }

  public T put(String s, T value) {
    return add(s, 0, value);
  }

  /**
   * Assign values to all intermediate states that lead to a unique value. In other words, suppose
   * you have these strings: - marius - mario - luigi - lorenzo all nodes from the 'u' and 'o' in
   * (marius/mario) get the value that is associated with 'marius' and 'mario' respectively, same
   * for all nodes after 'uigi' and 'orenzo'. The allows you to parse partial strings (prefixes).
   */
  public void markUnique() {
    Collection<T> values = values();
    if (values.size() == 1) {
      T unique = values.iterator().next();
      this.value = unique;
    }
    if (next != null) {
      Stream.of(next).filter(Objects::nonNull).forEach(Trie::markUnique);
    }
  }

  public Collection<T> values() {
    List<T> vals = new ArrayList<>();
    if (value != null) {
      vals.add(value);
    }
    if (next == null) {
      return vals;
    }
    Stream.of(next).filter(Objects::nonNull).map(Trie::values).forEach(vals::addAll);
    return vals;
  }

  private T add(String s, int pos, T value) {
    if (s.length() == pos) {
      T current = this.value;
      this.value = value;
      return current;
    }
    int character = s.charAt(pos);
    makeRoom(character);
    if (next[character - lower] == null) {
      next[character - lower] = new Trie<>();
    }
    return (T) next[character - lower].add(s, pos + 1, value);
  }

  @SuppressWarnings("unchecked")
  private void makeRoom(int character) {
    // initial
    if (lower == -1) {
      lower = character;
      upper = character + 1;
      next = new Trie[1];
      return;
    }
    if (character >= lower && character < upper) {
      return;
    }
    if (character + 1 > upper) {
      int newUpper = character + 1;
      next = Arrays.copyOf(next, newUpper - lower);
      upper = newUpper;
    } else { // (character < lower)
      int newLower = character;
      Trie<T>[] newNext = new Trie[upper - newLower];
      for (int i = lower; i < upper; i++) {
        newNext[i - newLower] = next[i - lower];
      }
      next = newNext;
      lower = newLower;
    }
  }

  public Trie<T> next(char character) {
    if (character < lower || character >= upper) {
      return null;
    }
    return next[character - lower];
  }

  @Generated
  @Value
  public class TrieHit {
    T value;
    int length;
  }
}
