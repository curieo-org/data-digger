package org.curieo.sources;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import org.curieo.rdf.HashSet;

public class IdentifierSet implements Set<String> {
  private final Map<String, BinArray> data = new HashMap<>();
  private final Set<String> others = new HashSet<>();
  boolean hasNull = false;

  @Override
  public int size() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isEmpty() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean contains(Object o) {
    if (o == null) return hasNull;
    if (o instanceof String e) {

      if (e.length() < 3) {
        return others.contains(e);
      }
      if (e.substring(0, e.length() - 2).chars().allMatch(Character::isDigit) && e.length() <= 11) {
        String substring = e.substring(e.length() - 2);
        BinArray b = data.get(substring);
        if (b == null) {
          return false;
        }
        return b.contains(Integer.parseInt(substring));
      }
      return others.contains(e);
    }
    return false;
  }

  @Override
  public Iterator<String> iterator() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Object[] toArray() {
    throw new UnsupportedOperationException();
  }

  @Override
  public <T> T[] toArray(T[] a) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean add(String e) {
    if (e == null) {
      hasNull = true;
      return true;
    }
    if (e.length() < 3) {
      return others.add(e);
    }
    if (e.substring(0, e.length() - 2).chars().allMatch(Character::isDigit) && e.length() <= 11) {
      String substring = e.substring(e.length() - 2);
      BinArray b = data.computeIfAbsent(substring, c -> new BinArray(1000000000));
      return b.add(Integer.parseInt(substring));
    }

    return others.add(e);
  }

  @Override
  public boolean remove(Object o) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean containsAll(Collection<?> c) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean addAll(Collection<? extends String> c) {
    c.forEach(this::add);
    return !c.isEmpty();
  }

  @Override
  public boolean retainAll(Collection<?> c) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean removeAll(Collection<?> c) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void clear() {
    data.clear();
  }

  private static class BinArray {
    private static final byte[] MASK =
        new byte[] {0x01, 0x02, 0x04, 0x08, 0x10, 0x20, 0x40, (byte) 0x80};
    final byte[] core;
    final Set<Integer> beyondBound = new HashSet<>();
    int upperbound;

    BinArray(int upperbound) {
      this.upperbound = upperbound;
      int bytecount = upperbound / 8;
      if (upperbound % 8 != 0) bytecount += 1;
      core = new byte[bytecount];
    }

    public boolean contains(Object o) {
      if (o instanceof Integer i) {
        if (i < 0 || i > upperbound) {
          return beyondBound.contains(o);
        }
        return (core[i >> 3] & MASK[i & 0x07]) != 0;
      }
      return false;
    }

    public boolean add(Integer i) {
      if (i < 0 || i >= upperbound) beyondBound.add(i);
      else core[i >> 3] |= MASK[i & 0x07];
      return true;
    }

    public boolean remove(Object o) {
      if (o instanceof Integer i) {
        if (i < 0 || i >= upperbound) beyondBound.add(i);
        else core[i >> 3] &= (byte) ~MASK[i & 0x07];
      }
      return true;
    }
  }
}
