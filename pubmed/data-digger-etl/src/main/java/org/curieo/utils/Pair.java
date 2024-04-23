package org.curieo.utils;

public record Pair<L, R>(L l, R r) {
  public static <L, R> Pair<L, R> of(L l, R r) {
    return new Pair<>(l, r);
  }
}
