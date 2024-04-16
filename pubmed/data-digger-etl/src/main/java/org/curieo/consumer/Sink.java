package org.curieo.consumer;

import java.util.function.Consumer;
import lombok.Generated;
import lombok.Value;

public interface Sink<T> extends Consumer<T> {
  void finalCall();

  int getTotalCount();

  int getUpdatedCount();

  default Sink<T> concatenate(Sink<T> other) {
    if (other == null) return this;
    return new Concat<>(this, other);
  }

  @Generated
  @Value
  class Concat<T> implements Sink<T> {
    Sink<T> s1;
    Sink<T> s2;

    @Override
    public void accept(T t) {
      s1.accept(t);
      s2.accept(t);
    }

    @Override
    public void finalCall() {
      s1.finalCall();
      s2.finalCall();
    }

    @Override
    public int getTotalCount() {
      return s1.getTotalCount() + s2.getTotalCount();
    }

    @Override
    public int getUpdatedCount() {
      return s1.getUpdatedCount() + s2.getUpdatedCount();
    }
  }
}
