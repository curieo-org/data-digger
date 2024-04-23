package org.curieo.consumer;

import java.util.function.Function;

public record MapSink<T, Y>(Function<T, Y> mapper, Sink<Y> embedded) implements Sink<T> {
  @Override
  public void accept(T t) {
    embedded.accept(mapper.apply(t));
  }

  @Override
  public void finalCall() {
    embedded.finalCall();
  }

  @Override
  public int getTotalCount() {
    return embedded.getTotalCount();
  }

  @Override
  public int getUpdatedCount() {
    return embedded.getUpdatedCount();
  }
}
