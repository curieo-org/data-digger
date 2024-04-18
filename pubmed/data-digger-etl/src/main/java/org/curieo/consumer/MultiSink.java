package org.curieo.consumer;

import java.util.List;
import java.util.function.Function;

public record MultiSink<T, Y>(Function<T, List<Y>> mapper, Sink<List<Y>> embedded)
    implements Sink<T> {
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
