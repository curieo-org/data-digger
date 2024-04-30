package org.curieo.consumer;

import java.util.List;
import java.util.function.Predicate;

public record FilteredSink<T>(Predicate<T> predicate, Sink<T> embedded) implements Sink<List<T>> {
  @Override
  public void accept(List<T> t) {
    t.stream().filter(predicate).forEach(embedded);
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
