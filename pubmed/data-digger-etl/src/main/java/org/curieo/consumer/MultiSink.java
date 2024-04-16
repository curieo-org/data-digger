package org.curieo.consumer;

import java.util.List;
import java.util.function.Function;
import lombok.Generated;
import lombok.Value;

@Generated
@Value
public class MultiSink<T, Y> implements Sink<T> {
  Function<T, List<Y>> mapper;
  Sink<List<Y>> embedded;

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
