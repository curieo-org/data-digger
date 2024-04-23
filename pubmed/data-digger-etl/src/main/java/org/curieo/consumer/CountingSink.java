package org.curieo.consumer;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Function;
import lombok.AllArgsConstructor;
import lombok.Generated;
import lombok.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Generated
@Value
@AllArgsConstructor
public class CountingSink<T, Y> implements Sink<T> {
  private static final Logger LOGGER = LoggerFactory.getLogger(CountingSink.class);
  int loggingInterval;
  Map<Y, Integer> track;
  AtomicInteger count;
  Function<T, Y> mapper;
  BiFunction<Y, Integer, String> formatter;

  public CountingSink(int li, Function<T, Y> mapper, BiFunction<Y, Integer, String> formatter) {
    this(li, new ConcurrentHashMap<>(), new AtomicInteger(), mapper, formatter);
  }

  @Override
  public void accept(T t) {
    track.merge(mapper.apply(t), 1, Integer::sum);
    if (count.incrementAndGet() % loggingInterval == 0) {
      logProgress();
    }
  }

  public void logProgress() {
    for (Map.Entry<Y, Integer> log : track.entrySet()) {
      LOGGER.info(formatter.apply(log.getKey(), log.getValue()));
    }
  }

  @Override
  public void finalCall() {
    logProgress();
  }

  public int getTotalCount() {
    return count.get();
  }

  public int getUpdatedCount() {
    return 0;
  }
}
