package org.curieo.consumer;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lombok.AllArgsConstructor;
import lombok.Generated;
import lombok.Value;

@Generated @Value @AllArgsConstructor
public class CountingConsumer<T, Y> implements Sink<T> {
    private static final Logger LOGGER = LoggerFactory.getLogger(CountingConsumer.class);
	int loggingInterval;
	Map<Y, Integer> track;
	AtomicInteger count;
	Function<T, Y> mapper;
	BiFunction<Y, Integer, String> formatter;

	public CountingConsumer(int li, Function<T, Y> mapper, BiFunction<Y, Integer, String> formatter) {
		this(li, new ConcurrentHashMap<>(), new AtomicInteger(), mapper, formatter);
	}
	
	@Override
	public void accept(T t) {
		track.merge(mapper.apply(t), 1, (a, b) -> a + b);
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
}