package org.curieo.consumer;

import java.util.function.Function;

import lombok.Generated;
import lombok.Value;

@Generated @Value
public class MultiSink<T, Y> implements Sink<T> {
	Function<T, Iterable<Y>> mapper;
	Sink<Y> embedded;
	
	@Override
	public void accept(T t) {
		for (Y y : mapper.apply(t)) {
			embedded.accept(y);
		}
	}
	
	public void finalCall() {
		embedded.finalCall();
	}
	public int getTotalCount() {
		return embedded.getTotalCount();
	}
}
