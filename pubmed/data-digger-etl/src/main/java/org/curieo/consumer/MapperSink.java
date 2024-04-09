package org.curieo.consumer;

import java.util.function.Function;

import lombok.Generated;
import lombok.Value;

@Generated @Value
public class MapperSink<T, Y> implements Sink<T> {
	Function<T, Y> mapper;
	Sink<Y> embedded;
	
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
