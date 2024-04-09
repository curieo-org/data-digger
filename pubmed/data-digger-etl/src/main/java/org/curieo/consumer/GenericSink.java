package org.curieo.consumer;

import java.util.Collections;

import lombok.Generated;
import lombok.Value;

@Generated @Value
class GenericSink<T> implements Sink<T> {
	AbstractSink<T> sink;

	@Override
	public void accept(T t) {
		if (sink.getKeyExtractor() != null) {
			sink.guaranteeUniqueKeys(Collections.singleton(sink.getKeyExtractor().getAsString(t)));
		}
		sink.accept(t);
	}

	@Override
	public void finalCall() {
		sink.finalCall();
	}

	@Override
	public int getTotalCount() {
		return sink.getTotalCount();
	}
	
	@Override
	public int getUpdatedCount() {
		return sink.getUpdatedCount();
	}
}