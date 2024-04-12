package org.curieo.consumer;

import java.util.Collections;

import lombok.Generated;
import lombok.Value;

@Generated @Value
class GenericSink<T> implements Sink<T> {
	AbstractSink<T> sink;
	boolean doNotDownloadDuplicates;

	@Override
	public void accept(T t) {
		boolean skip = false;
		if (sink.getKeyExtractor() != null) {
			String key = sink.getKeyExtractor().getAsString(t);
			if (doNotDownloadDuplicates) {
				skip = sink.isPresent(key);
			}
			else {
				sink.guaranteeUniqueKeys(Collections.singleton(key));
			}
		}
		if (!skip) {
			sink.accept(t);
		}
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