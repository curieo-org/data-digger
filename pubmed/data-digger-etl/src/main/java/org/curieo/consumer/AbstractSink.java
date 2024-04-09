package org.curieo.consumer;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.curieo.rdf.HashSet;
import org.curieo.utils.ListUtils;

import lombok.Generated;
import lombok.Value;

@Generated @Value
class AbstractSink<T> implements Sink<T> {
	final List<Extract<T>> extracts;
	final PreparedStatement insert;
	final PreparedStatement delete;
	final AtomicInteger count;
	final AtomicInteger updated;
	int batchSize;
	final HashSet<String> keys;
	final Extract<T> keyExtractor;

	// guarantee uniqueness of keys.
	// we are assuming a linear read: later update overrides previous
	void guaranteeUniqueKeys(Set<String> keysFound) {
		try {
			for (String key : keysFound) {
				if (keys.contains(key)) {
					// execute batch
					insert.executeBatch();
					insert.clearBatch();
					
					// issue delete
					delete.setString(1, key);
					delete.execute();
					count.decrementAndGet();
					updated.incrementAndGet();
				}
				keys.add(key);
			}
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}
	
	@Override
	public void accept(T t) {
		List<List<String>> exploded = new ArrayList<>();
		for (Extract<T> extract : extracts) {
			if (extract.getSpec().getType() == ExtractType.List) {
				exploded.add(extract.getExplode().apply(t));
			}
		}

		exploded = ListUtils.cartesian(exploded);
		if (exploded.isEmpty()) {
			exploded = Collections.singletonList(Collections.emptyList());
		}

		try {
			for (List<String> values : exploded) {
				int e = 0;
				for (int i = 1; i <= extracts.size(); i++) {
					Extract<T> extract = extracts.get(i - 1);
					switch (extract.getSpec().getType()) {
						case SmallInt:
						case Integer:
							insert.setInt(i, extract.getInteger(t));
							break;
						case List:
							if (values.size() > e) {
								insert.setString(i, values.get(e++));
							} else {
								insert.setString(i, null);
							}
							break;
						case String:
						case Text:
							insert.setString(i, extract.getString(t));
							break;
						default:
							break;
					}
				}
				insert.addBatch();
			}
			int currentCount = count.incrementAndGet();
			if (currentCount%batchSize == 0) {
				insert.executeBatch();
				insert.clearBatch();
			}
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}
	
	public void finalCall() {
		int currentCount = count.get();
		if (currentCount%batchSize != 0) {
			try {
				insert.executeBatch();
				insert.clearBatch();
			} catch (SQLException e) {
				throw new RuntimeException(e);
			}
		}
	}

	public int getTotalCount() {
		return count.get();
	}
	public int getUpdatedCount() {
		return updated.get();
	}
}