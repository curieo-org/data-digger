package org.curieo.consumer;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.curieo.utils.ListUtils;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Generated;

@Generated @Data @AllArgsConstructor
class AbstractSink<T> implements Sink<T> {
	final List<Extract<T>> extracts;
	final PreparedStatement insert;
	final PreparedStatement delete;
	int insertions;
	int deletions;
	int deletesInBatch;
	int batchSize;
	final Set<String> keys;
	final Extract<T> keyExtractor;

	public AbstractSink(List<Extract<T>> extracts, PreparedStatement insert, PreparedStatement deleteStatement,
			int batchSize, Set<String> keys, Extract<T> keyExtractor) {
		this(extracts, insert, deleteStatement, 0, 0, 0, batchSize, keys, keyExtractor);
	}
	
	// guarantee uniqueness of keys.
	// we are assuming a linear read: later update overrides previous
	void guaranteeUniqueKeys(Set<String> keysFound) {
		try {
			for (String key : keysFound) {
				if (keys.contains(key)) {
					
					// issue delete
					delete.setString(1, key);
					delete.addBatch();
					deletions++;
					deletesInBatch++;
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
				insertions++;
				if (insertions%batchSize == 0) {
					executeAndClearBatch();
				}
			}
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}
	
	public void finalCall() {
		if (insertions%batchSize != 0) {
			executeAndClearBatch();
		}
	}

	public int getTotalCount() {
		return insertions;
	}
	public int getUpdatedCount() {
		return deletions;
	}
	
	private void executeAndClearBatch() {
		try {
			if (deletesInBatch != 0) {
				delete.executeBatch();
				delete.clearBatch();
				deletesInBatch = 0;
			}
			insert.executeBatch();
			insert.clearBatch();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	public boolean isPresent(String key) {
		return keys.contains(key);
	}
}