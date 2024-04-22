package org.curieo.consumer;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.curieo.utils.ListUtils;

@Data
@AllArgsConstructor
class AbstractSink<T> implements Sink<T> {
  final List<Extract<T>> extracts;
  final PreparedStatement insert;
  int insertions;
  int updates;
  int batchSize;

  public AbstractSink(List<Extract<T>> extracts, PreparedStatement insert, int batchSize) {
    this(extracts, insert, 0, 0, batchSize);
  }

  @Override
  public void accept(T t) {
    List<List<String>> exploded = new ArrayList<>();
    for (Extract<T> extract : extracts) {
      if (extract.spec().getType() == ExtractType.List) {
        exploded.add(extract.explode().apply(t));
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
          switch (extract.spec().getType()) {
            case SmallInt, Integer:
              insert.setInt(i, extract.getInteger(t));
              break;
            case BigInteger:
              insert.setLong(i, extract.getLong(t));
              break;
            case List:
              if (values.size() > e) {
                insert.setString(i, values.get(e++));
              } else {
                insert.setString(i, null);
              }
              break;
            case String, Text:
              insert.setString(i, extract.getString(t));
              break;
            default:
              break;
          }
        }
        insert.addBatch();
        insertions++;
        if (insertions % batchSize == 0) {
          executeAndClearBatch();
        }
      }
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  public void finalCall() {
    if (insertions > 0) {
      executeAndClearBatch();
    }
  }

  public int getTotalCount() {
    return insertions;
  }

  public int getUpdatedCount() {
    return updates;
  }

  private void executeAndClearBatch() {
    try {
      int[] updateCounts = insert.executeBatch();
      int updateSum = Arrays.stream(updateCounts).filter(i -> i > 0).sum();
      updates += updateSum;

      insert.clearBatch();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }
}
