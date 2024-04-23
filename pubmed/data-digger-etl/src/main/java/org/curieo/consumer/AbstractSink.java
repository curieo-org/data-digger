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
  final PreparedStatement statement;
  int insertions;
  int updates;
  int batchSize;

  public AbstractSink(List<Extract<T>> extracts, PreparedStatement statement, int batchSize) {
    this(extracts, statement, 0, 0, batchSize);
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
              statement.setInt(i, extract.getInteger(t));
              break;
            case BigInteger:
              statement.setLong(i, extract.getLong(t));
              break;
            case List:
              if (values.size() > e) {
                statement.setString(i, values.get(e++));
              } else {
                statement.setString(i, null);
              }
              break;
            case String, Text:
              statement.setString(i, extract.getString(t));
              break;
            case Timestamp:
              statement.setTimestamp(i, extract.getTimestamp(t));
              break;
          }
        }
        statement.addBatch();
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
    try {
      if (!statement.isClosed()) {
        statement.close();
      }
    } catch (SQLException e) {
      throw new RuntimeException(e);
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
      int[] updateCounts = statement.executeBatch();
      int updateSum = Arrays.stream(updateCounts).filter(i -> i > 0).sum();
      updates += updateSum;
      statement.clearBatch();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }
}
