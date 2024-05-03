package org.curieo.utils;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.commons.cli.ParseException;
import org.curieo.consumer.PostgreSQLClient;
import org.curieo.model.TaskState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TaskUtil {
  private static final Logger LOGGER = LoggerFactory.getLogger(TaskUtil.class);

  public static boolean checkPreviousJob(String previousJob, PostgreSQLClient postgreSQLClient)
      throws ParseException, IOException, SQLException {
    String query = "select count(*) as task_count from tasks where job = '%s' and state != %d";
    query = String.format(query, previousJob, TaskState.State.Completed.ordinal());

    List<String> retrievedColumns = Arrays.asList("task_count");

    List<Map<String, String>> result = postgreSQLClient.getQueryResult(query, retrievedColumns);
    if (result != null && result.size() > 0 && result.get(0).get("task_count").equals("0")) {
      return true;
    }

    return false;
  }
}
