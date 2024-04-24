package org.curieo.consumer;

import com.zaxxer.hikari.HikariDataSource;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.curieo.model.TS;
import org.curieo.model.Task;
import org.curieo.utils.Config;
import org.curieo.utils.HashSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class encapsulates the database connection to PostgreSQL server using JDBC.
 *
 * @author M Doornenbal for Curieo Technologies BV
 */
public class PostgreSQLClient implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(PostgreSQLClient.class);
  public static final String LOCALDB = "jdbc:postgresql://localhost:5432/postgres";
  private final HikariDataSource dataSource;
  private final String connectionString;

  public enum CreateFlags {
    OnExistFail,
    OnExistSilentNoOp,
    OnExistSilentOverride
  }

  public PostgreSQLClient(String dbUrl, String user, String password) throws SQLException {
    // Connect method #2
    if (dbUrl == null) {
      dbUrl = LOCALDB;
    }
    HikariDataSource ds = new HikariDataSource();
    ds.setJdbcUrl(dbUrl);
    ds.setUsername(user);
    ds.setPassword(password);
    ds.setMinimumIdle(10);
    ds.setMaximumPoolSize(45);
    // ds.setKeepaliveTime(60000);
    // ds.setIdleTimeout(120000);
    // ds.setLeakDetectionThreshold(150000);
    // ds.setMaxLifetime(180000);
    // ds.setConnectionTimeout(3000);
    // ds.setValidationTimeout(2500);
    ds.setRegisterMbeans(true);
    ds.setAllowPoolSuspension(false);
    ds.setAutoCommit(true);

    connectionString = dbUrl;
    dataSource = ds;
  }

  public Connection getConnection() throws SQLException {
    return dataSource.getConnection();
  }

  public PreparedStatement prepareStatement(String sql) throws SQLException {
    PreparedStatement prepStmt = getConnection().prepareStatement(sql);
    prepStmt.closeOnCompletion();
    return prepStmt;
  }

  public void execute(String sql) throws SQLException {
    try (Connection connection = getConnection()) {
      connection.createStatement().execute(sql);
    }
  }

  public static PostgreSQLClient getPostgreSQLClient(Config config) throws SQLException {
    String user = config.postgres_user;
    String database = config.postgres_database;
    String password = config.postgres_password;
    return new PostgreSQLClient(database, user, password);
  }

  public static Set<String> retrieveSetOfStrings(Connection connection, String query)
      throws SQLException {
    Set<String> keys = new HashSet<>();
    // https://jdbc.postgresql.org/documentation/query/#getting-results-based-on-a-cursor
    boolean autocommit = connection.getAutoCommit();
    connection.setAutoCommit(false);
    // give some hints as to how to read economically
    Statement statement =
        connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    statement.setFetchSize(100);
    try (ResultSet resultSet = statement.executeQuery(query)) {
      while (resultSet.next()) {
        keys.add(resultSet.getString(1));
      }
    }
    connection.setAutoCommit(autocommit); // back to original value
    return keys;
  }

  public static Map<String, TS<Task>> retrieveJobTasks(Connection connection, String group)
      throws SQLException {

    Map<String, TS<Task>> tasks = new HashMap<>();

    // https://jdbc.postgresql.org/documentation/query/#getting-results-based-on-a-cursor
    boolean autocommit = connection.getAutoCommit();
    connection.setAutoCommit(false);
    // give some hints as to how to read economically
    Statement statement =
        connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    statement.setFetchSize(100);
    String query =
        String.format(
            "select name, state, timestamp from tasks where job = '%s'", escapeSingleQuotes(group));
    try (ResultSet resultSet = statement.executeQuery(query)) {
      while (resultSet.next()) {
        Task task =
            new Task(
                resultSet.getString(1),
                Task.State.fromInt(resultSet.getInt(2)),
                resultSet.getString(3));

        TS<Task> jobTs = new TS<>(task, resultSet.getTimestamp(3));
        tasks.put(task.name(), jobTs);
      }
    }
    connection.setAutoCommit(autocommit); // back to original value
    return tasks;
  }

  /**
   * Create a database.
   *
   * @param databaseName
   * @param flags
   * @throws SQLException
   */
  public void createDatabase(String databaseName, CreateFlags flags) throws SQLException {
    int i = connectionString.lastIndexOf('/');
    String dbConnectionString =
        i != -1
            ? connectionString.substring(0, i + 1) + databaseName
            : connectionString + databaseName;

    try (Statement stmt = getConnection().createStatement()) {
      String testIfExists =
          String.format(
              "SELECT * FROM pg_database WHERE datname='%s'", escapeSingleQuotes(databaseName));

      try (ResultSet resultSet = stmt.executeQuery(testIfExists)) {
        if (resultSet.next()) {
          // there is already a database with this name
          switch (flags) {
            case OnExistFail:
              stmt.close();
              throw new RuntimeException(String.format("Database %s already exists", databaseName));
            case OnExistSilentNoOp:
              return;
            case OnExistSilentOverride:
              dropDatabase(databaseName);
          }
        }
      }

      String sql = String.format("CREATE DATABASE %s", escapeSingleQuotes(databaseName));
      int result = stmt.executeUpdate(sql);
      LOGGER.info("Executed create database with success {}", result);
    }
  }

  public void dropDatabase(String databaseName) throws SQLException {
    // create three connections to three different databases on localhost
    String sql = String.format("DROP DATABASE %s", databaseName);
    try (Statement stmt = getConnection().createStatement()) {
      int result = stmt.executeUpdate(sql);
      LOGGER.info("Dropped database with success {}", result);
    }
  }

  public static String escapeSingleQuotes(String s) {
    StringBuilder sb = new StringBuilder();
    for (char c : s.toCharArray()) {
      if (c == '\'') {
        sb.append("''");
      } else {
        sb.append(c);
      }
    }
    return sb.toString();
  }

  @Override
  public void close() {
    dataSource.close();
  }
}
