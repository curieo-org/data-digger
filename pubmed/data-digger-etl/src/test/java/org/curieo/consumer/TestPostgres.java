package org.curieo.consumer;

import static org.curieo.consumer.PostgreSQLClient.CreateFlags.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.curieo.utils.Config;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

class TestPostgres {

  @Test
  @Tag("slow")
  void testCreateDb() throws IOException, SQLException {
    Config config = new Config();

    String user = config.postgres_user;
    String password = config.postgres_password;
    try (PostgreSQLClient client = new PostgreSQLClient(null, user, password)) {
      client.createDatabase("test", OnExistSilentNoOp);
      assertThrows(RuntimeException.class, () -> client.createDatabase("test", OnExistFail));
      client.createDatabase("test", OnExistSilentOverride);
    }
  }

  @Test
  void testCursor() throws IOException, SQLException {
    Config config = new Config();

    String select = "SELECT identifier FROM records";
    String create = "DECLARE cursor_name CURSOR FOR " + select;
    String open = "OPEN cursor_name ";
    String fetch = "FETCH 10000 FROM cursor_name";
    String close = "CLOSE cursor_name ";
    try (PostgreSQLClient client = PostgreSQLClient.getPostgreSQLClient(config)) {
      int count;
      try (Statement statement = client.getConnection().createStatement()) {
        client.getConnection().setAutoCommit(false);
        count = 0;
        /*
        statement.execute(create);
        statement.execute(open);
        boolean notEmpty = true;
        while (notEmpty) {
          try (ResultSet set = statement.executeQuery(fetch)) {
            while (set.next())
              count++;
          }
        }
        statement.execute(close);
        */
        try (ResultSet set = statement.executeQuery(select)) {
          while (set.next()) count++;
        }
      }
      System.out.printf("Size = %d%n", count);
    }
  }
}
