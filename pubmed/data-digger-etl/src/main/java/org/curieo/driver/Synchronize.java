package org.curieo.driver;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Iterator;
import java.util.function.Function;
import java.util.stream.StreamSupport;
import org.curieo.consumer.AWSStorageSink;
import org.curieo.consumer.S3Helpers;
import org.curieo.consumer.Sink;
import org.curieo.model.FullTextTask;
import org.curieo.model.TS;
import org.curieo.model.TaskState;
import org.curieo.retrieve.s3.S3Stream;
import org.curieo.utils.Config;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;

public class Synchronize {

  /**
   * Synchronize a query with a remote file Synchronize remote file with table Remote file goes
   * first
   */
  public static <T, Y> void synchronizeLocalWithRemote(
      Iterable<T> remote, Sink<Y> sink, Function<T, Y> mapper) {
    StreamSupport.stream(remote.spliterator(), false).map(mapper).forEach(sink);
  }

  public static record S3(S3Client s3, String bucketName) {

    public S3(Config config) {
      this(S3Helpers.getS3Client(config), config.getEnv(AWSStorageSink.BUCKET_ENV_VAR, true, null));
    }

    public <T, Y> PutObjectResponse synchronizeRemoteWithLocal(
        Iterable<T> local, String objectKey, Function<T, String[]> mapper)
        throws S3Exception, IOException {
      Iterable<String[]> localData =
          StreamSupport.stream(local.spliterator(), false).map(mapper)::iterator;
      return S3Stream.writeTabSeparatedFile(s3, bucketName, objectKey, localData);
    }

    public PutObjectResponse synchronizeRemoteWithLocalQuery(
        Connection connection,
        String query,
        String objectKey,
        Function<ResultSet, String[]> recordMapper)
        throws SQLException, S3Exception, IOException {
      PutObjectResponse por = null;
      // https://jdbc.postgresql.org/documentation/query/#getting-results-based-on-a-cursor
      boolean autocommit = connection.getAutoCommit();
      connection.setAutoCommit(false);
      // give some hints as to how to read economically
      Statement statement =
          connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
      statement.setFetchSize(100);
      try (ResultSet resultSet = statement.executeQuery(query)) {
        Iterable<ResultSet> resultAsIterable =
            new Iterable<ResultSet>() {
              @Override
              public Iterator<ResultSet> iterator() {
                return new Iterator<ResultSet>() {
                  @Override
                  public boolean hasNext() {
                    try {
                      return resultSet.next();
                    } catch (SQLException e) {
                      throw new RuntimeException(e);
                    }
                  }

                  @Override
                  public ResultSet next() {
                    return resultSet;
                  }
                };
              }
            };
        por = synchronizeRemoteWithLocal(resultAsIterable, objectKey, recordMapper);
      }
      connection.setAutoCommit(autocommit); // back to original value
      return por;
    }

    public void synchronizeRemoteWithFullTextTable(
        Connection connection, String query, String objectKey)
        throws S3Exception, SQLException, IOException {
      synchronizeRemoteWithLocalQuery(
          connection, query, objectKey, Synchronize::extractFullTextTask);
    }

    public void synchronizeFullTextTableWithRemote(String objectKey, Sink<TS<FullTextTask>> sink) {
      synchronizeLocalWithRemote(
          S3Stream.readTabSeparatedFile(s3, bucketName, objectKey),
          sink,
          Synchronize::mapFullTextTask);
    }
  }

  private static TS<FullTextTask> mapFullTextTask(String[] remote) {
    // state is always 'completed' - or else it would not be there.
    FullTextTask job =
        new FullTextTask(
            // name, location, year, timestamp
            remote[0], remote[1], Integer.parseInt(remote[2]), TaskState.State.Completed);
    return new TS<>(job, Timestamp.valueOf(remote[3]));
  }

  private static String[] extractFullTextTask(ResultSet rs) {
    try {
      return new String[] {
        rs.getString(1), // identifier
        rs.getString(2), // location
        Integer.toString(rs.getInt(3)), // year
        // 4 (state) is skipped -- it MUST be 'complete'
        rs.getTimestamp(5).toString() // timestamp
      };
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }
}
