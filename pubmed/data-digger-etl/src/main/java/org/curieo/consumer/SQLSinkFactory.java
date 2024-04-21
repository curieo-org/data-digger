package org.curieo.consumer;

import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;
import lombok.Generated;
import org.curieo.model.*;
import org.curieo.rdf.HashSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Class to create record consumers into an SQL database. */
@Generated
public record SQLSinkFactory(Connection connection, int batchSize, boolean useKeys) {
  private static final Logger LOGGER = LoggerFactory.getLogger(SQLSinkFactory.class);
  public static final int DEFAULT_BATCH_SIZE = 100;

  public Sink<Job> createJobsSink() throws SQLException {
    List<StorageSpec> storageSpecs =
        List.of(
            new StorageSpec("name", ExtractType.String, 60, true),
            new StorageSpec("state", ExtractType.SmallInt));
    String tableName = "Jobs";

    createTable(tableName, storageSpecs);
    PreparedStatement upsert = upsertStatement(tableName, storageSpecs, "name");

    List<Extract<Job>> extracts = new ArrayList<>();
    extracts.add(storageSpecs.get(0).extractString(Job::getName));
    extracts.add(storageSpecs.get(1).extractInt(Job::getJobStateInner));

    return new GenericSink<>(createAbstractSink(extracts, upsert, 1, connection, tableName), false);
  }

  /**
   * Create a sink of authorships into a JDBC SQL table. Once the connection is closed, the sink is
   * invalidated. PostgreSQL dialect is assumed.
   *
   * @return a consumer.
   * @throws SQLException
   */
  public Sink<List<LinkedField<Authorship>>> createAuthorshipSink() throws SQLException {
    List<StorageSpec> storageSpecs =
        Arrays.asList(
            StorageSpec.identity("publicationId", ExtractType.BigInteger),
            new StorageSpec("Ordinal", ExtractType.SmallInt),
            new StorageSpec("foreName", ExtractType.String, 40),
            new StorageSpec("lastName", ExtractType.String, 60),
            new StorageSpec("initials", ExtractType.String, 10),
            new StorageSpec("affiliation", ExtractType.List, 2000),
            new StorageSpec("yearActive", ExtractType.SmallInt),
            new StorageSpec("emailaddress", ExtractType.String, 60));
    String tableName = "Authorships";
    createTable(tableName, storageSpecs);
    PreparedStatement insert = upsertStatement(tableName, storageSpecs, "publicationId");

    List<Extract<LinkedField<Authorship>>> extracts = new ArrayList<>();
    extracts.add(storageSpecs.get(0).extractLong(LinkedField::publicationId));
    extracts.add(storageSpecs.get(1).extractInt(LinkedField::ordinal));
    extracts.add(storageSpecs.get(2).extractString(l -> l.field().getForeName()));
    extracts.add(storageSpecs.get(3).extractString(l -> l.field().getLastName()));
    extracts.add(storageSpecs.get(4).extractString(l -> l.field().getInitials()));
    extracts.add(storageSpecs.get(5).extractList(l -> l.field().getAffiliations()));
    extracts.add(storageSpecs.get(6).extractInt(l -> l.field().getYearActive()));
    extracts.add(storageSpecs.get(7).extractString(l -> l.field().getEmailAddress()));

    return new ListSink<>(createAbstractSink(extracts, insert, batchSize, connection, tableName));
  }

  /**
   * Create a sink of references into a JDBC SQL table. Once the connection is closed, the sink is
   * invalidated. PostgreSQL dialect is assumed.
   *
   * @return a consumer.
   * @throws SQLException
   */
  public Sink<List<LinkedField<Reference>>> createReferenceSink(String... ids) throws SQLException {
    String tableName = "ReferenceTable";
    List<StorageSpec> storageSpecs =
        new ArrayList<>(
            Arrays.asList(
                StorageSpec.identity("articleId", ExtractType.BigInteger),
                new StorageSpec("ordinal", ExtractType.Integer),
                new StorageSpec("citation", ExtractType.String, 500)));
    // a variable number of identifiers
    for (String id : ids) {
      storageSpecs.add(new StorageSpec(id, ExtractType.String, 30));
    }
    createTable(tableName, storageSpecs);
    PreparedStatement upsert = upsertStatement(tableName, storageSpecs, "articleId");

    List<Extract<LinkedField<Reference>>> extracts = new ArrayList<>();
    extracts.add(storageSpecs.get(0).extractLong(LinkedField::publicationId));
    extracts.add(storageSpecs.get(1).extractInt(LinkedField::ordinal));
    extracts.add(storageSpecs.get(2).extractString(l -> l.field().getCitation()));

    // a variable number of identifiers
    for (String id : ids) {
      extracts.add(
          storageSpecs
              .get(3)
              .extractString(
                  l ->
                      l.field().getIdentifiers().stream()
                          .filter(m -> m.key().equals(id))
                          .map(Metadata::value)
                          .findFirst()
                          .orElse(null)));
    }

    return new ListSink<>(createAbstractSink(extracts, upsert, batchSize, connection, tableName));
  }

  /**
   * @param sourceIdentifier
   * @param targetIdentifier
   * @return
   * @throws SQLException
   */
  public Sink<List<Metadata>> createLinkTable(String sourceIdentifier, String targetIdentifier)
      throws SQLException {
    String tableName = "LinkTable";
    List<StorageSpec> storageSpecs =
        new ArrayList<>(
            Arrays.asList(
                StorageSpec.identity(sourceIdentifier, ExtractType.BigInteger),
                new StorageSpec(targetIdentifier, ExtractType.String, 20)));

    createTable(tableName, storageSpecs);
    PreparedStatement upsert = upsertStatement(tableName, storageSpecs, sourceIdentifier);

    List<Extract<Metadata>> extracts = new ArrayList<>();
    extracts.add(storageSpecs.get(0).extractLong(s -> Long.parseLong(s.key())));
    extracts.add(storageSpecs.get(1).extractString(Metadata::value));

    return new ListSink<>(createAbstractSink(extracts, upsert, batchSize, connection, tableName));
  }

  /**
   * Create a sink of full records into a JDBC SQL table. Once the connection is closed, the sink is
   * invalidated. PostgreSQL dialect is assumed.
   *
   * @return a consumer.
   * @throws SQLException
   */
  public Sink<StandardRecord> createRecordSink() throws SQLException {
    List<StorageSpec> storageSpecs =
        Arrays.asList(
            StorageSpec.identity("Identifier", ExtractType.BigInteger),
            new StorageSpec("Year", ExtractType.SmallInt),
            new StorageSpec("Record", ExtractType.Text),
            new StorageSpec("Origin", ExtractType.String, 60));
    String tableName = "Records";
    createLargeTable(tableName, storageSpecs);

    PreparedStatement upsert = upsertStatement(tableName, storageSpecs, "identifier");

    List<Extract<StandardRecord>> extracts = new ArrayList<>();
    extracts.add(storageSpecs.get(0).extractLong(StandardRecord::getNumericIdentifier));
    extracts.add(storageSpecs.get(1).extractInt(StandardRecord::getYear));
    extracts.add(storageSpecs.get(2).extractString(StandardRecord::toJson));
    extracts.add(storageSpecs.get(3).extractString(StandardRecord::getOrigin));

    return new GenericSink<>(
        createAbstractSink(extracts, upsert, batchSize, connection, tableName), false);
  }

  public Sink<FullTextRecord> createPMCSink(String tableName) throws SQLException {
    List<StorageSpec> storageSpecs =
        Arrays.asList(
            new StorageSpec("Identifier", ExtractType.String, 20, useKeys),
            new StorageSpec("Record", ExtractType.Text, 0));
    createTable(tableName, storageSpecs);
    PreparedStatement insert = insertStatement(tableName, storageSpecs);
    if (useKeys) {
      insert = upsertStatement(tableName, storageSpecs, "identifier");
    }

    List<Extract<FullTextRecord>> extracts = new ArrayList<>();
    extracts.add(storageSpecs.get(0).extractString(FullTextRecord::getIdentifier));
    extracts.add(storageSpecs.get(1).extractString(FullTextRecord::getContent));

    return new GenericSink<>(
        createAbstractSink(extracts, insert, batchSize, connection, tableName), true);
  }

  private void createTableHelper(
      String tableName, List<StorageSpec> storageSpecs, ExtractType idType) throws SQLException {
    storageSpecs = new ArrayList<>(storageSpecs);

    boolean missingIdentityColumn = storageSpecs.stream().noneMatch(StorageSpec::isIdentityColumn);
    if (missingIdentityColumn) {
      storageSpecs.addFirst(StorageSpec.identity(idType));
    }
    storageSpecs.add(new StorageSpec("Timestamp", ExtractType.Timestamp, "now()"));
    String creation =
        String.format(
            "CREATE TABLE IF NOT EXISTS %s (%s)",
            tableName,
            storageSpecs.stream().map(StorageSpec::toSql).collect(Collectors.joining(", ")));
    Statement statement = connection.createStatement();
    statement.execute(creation);
  }

  private void createTable(String tableName, List<StorageSpec> storageSpecs) throws SQLException {
    // Use integer as we probably won't go over 2 billion entries
    createTableHelper(tableName, storageSpecs, ExtractType.Integer);
  }

  private void createLargeTable(String tableName, List<StorageSpec> storageSpecs)
      throws SQLException {
    createTableHelper(tableName, storageSpecs, ExtractType.BigInteger);
  }

  private PreparedStatement insertStatement(String tableName, List<StorageSpec> storageSpecs)
      throws SQLException {
    String insert =
        String.format(
            "INSERT INTO %s (%s) VALUES (%s)",
            tableName,
            storageSpecs.stream().map(StorageSpec::getField).collect(Collectors.joining(", ")),
            storageSpecs.stream().map(s -> "?").collect(Collectors.joining(", ")));
    return connection.prepareStatement(insert);
  }

  private PreparedStatement upsertStatement(
      String tableName, List<StorageSpec> storageSpecs, String conflictColumn) throws SQLException {
    // String idColumn =
    //     storageSpecs.stream()
    //         .filter(StorageSpec::isIdColumn)
    //         .findFirst()
    //         .map(StorageSpec::getField)
    //         .orElse("id");
    String insert =
        String.format(
            "insert into %s (%s) VALUES (%s) on conflict (%s) do update set %s, timestamp = now();",
            tableName,
            storageSpecs.stream().map(StorageSpec::getField).collect(Collectors.joining(", ")),
            storageSpecs.stream().map(s -> "?").collect(Collectors.joining(", ")),
            conflictColumn,
            storageSpecs.stream()
                .map(StorageSpec::getField)
                .filter(s -> !Objects.equals(s, conflictColumn))
                .map(s -> String.format("%s = EXCLUDED.%s", s, s))
                .collect(Collectors.joining(", ")));

    return connection.prepareStatement(insert);
  }

  private static <T> AbstractSink<T> createAbstractSink(
      List<Extract<T>> extracts,
      PreparedStatement insert,
      int batchSize,
      Connection connection,
      String tableName) {
    Set<String> keys = new HashSet<>(); // org.curieo.sources.IdentifierSet();
    /*
    Optional<Extract<T>> uniqueOpt =
        extracts.stream().filter(extract -> extract.spec().isUnique()).findFirst();
    Extract<T> keyExtractor = null;
    PreparedStatement deleteStatement = null;
    if (uniqueOpt.isPresent()) {
      keyExtractor = uniqueOpt.get();
      String query = String.format("SELECT %s FROM %s", keyExtractor.spec().getField(), tableName);
      keys = PostgreSQLClient.retrieveSetOfStrings(connection, query);
      LOGGER.info("Read {} keys from {}", keys.size(), tableName);
      deleteStatement =
          connection.prepareStatement(
              String.format(
                  "DELETE FROM %s WHERE %s = ?", tableName, keyExtractor.spec().getField()));
    }

     */
    return new AbstractSink<>(extracts, insert, null, batchSize, keys, null);
  }
}
