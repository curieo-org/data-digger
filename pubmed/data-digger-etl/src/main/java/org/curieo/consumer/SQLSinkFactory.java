package org.curieo.consumer;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import lombok.Generated;
import org.curieo.model.Authorship;
import org.curieo.model.FullTextRecord;
import org.curieo.model.FullTextTask;
import org.curieo.model.LinkedField;
import org.curieo.model.Metadata;
import org.curieo.model.PubmedTask;
import org.curieo.model.Reference;
import org.curieo.model.StandardRecord;
import org.curieo.model.TS;

/** Class to create record consumers into an SQL database. */
@Generated
public record SQLSinkFactory(PostgreSQLClient psqlClient, int batchSize, boolean useKeys) {
  public static final int DEFAULT_BATCH_SIZE = 100;
  public static final int IDENTIFIER_LENGTH = 100;

  public Sink<TS<PubmedTask>> createTasksSink() throws SQLException {

    FieldSpec name =
        FieldSpec.builder().field("name").type(ExtractType.String).size(60).nullable(false).build();
    FieldSpec state =
        FieldSpec.builder().field("state").type(ExtractType.SmallInt).nullable(false).build();
    FieldSpec groupName =
        FieldSpec.builder().field("job").type(ExtractType.String).size(60).nullable(false).build();

    TableSpec specification =
        TableSpec.of(
            "tasks",
            List.of(name, state, groupName, FieldSpec.timestamp("timestamp")),
            CompositeUniqueKey.of(name, groupName));

    createTable(specification);
    PreparedStatement upsert =
        upsertStatement(specification.name(), specification.fields(), "name", "job");

    List<FieldSpec> fieldSpecs = specification.fields();
    List<Extract<TS<PubmedTask>>> extracts = new ArrayList<>();
    extracts.add(fieldSpecs.get(1).extractString(ts -> ts.value().name()));
    extracts.add(fieldSpecs.get(2).extractInt(ts -> ts.value().state().ordinal()));
    extracts.add(fieldSpecs.get(3).extractString(ts -> ts.value().job()));
    extracts.add(fieldSpecs.get(4).extractTimestamp(TS::timestamp));

    return createAbstractSink(extracts, upsert);
  }

  public Sink<TS<FullTextTask>> createFullTextTasksSink(String tableName) throws SQLException {
    TableSpec tableSpec =
        TableSpec.of(
            tableName,
            List.of(
                new FieldSpec("identifier", ExtractType.String, 60, true),
                new FieldSpec("location", ExtractType.String, 200),
                new FieldSpec("year", ExtractType.SmallInt),
                new FieldSpec("state", ExtractType.SmallInt),
                FieldSpec.timestamp("timestamp")));

    createTable(tableSpec);
    PreparedStatement upsert = upsertStatement(tableName, tableSpec.fields(), "identifier");

    List<FieldSpec> fieldSpecs = tableSpec.fields();
    List<Extract<TS<FullTextTask>>> extracts = new ArrayList<>();
    extracts.add(fieldSpecs.get(1).extractString(ts -> ts.value().getIdentifier()));
    extracts.add(fieldSpecs.get(2).extractString(ts -> ts.value().getLocation()));
    extracts.add(fieldSpecs.get(3).extractInt(ts -> ts.value().getYear()));
    extracts.add(fieldSpecs.get(4).extractInt(ts -> ts.value().getTaskState().ordinal()));
    extracts.add(fieldSpecs.get(5).extractTimestamp(TS::timestamp));

    return createAbstractSink(extracts, upsert);
  }

  /**
   * Create a sink of authorships into a JDBC SQL table. Once the connection is closed, the sink is
   * invalidated. PostgreSQL dialect is assumed.
   *
   * @return a consumer.
   * @throws SQLException
   */
  public Sink<List<LinkedField<Authorship>>> createAuthorshipSink() throws SQLException {
    List<FieldSpec> fieldSpecs =
        Arrays.asList(
            FieldSpec.unique("publicationId", ExtractType.BigInteger),
            new FieldSpec("Ordinal", ExtractType.SmallInt),
            new FieldSpec("foreName", ExtractType.String, 40),
            new FieldSpec("lastName", ExtractType.String, 60),
            new FieldSpec("initials", ExtractType.String, 10),
            new FieldSpec("affiliation", ExtractType.List, 2000),
            new FieldSpec("yearActive", ExtractType.SmallInt),
            new FieldSpec("emailaddress", ExtractType.String, 60));
    String tableName = "Authorships";
    createTable(tableName, fieldSpecs);
    PreparedStatement insert = upsertStatement(tableName, fieldSpecs, "publicationId");

    List<Extract<LinkedField<Authorship>>> extracts = new ArrayList<>();
    extracts.add(fieldSpecs.get(0).extractLong(LinkedField::publicationId));
    extracts.add(fieldSpecs.get(1).extractInt(LinkedField::ordinal));
    extracts.add(fieldSpecs.get(2).extractString(l -> l.field().getForeName()));
    extracts.add(fieldSpecs.get(3).extractString(l -> l.field().getLastName()));
    extracts.add(fieldSpecs.get(4).extractString(l -> l.field().getInitials()));
    extracts.add(fieldSpecs.get(5).extractList(l -> l.field().getAffiliations()));
    extracts.add(fieldSpecs.get(6).extractInt(l -> l.field().getYearActive()));
    extracts.add(fieldSpecs.get(7).extractString(l -> l.field().getEmailAddress()));

    return new ListSink<>(createAbstractSink(extracts, insert, batchSize));
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
    List<FieldSpec> fieldSpecs =
        new ArrayList<>(
            Arrays.asList(
                FieldSpec.unique("articleId", ExtractType.BigInteger),
                new FieldSpec("ordinal", ExtractType.Integer),
                new FieldSpec("citation", ExtractType.String, 500)));
    // a variable number of identifiers
    for (String id : ids) {
      fieldSpecs.add(new FieldSpec(id, ExtractType.String, 30));
    }
    createTable(tableName, fieldSpecs);
    PreparedStatement upsert = upsertStatement(tableName, fieldSpecs, "articleId");

    List<Extract<LinkedField<Reference>>> extracts = new ArrayList<>();
    extracts.add(fieldSpecs.get(0).extractLong(LinkedField::publicationId));
    extracts.add(fieldSpecs.get(1).extractInt(LinkedField::ordinal));
    extracts.add(fieldSpecs.get(2).extractString(l -> l.field().getCitation()));

    // a variable number of identifiers
    for (String id : ids) {
      extracts.add(
          fieldSpecs
              .get(3)
              .extractString(
                  l ->
                      l.field().getIdentifiers().stream()
                          .filter(m -> m.key().equals(id))
                          .map(Metadata::value)
                          .findFirst()
                          .orElse(null)));
    }

    return new ListSink<>(createAbstractSink(extracts, upsert, batchSize));
  }

  /**
   * @param tableName name for the table
   * @param sourceIdentifier
   * @param targetIdentifier
   * @return
   * @throws SQLException
   */
  public Sink<List<Metadata>> createLinkoutTable(
      String tableName, String sourceIdentifier, String targetIdentifier) throws SQLException {
    List<FieldSpec> fieldSpecs =
        new ArrayList<>(
            Arrays.asList(
                FieldSpec.unique(sourceIdentifier, ExtractType.BigInteger),
                new FieldSpec(targetIdentifier, ExtractType.String, IDENTIFIER_LENGTH)));

    createTable(tableName, fieldSpecs);
    PreparedStatement upsert = upsertStatement(tableName, fieldSpecs, sourceIdentifier);

    List<Extract<Metadata>> extracts = new ArrayList<>();
    extracts.add(fieldSpecs.get(0).extractLong(s -> Long.parseLong(s.key())));
    extracts.add(fieldSpecs.get(1).extractString(Metadata::value));

    return new ListSink<>(createAbstractSink(extracts, upsert, batchSize));
  }

  /**
   * Create a sink of full records into a JDBC SQL table. Once the connection is closed, the sink is
   * invalidated. PostgreSQL dialect is assumed.
   *
   * @return a consumer.
   * @throws SQLException
   */
  public Sink<StandardRecord> createRecordSink() throws SQLException {
    List<FieldSpec> fieldSpecs =
        Arrays.asList(
            FieldSpec.unique("Identifier", ExtractType.BigInteger),
            new FieldSpec("Year", ExtractType.SmallInt),
            new FieldSpec("Record", ExtractType.Text),
            new FieldSpec("Origin", ExtractType.String, 60));
    String tableName = "Records";
    createLargeTable(tableName, fieldSpecs);

    PreparedStatement upsert = upsertStatement(tableName, fieldSpecs, "Identifier");

    List<Extract<StandardRecord>> extracts = new ArrayList<>();
    extracts.add(fieldSpecs.get(0).extractLong(StandardRecord::getNumericIdentifier));
    extracts.add(fieldSpecs.get(1).extractInt(StandardRecord::getYear));
    extracts.add(fieldSpecs.get(2).extractString(StandardRecord::toJson));
    extracts.add(fieldSpecs.get(3).extractString(StandardRecord::getOrigin));

    return createAbstractSink(extracts, upsert, batchSize);
  }

  public Sink<FullTextRecord> createPMCSink(String tableName) throws SQLException {
    List<FieldSpec> fieldSpecs =
        Arrays.asList(
            new FieldSpec("Identifier", ExtractType.String, 20, useKeys),
            new FieldSpec("Year", ExtractType.SmallInt),
            new FieldSpec("Record", ExtractType.Text, 0));
    createTable(tableName, fieldSpecs);
    PreparedStatement insert = insertStatement(tableName, fieldSpecs);
    if (useKeys) {
      insert = upsertStatement(tableName, fieldSpecs, "identifier");
    }

    List<Extract<FullTextRecord>> extracts = new ArrayList<>();
    extracts.add(fieldSpecs.get(0).extractString(FullTextRecord::getIdentifier));
    extracts.add(fieldSpecs.get(1).extractInt(FullTextRecord::getYear));
    extracts.add(fieldSpecs.get(2).extractString(FullTextRecord::getContent));

    return createAbstractSink(extracts, insert, batchSize);
  }

  private List<FieldSpec> createTableHelper(TableSpec specification) throws SQLException {
    psqlClient.execute(specification.toSql());
    return specification.fields();
  }

  private List<FieldSpec> createTableHelper(
      String tableName, List<FieldSpec> fieldSpecs, ExtractType idType) throws SQLException {
    fieldSpecs = new ArrayList<>(fieldSpecs);

    boolean missingIdentityColumn = fieldSpecs.stream().noneMatch(FieldSpec::isIdentityColumn);
    boolean missingTimestamp = fieldSpecs.stream().noneMatch(FieldSpec::isTimestamp);
    if (missingIdentityColumn) {
      fieldSpecs.addFirst(FieldSpec.identity(idType));
    }
    if (missingTimestamp) {
      fieldSpecs.add(FieldSpec.timestamp("timestamp", "now()"));
    }
    String create =
        String.format(
            "CREATE TABLE IF NOT EXISTS %s (%s)",
            tableName, fieldSpecs.stream().map(FieldSpec::toSql).collect(Collectors.joining(", ")));

    psqlClient.execute(create);

    return fieldSpecs;
  }

  private List<FieldSpec> createTable(TableSpec specification) throws SQLException {
    return createTableHelper(specification);
  }

  private List<FieldSpec> createTable(String tableName, List<FieldSpec> fieldSpecs)
      throws SQLException {
    // Use integer as we probably won't go over 2 billion entries
    return createTableHelper(tableName, fieldSpecs, ExtractType.Integer);
  }

  private List<FieldSpec> createLargeTable(String tableName, List<FieldSpec> fieldSpecs)
      throws SQLException {
    return createTableHelper(tableName, fieldSpecs, ExtractType.BigInteger);
  }

  private PreparedStatement insertStatement(String tableName, List<FieldSpec> fieldSpecs)
      throws SQLException {
    String insert =
        String.format(
            "INSERT INTO %s (%s) VALUES (%s)",
            tableName,
            fieldSpecs.stream().map(FieldSpec::getField).collect(Collectors.joining(", ")),
            fieldSpecs.stream().map(s -> "?").collect(Collectors.joining(", ")));

    return psqlClient.prepareStatement(insert);
  }

  private PreparedStatement upsertStatement(
      String tableName, List<FieldSpec> fieldSpecs, String... conflictColumns) throws SQLException {

    // Don't insert/update generative columns
    List<FieldSpec> fields =
        fieldSpecs.stream()
            .filter(
                f ->
                    f.getIdentityType() == null
                        || f.getIdentityType().equals(FieldSpec.IdentityType.Manual))
            .toList();
    String upsert =
        String.format(
            "insert into %s (%s) VALUES (%s) on conflict (%s) do update set %s;",
            tableName,
            fields.stream().map(FieldSpec::getField).collect(Collectors.joining(", ")),
            fields.stream().map(s -> "?").collect(Collectors.joining(", ")),
            String.join(", ", conflictColumns),
            fields.stream()
                .map(FieldSpec::getField)
                .filter(s -> Arrays.stream(conflictColumns).noneMatch(c -> c.equalsIgnoreCase(s)))
                .map(s -> String.format("%s = EXCLUDED.%s", s, s))
                .collect(Collectors.joining(", ")));

    return psqlClient.prepareStatement(upsert);
  }

  private static <T> AbstractSink<T> createAbstractSink(
      List<Extract<T>> extracts, PreparedStatement statement, int batchSize) {
    return new AbstractSink<>(extracts, statement, batchSize);
  }

  private static <T> AbstractSink<T> createAbstractSink(
      List<Extract<T>> extracts, PreparedStatement statement) {
    return new AbstractSink<>(extracts, statement, 1);
  }
}
