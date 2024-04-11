package org.curieo.consumer;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.curieo.model.Authorship;
import org.curieo.model.LinkedField;
import org.curieo.model.Metadata;
import org.curieo.model.Reference;
import org.curieo.model.StandardRecord;
import org.curieo.rdf.HashSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lombok.Generated;
import lombok.Value;

/**
 * Class to create record consumers into an SQL database.
 */
@Generated @Value
public class SQLSinkFactory {
	private static final Logger LOGGER = LoggerFactory.getLogger(SQLSinkFactory.class);
	public static final int DEFAULT_BATCH_SIZE = 100;

	Connection connection;
	int batchSize;
	boolean useKeys;
	
	/**
	 * Create a sink of authorships into a JDBC SQL table.
	 * Once the connection is closed, the sink is invalidated.
	 * PostgreSQL dialect is assumed.
	 * 
	 * @return a consumer.
	 * @throws SQLException
	 */
	public Sink<List<LinkedField<Authorship>>> createAuthorshipSink() throws SQLException {
		List<StorageSpec> storageSpecs = Arrays.asList(
				new StorageSpec("Ordinal", ExtractType.SmallInt),
				new StorageSpec("foreName", ExtractType.String, 40),
				new StorageSpec("lastName", ExtractType.String, 60),
				new StorageSpec("initials", ExtractType.String, 10),
				new StorageSpec("affiliation", ExtractType.List, 2000),
				new StorageSpec("yearActive", ExtractType.Integer),
				new StorageSpec("emailaddress", ExtractType.String, 60),
				new StorageSpec("publicationId", ExtractType.String, 30, useKeys));
		String tableName = "Authorships";
		createTable(tableName, storageSpecs);
		PreparedStatement insert = insertStatement(tableName, storageSpecs);
		
		List<Extract<LinkedField<Authorship>>> extracts = new ArrayList<>();
		extracts.add(storageSpecs.get(0).extractInt(LinkedField::getOrdinal));
		extracts.add(storageSpecs.get(1).extractString(l -> l.getField().getForeName()));
		extracts.add(storageSpecs.get(2).extractString(l -> l.getField().getLastName()));
		extracts.add(storageSpecs.get(3).extractString(l -> l.getField().getInitials()));
		extracts.add(storageSpecs.get(4).extractList(l -> l.getField().getAffiliations()));
		extracts.add(storageSpecs.get(5).extractInt(l -> l.getField().getYearActive()));
		extracts.add(storageSpecs.get(6).extractString(l -> l.getField().getEmailAddress()));
		extracts.add(storageSpecs.get(7).extractString(LinkedField::getPublicationId));

		return new ListSink<>(createAbstractSink(extracts, insert, batchSize, connection, tableName));
	}

	/**
	 * Create a sink of references into a JDBC SQL table.
	 * Once the connection is closed, the sink is invalidated.
	 * PostgreSQL dialect is assumed.
	 * 
	 * @return a consumer.
	 * @throws SQLException
	 */
	public Sink<List<LinkedField<Reference>>> createReferenceSink(String... ids) throws SQLException {
		List<StorageSpec> storageSpecs = new ArrayList<>();
		String tableName = "ReferenceTable";
		storageSpecs.addAll(Arrays.asList(
				new StorageSpec("articleId", ExtractType.String, 20, useKeys),
				new StorageSpec("ordinal", ExtractType.Integer),
				new StorageSpec("citation", ExtractType.String, 500)));
		// a variable number of identifiers
		for (String id : ids) {
			storageSpecs.add(new StorageSpec(id, ExtractType.String, 30));
		}
		createTable(tableName, storageSpecs);
		PreparedStatement insert = insertStatement(tableName, storageSpecs);
		
		List<Extract<LinkedField<Reference>>> extracts = new ArrayList<>();
		extracts.add(storageSpecs.get(0).extractInt(LinkedField::getOrdinal));
		extracts.add(storageSpecs.get(1).extractString(LinkedField::getPublicationId));
		extracts.add(storageSpecs.get(2).extractString(l -> l.getField().getCitation()));

		// a variable number of identifiers
		for (String id : ids) {
			extracts.add(storageSpecs.get(3).extractString(l -> l.getField().getIdentifiers().stream()
							.filter(m -> m.getKey().equals(id)).map(Metadata::getValue).findFirst().orElse(null)));
		}

		return new ListSink<>(createAbstractSink(extracts, insert, batchSize, connection, tableName));
	}

	/**
	 * 
	 * @param sourceIdentifier
	 * @param targetIdentifier
	 * @return
	 * @throws SQLException
	 */
	public Sink<List<Metadata>> createLinkoutTable(String sourceIdentifier, String targetIdentifier) throws SQLException {
		List<StorageSpec> storageSpecs = new ArrayList<>();
		String tableName = "LinkTable";
		storageSpecs.addAll(Arrays.asList(
				new StorageSpec(sourceIdentifier, ExtractType.String, 20, useKeys),
				new StorageSpec(targetIdentifier, ExtractType.String, 20)));
		
		createTable(tableName, storageSpecs);
		PreparedStatement insert = insertStatement(tableName, storageSpecs);
		
		List<Extract<Metadata>> extracts = new ArrayList<>();
		extracts.add(storageSpecs.get(0).extractString(Metadata::getKey));
		extracts.add(storageSpecs.get(2).extractString(Metadata::getValue));

		return new ListSink<>(createAbstractSink(extracts, insert, batchSize, connection, tableName));
	}

	/**
	 * Create a sink of full records into a JDBC SQL table.
	 * Once the connection is closed, the sink is invalidated.
	 * PostgreSQL dialect is assumed.
	 * 
	 * @return a consumer.
	 * @throws SQLException
	 */
	public Sink<StandardRecord> createRecordSink() throws SQLException {
		List<StorageSpec> storageSpecs = Arrays.asList(
				new StorageSpec("Identifier", ExtractType.String, 20, useKeys),
				new StorageSpec("Year", ExtractType.SmallInt, 0),
				new StorageSpec("Record", ExtractType.Text, 0));
		String tableName = "Records";
		createTable(tableName, storageSpecs);
		PreparedStatement insert = insertStatement(tableName, storageSpecs);
		
		List<Extract<StandardRecord>> extracts = new ArrayList<>();
		extracts.add(storageSpecs.get(0).extractString(StandardRecord::getIdentifier));
		extracts.add(storageSpecs.get(1).extractInt(StandardRecord::getYear));
		extracts.add(storageSpecs.get(2).extractString(StandardRecord::toJson));

		return new GenericSink<>(createAbstractSink(extracts, insert, batchSize, connection, tableName));
	}

	private void createTable(String tableName, List<StorageSpec> storageSpecs) throws SQLException {
		String creation = String.format("CREATE TABLE IF NOT EXISTS %s (%s)",
				tableName,
				storageSpecs.stream().map(Object::toString).collect(Collectors.joining(", ")));
		Statement statement = connection.createStatement();
		statement.execute(creation);
	}

	private PreparedStatement insertStatement(String tableName, List<StorageSpec> storageSpecs) throws SQLException {
		String insert = String.format("INSERT INTO %s (%s) VALUES (%s)",
				tableName,
				storageSpecs.stream().map(StorageSpec::getField).collect(Collectors.joining(", ")),
				storageSpecs.stream().map(s -> "?").collect(Collectors.joining(", ")));
		return connection.prepareStatement(insert);
	}

	private static <T> AbstractSink<T> createAbstractSink(List<Extract<T>> extracts, 
			PreparedStatement insert, int batchSize, Connection connection, String tableName) throws SQLException {
		Set<String> keys = new HashSet<>();//org.curieo.sources.IdentifierSet();
		Optional<Extract<T>> uniqueOpt = extracts.stream().filter(extract -> extract.getSpec().isUnique()).findFirst();
		Extract<T> keyExtractor = null;
		PreparedStatement deleteStatement = null;
		if (uniqueOpt.isPresent()) {
			keyExtractor = uniqueOpt.get();
			// https://jdbc.postgresql.org/documentation/query/#getting-results-based-on-a-cursor
			boolean autocommit = connection.getAutoCommit();
			connection.setAutoCommit(false);
			String query = String.format("SELECT %s FROM %s", keyExtractor.getSpec().getField(), tableName);
			// give some hints as to how to read economically
			Statement statement = connection.createStatement(ResultSet.CONCUR_READ_ONLY, ResultSet.TYPE_FORWARD_ONLY);
			statement.setFetchSize(100);
			try (ResultSet resultSet = statement.executeQuery(query)) {
				while (resultSet.next()) {
					keys.add(resultSet.getString(1));
				}
			}
			connection.setAutoCommit(autocommit); // back to original value
			LOGGER.info("Read {} keys from {}", keys.size(), tableName);
			deleteStatement = connection.prepareStatement(
					String.format("DELETE FROM %s WHERE %s = ?", tableName, keyExtractor.getSpec().getField()));
		}
		return new AbstractSink<T>(extracts, insert, deleteStatement, batchSize, keys, keyExtractor);
	}
}
