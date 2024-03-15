package org.curieo.consumer;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.curieo.model.Authorship;
import org.curieo.model.StandardRecord;
import org.curieo.utils.ListUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lombok.AllArgsConstructor;
import lombok.Generated;
import lombok.Value;

/**
 * Class to store records into PostgreSQL database.
 */
public class PostgreSQLSink {
    private static final Logger LOGGER = LoggerFactory.getLogger(PostgreSQLSink.class);
    
	private static enum ExtractType {
		List("VARCHAR"),
		String("VARCHAR"), 
		Integer("INT"),
		SmallInt("SMALLINT"),
		Text("TEXT");
		
		final String sqlType;
		ExtractType(String s) {
			sqlType = s;
		}
		String getSqlType() {
			return sqlType;
		}
	}
	
	@Generated @Value
	private static class StorageSpec {
		String field;
		ExtractType type;
		int size;

		<T> Extract<T> trim(Function<T, String> f) {
			return new Extract<>(this.type, null, new TrimToSize<>(size, f, field), null);
		}
		<T> Extract<T> trimAll(Function<T, List<String>> f) {
			return new Extract<>(this.type, new TrimAllToSize<>(size, f, field), null, null);
		}
		
		public String toString() {
			return String.format("%s %s%s", field, type.getSqlType(), size == 0 ? "" : String.format("(%d)", size));
		}
	}
	
	/**
	 * Create a sink of authorships into a JDBC SQL table.
	 * Once the connection is closed, the sink is invalidated.
	 * PostgreSQL dialect is assumed.
	 * 
	 * @param connection
	 * @return a consumer.
	 * @throws SQLException
	 */
	public static Sink<Authorship> createAuthorshipSink(Connection connection) throws SQLException {
		List<StorageSpec> storageSpecs = Arrays.asList(
				new StorageSpec("Ordinal", ExtractType.SmallInt, 0),
				new StorageSpec("foreName", ExtractType.String, 40),
				new StorageSpec("lastName", ExtractType.String, 60),
				new StorageSpec("initials", ExtractType.String, 10),
				new StorageSpec("affiliation", ExtractType.List, 2000),
				new StorageSpec("yearActive", ExtractType.Integer, 0),
				new StorageSpec("emailaddress", ExtractType.String, 60),
				new StorageSpec("publicationId", ExtractType.String, 30)
		);
		
		String creation = String.format("CREATE TABLE IF NOT EXISTS AuthorShips (%s)",
				 storageSpecs.stream().map(Object::toString).collect(Collectors.joining(", ")));
		Statement statement = connection.createStatement();
		statement.execute(creation);

		String insert = String.format("INSERT INTO AuthorShips (%s) VALUES (%s)",
				 storageSpecs.stream().map(StorageSpec::getField).collect(Collectors.joining(", ")),
				 storageSpecs.stream().map(s -> "?").collect(Collectors.joining(", ")));
		PreparedStatement p = connection.prepareStatement(insert);
		List<Extract<Authorship>> extracts = new ArrayList<>();
		extracts.add(new Extract<>(storageSpecs.get(0).type, null, null, Authorship::getOrdinal));
		extracts.add(storageSpecs.get(1).trim(Authorship::getForeName));
		extracts.add(storageSpecs.get(2).trim(Authorship::getLastName));
		extracts.add(storageSpecs.get(3).trim(Authorship::getInitials));
		extracts.add(storageSpecs.get(4).trimAll(Authorship::getAffiliations));
		extracts.add(new Extract<>(storageSpecs.get(5).type, null, null, Authorship::getYearActive));
		extracts.add(storageSpecs.get(6).trim(Authorship::getEmailAddress));
		extracts.add(storageSpecs.get(7).trim(Authorship::getPublicationId));
		
		return new GenericSink<>(extracts, p);
	}

	/**
	 * Create a sink of full records into a JDBC SQL table.
	 * Once the connection is closed, the sink is invalidated.
	 * PostgreSQL dialect is assumed.
	 * 
	 * @param connection
	 * @return a consumer.
	 * @throws SQLException
	 */
	public static Sink<StandardRecord> createRecordSink(Connection connection) throws SQLException {
		List<StorageSpec> storageSpecs = Arrays.asList(
				new StorageSpec("Identifier", ExtractType.String, 20),
				new StorageSpec("Record", ExtractType.Text, 0)
		);
		
		String creation = String.format("CREATE TABLE IF NOT EXISTS Records (%s)",
				 storageSpecs.stream().map(Object::toString).collect(Collectors.joining(", ")));
		Statement statement = connection.createStatement();
		statement.execute(creation);

		String insert = String.format("INSERT INTO Records (%s) VALUES (%s)",
				 storageSpecs.stream().map(StorageSpec::getField).collect(Collectors.joining(", ")),
				 storageSpecs.stream().map(s -> "?").collect(Collectors.joining(", ")));
		PreparedStatement p = connection.prepareStatement(insert);
		List<Extract<StandardRecord>> extracts = new ArrayList<>();
		//StandardRecord sr = new StandardRecord();
		extracts.add(new Extract<>(storageSpecs.get(0).type, null, StandardRecord::getIdentifier, null));
		extracts.add(new Extract<>(storageSpecs.get(1).type, null, StandardRecord::toJson, null));
		
		return new GenericSink<>(extracts, p);
	}
	
	@Generated @Value
	private static class Extract<T> {
		ExtractType type;
		Function<T, List<String>> explode;
		Function<T, String> stringExtract;
		Function<T, Integer> intExtract;
	}
	
	static String trimField(String field, String content, int maximum) {
		if (content == null || content.length() <= maximum) {
			return content;
		}
		LOGGER.info("Trimming field {} to size {} down from {}", field, maximum, content.length());
		return content.substring(0, maximum);
	}
	
	@Generated @Value
	private static class TrimToSize<T> implements Function<T, String> {
		int size;
		Function<T, String> extract;
		String field;

		@Override
		public String apply(T t) {
			return trimField(field, extract.apply(t), size);
		}
	}

	@Generated @Value
	private static class TrimAllToSize<T> implements Function<T, List<String>> {
		int size;
		Function<T, List<String>> extract;
		String field;

		@Override
		public List<String> apply(T t) {
			List<String> s = extract.apply(t);
			if (s == null) return s;
			return s.stream().map(v -> trimField(field, v, size)).toList();
		}
	}
	
	@Generated @Value @AllArgsConstructor
	public static class GenericSink<T> implements Sink<T> {
		List<Extract<T>> extracts;
		PreparedStatement p;
		AtomicInteger count;
		
		GenericSink(List<Extract<T>> extracts, PreparedStatement p) {
			this(extracts, p, new AtomicInteger());
		}
		
		@Override
		public void accept(T t) {
			List<List<String>> exploded = new ArrayList<>();
			for (Extract<T> extract : extracts) {
				if (extract.type == ExtractType.List) {
					exploded.add(extract.explode.apply(t));
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
						Extract<T> extract = extracts.get(i-1);
						switch (extract.type) {
						case SmallInt:
						case Integer:
							p.setInt(i, extract.intExtract.apply(t));
							break;
						case List:
							if (values.size() > e) {
								p.setString(i, values.get(e++));
							}
							else {
								p.setString(i, null);
							}
							break;
						case String:
							p.setString(i, extract.stringExtract.apply(t));
							break;
						default:
							break;
						}
					}
					count.getAndAdd(p.executeUpdate());
				}
			} catch (SQLException e) {
				throw new RuntimeException(e);
			}
		}
		
		public void finalCall() {
			
		}
		
		public int getTotalCount() {
			return count.get();
		}
	}
}
