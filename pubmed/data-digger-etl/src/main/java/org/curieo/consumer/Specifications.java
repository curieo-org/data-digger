package org.curieo.consumer;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.*;
import org.curieo.utils.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

record TableSpec(String name, List<FieldSpec> fields, List<Constraint> constraints)
    implements ToSql {

  public TableSpec {
    StringUtils.requireNonEmpty(name);
    Objects.requireNonNull(fields);
    Objects.requireNonNull(constraints);
  }

  public static TableSpec of(String name, List<FieldSpec> fields, Constraint... constraints) {
    return new TableSpec(name, new ArrayList<>(fields), List.of(constraints));
  }

  public void addField(FieldSpec field) {
    fields.add(field);
  }

  public void addField(int index, FieldSpec field) {
    fields.add(index, field);
  }

  @Override
  public String toSql() {
    boolean missingIdentityColumn = fields.stream().noneMatch(FieldSpec::isIdentityColumn);
    boolean missingTimestamp = fields.stream().noneMatch(FieldSpec::isTimestamp);
    if (missingIdentityColumn) {
      addField(0, FieldSpec.identity(ExtractType.BigInteger));
    }
    if (missingTimestamp) {
      addField(FieldSpec.timestamp("timestamp", "now()"));
    }
    return String.format(
        "CREATE TABLE IF NOT EXISTS %s (%s)",
        name,
        Stream.concat(
                fields.stream().map(ToSql::toSql),
                constraints.stream().map(Constraint::toConstraint))
            .collect(Collectors.joining(", ")));
  }
}

interface ToSql {
  String toSql();
}

interface SpecKey {
  String key();
}

interface Constraint extends ToSql {
  String toConstraint();

  @Override
  default String toSql() {
    return toConstraint();
  }
}

record CompositeUniqueKey(List<SpecKey> keys) implements Constraint {
  public CompositeUniqueKey {
    Objects.requireNonNull(keys);
  }

  public static CompositeUniqueKey of(SpecKey... keys) {
    return new CompositeUniqueKey(List.of(keys));
  }

  public static CompositeUniqueKey of(List<SpecKey> keys) {
    return new CompositeUniqueKey(keys);
  }

  public static CompositeUniqueKey fromStrings(List<String> keys) {
    return new CompositeUniqueKey(keys.stream().map(k -> (SpecKey) () -> k).toList());
  }

  public static CompositeUniqueKey fromStrings(String... keys) {
    return new CompositeUniqueKey(Stream.of(keys).map(k -> (SpecKey) () -> k).toList());
  }

  @Override
  public String toConstraint() {
    return String.format(
        "unique (%s)", keys.stream().map(SpecKey::key).collect(Collectors.joining(", ")));
  }
}

@Value
@Builder
class FieldSpec implements ToSql, SpecKey {
  private static final Logger LOGGER = LoggerFactory.getLogger(FieldSpec.class);

  String field;
  ExtractType type;
  int size;
  // if the field must be kept unique
  boolean unique;
  // if the field is non-null or not.
  Boolean nullable;
  String defaultValue;
  // Generally only applicable to primary keys.
  // If identity type is null it is not an identity type.
  IdentityType identityType;

  enum IdentityType {
    Generated,
    Manual
  }

  FieldSpec(
      String field,
      ExtractType type,
      int size,
      boolean unique,
      Boolean nullable,
      String defaultValue,
      IdentityType identityType) {
    this.field = StringUtils.requireNonEmpty(field);
    this.type = Objects.requireNonNull(type);
    this.size = size;
    this.unique = unique;
    // Default to nullable
    this.nullable = Objects.requireNonNullElse(nullable, true);
    this.defaultValue = Objects.requireNonNullElse(defaultValue, "");
    this.identityType = identityType;

    if (size == 0 && (this.type == ExtractType.String || this.type == ExtractType.List)) {
      throw new IllegalArgumentException("VARCHAR types must have a size specified");
    }
  }

  FieldSpec(String field, ExtractType type) {
    this(field, type, 0, false, true, "", null);
  }

  static FieldSpec identity(ExtractType type) {
    switch (type) {
      case SmallInt, Integer, BigInteger -> {
        return new FieldSpec("id", type, 0, false, false, "", IdentityType.Generated);
      }
      default ->
          throw new IllegalArgumentException(
              "Identity must be smallint, int, or bigint. Type provided: " + type);
    }
  }

  static FieldSpec unique(String field, ExtractType type) {
    return new FieldSpec(field, type, 0, true, false, "", IdentityType.Manual);
  }

  static FieldSpec timestamp(String field, String defaultValue) {
    return new FieldSpec(field, ExtractType.Timestamp, 0, false, false, defaultValue, null);
  }

  static FieldSpec timestamp(String field) {
    return new FieldSpec(field, ExtractType.Timestamp, 0, false, false, "", null);
  }

  FieldSpec(String field, ExtractType type, int size, boolean unique) {
    this(field, type, size, unique, false, "", null);
  }

  FieldSpec(String field, ExtractType type, int size) {
    this(field, type, size, false, false, "", null);
  }

  public boolean isTimestamp() {
    return type == ExtractType.Timestamp;
  }

  public boolean isIdentityColumn() {
    return identityType != null;
  }

  public boolean isDefault() {
    return defaultValue != null && !defaultValue.isEmpty();
  }

  <T> Extract<T> extractString(Function<T, String> f) {
    return switch (this.type) {
      case ExtractType.String ->
          new Extract<>(this, null, new TrimToSize<>(size, f, field), null, null, null);
      case ExtractType.Text -> new Extract<>(this, null, f, null, null, null);
      default ->
          throw new IllegalArgumentException(
              "No string extractor for specified type: " + this.type);
    };
  }

  <T> Extract<T> extractList(Function<T, List<String>> f) {
    return new Extract<>(this, new TrimAllToSize<>(size, f, field), null, null, null, null);
  }

  <T> Extract<T> extractInt(Function<T, Integer> f) {
    return new Extract<>(this, null, null, f, null, null);
  }

  <T> Extract<T> extractLong(Function<T, Long> f) {
    return new Extract<>(this, null, null, null, f, null);
  }

  <T> Extract<T> extractTimestamp(Function<T, Timestamp> f) {
    return new Extract<>(this, null, null, null, null, f);
  }

  static String trimField(String field, String content, int maximum) {
    if (content == null || content.length() <= maximum) {
      return content;
    }
    LOGGER.debug("Trimming field {} to size {} down from {}", field, maximum, content.length());
    return content.substring(0, maximum);
  }

  private record TrimToSize<T>(int size, Function<T, String> extract, String field)
      implements Function<T, String> {

    @Override
    public String apply(T t) {
      return trimField(field, extract.apply(t), size);
    }
  }

  public static <T, R> List<R> mapper(T[] a, Function<T, R> mapFunction) {
    return Arrays.stream(a).map(mapFunction).toList();
  }

  private record TrimAllToSize<T>(int size, Function<T, List<String>> extract, String field)
      implements Function<T, List<String>> {
    @Override
    public List<String> apply(T t) {
      List<String> s = extract.apply(t);
      if (s == null) return null;
      return s.stream().map(v -> trimField(field, v, size)).toList();
    }
  }

  @Override
  public String toSql() {
    if (identityType == IdentityType.Generated) {
      return String.format(
          "%s %s %s", field, type.getSqlType(), "GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY");
    } else if (identityType == IdentityType.Manual) {
      return String.format("%s %s %s", field, type.getSqlType(), "PRIMARY KEY");
    }
    return String.format(
        "%s %s%s %s %s %s",
        field,
        type.getSqlType(),
        size == 0 ? "" : String.format("(%d)", size),
        unique ? "unique" : "",
        nullable ? "" : "not null",
        (defaultValue == null || defaultValue.isEmpty())
            ? ""
            : String.format("DEFAULT %s", defaultValue));
  }

  @Override
  public String key() {
    return field;
  }
}
