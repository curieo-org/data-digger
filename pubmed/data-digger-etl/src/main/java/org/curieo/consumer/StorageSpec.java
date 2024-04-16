package org.curieo.consumer;

import java.util.List;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lombok.AllArgsConstructor;
import lombok.Generated;
import lombok.Value;

@Generated
@Value
@AllArgsConstructor
class StorageSpec {
	private static final Logger LOGGER = LoggerFactory.getLogger(StorageSpec.class);

	String field;
	ExtractType type;
	int size;
	// if the field must be kept unique
	boolean unique;
	String defaultValue;

	StorageSpec(String field, ExtractType type) {
		this(field, type, 0, false, "");
		if (type == ExtractType.String || type == ExtractType.List)
			throw new IllegalArgumentException("VARCHAR types must have a size specified");
	}

	StorageSpec(String field, ExtractType type, int size, boolean unique) {
		this(field, type, size, unique, "");
		if (type != ExtractType.String && type != ExtractType.List)
			throw new IllegalArgumentException("Only VARCHAR types can have a size specified");
	}

	StorageSpec(String field, ExtractType type, int size) {
		this(field, type, size, false, "");
		if (type != ExtractType.String && type != ExtractType.List)
			throw new IllegalArgumentException("Only VARCHAR types can have a size specified");
	}

	StorageSpec(String field, ExtractType type, String defaultValue) {
		this(field, type, 0, false, defaultValue);
		if (type == ExtractType.String || type == ExtractType.List)
			throw new IllegalArgumentException("VARCHAR types must have a size specified");
	}

	<T> Extract<T> extractString(Function<T, String> f) {
		if (this.type == ExtractType.String)
			return new Extract<>(this, null, new TrimToSize<>(size, f, field), null);
		if (this.type == ExtractType.Text)
			return new Extract<>(this,  null, f, null);
		throw new IllegalArgumentException("Wrong extractor for specified type");
	}

	<T> Extract<T> extractList(Function<T, List<String>> f) {
		return new Extract<>(this, new TrimAllToSize<>(size, f, field), null, null);
	}

	<T> Extract<T> extractInt(Function<T, Integer> f) {
		return new Extract<>(this, null, null, f);
	}

	public String toString() {
		return String.format("%s %s%s %s", field, type.getSqlType(), size == 0 ? "" : String.format("(%d)", size), defaultValue == "" ? "" : String.format("DEFAULT %s", defaultValue));
	}


	static String trimField(String field, String content, int maximum) {
		if (content == null || content.length() <= maximum) {
			return content;
		}
		LOGGER.info("Trimming field {} to size {} down from {}", field, maximum, content.length());
		return content.substring(0, maximum);
	}

	@Generated
	@Value
	private static class TrimToSize<T> implements Function<T, String> {
		int size;
		Function<T, String> extract;
		String field;

		@Override
		public String apply(T t) {
			return trimField(field, extract.apply(t), size);
		}
	}

	@Generated
	@Value
	private static class TrimAllToSize<T> implements Function<T, List<String>> {
		int size;
		Function<T, List<String>> extract;
		String field;

		@Override
		public List<String> apply(T t) {
			List<String> s = extract.apply(t);
			if (s == null)
				return s;
			return s.stream().map(v -> trimField(field, v, size)).toList();
		}
	}
}
