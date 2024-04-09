package org.curieo.consumer;

enum ExtractType {
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
