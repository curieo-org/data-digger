package org.curieo.consumer;

enum ExtractType {
  List("VARCHAR"),
  String("VARCHAR"),
  Integer("INT"),
  SmallInt("SMALLINT"),
  BigInteger("BIGINT"),
  Text("TEXT"),
  Timestamp("TIMESTAMP");

  final String sqlType;

  ExtractType(String sqlType) {
    this.sqlType = sqlType;
  }

  String getSqlType() {
    return sqlType;
  }
}
