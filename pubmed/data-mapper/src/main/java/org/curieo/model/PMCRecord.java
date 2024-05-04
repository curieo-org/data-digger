package org.curieo.model;

import java.sql.Timestamp;
import lombok.Generated;
import lombok.Value;

@Generated
@Value
public class PMCRecord {
  String container; // tar.gz blob file that contains this record
  String articleFile; // path *within* the tar.gz file that contains this record
  String articleCitation;
  String pmcId;
  Timestamp lastUpdated;
  long pmId;
  String license;
  String retracted;

  public static PMCRecord fromCSV(String container, String[] line) {
    return new PMCRecord(
        container,
        line[0],
        line[1],
        line[2],
        Timestamp.valueOf(line[3]),
        Long.parseLong(line[4]),
        line[5],
        line[6]);
  }
}
