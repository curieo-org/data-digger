package org.curieo.sources.pubmedcentral;

import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import org.curieo.consumer.S3Helpers;
import org.curieo.consumer.Sink;
import org.curieo.model.PMCRecord;
import org.curieo.retrieve.ftp.FTPProcessing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;

public record BulkFileHandler(S3Client s3, String bucket, Sink<PMCRecord> pmcSink) {
  private static final Logger LOGGER = LoggerFactory.getLogger(BulkFileHandler.class);
  public static final String FILELIST_CSV = ".filelist.csv";

  public FTPProcessing.Status processBulkFile(File file, String name) {
    try {

      // if it's a data file, move to S3
      if (name.toLowerCase().endsWith(".tar.gz")) {
        S3Helpers.putFile(s3, file, bucket, "bulk/" + name);
      } else if (name.toLowerCase().endsWith(FILELIST_CSV)) {
        // If it's a file list, read into table CSV
        try (FileInputStream fis = new FileInputStream(file);
            Reader reader = new InputStreamReader(fis);
            CSVReader csvReader = new CSVReaderBuilder(reader).withSkipLines(1).build()) {
          for (String[] line : csvReader) {
            pmcSink.accept(
                PMCRecord.fromCSV(name.substring(0, name.length() - FILELIST_CSV.length()), line));
          }
        }
      } else {
        return FTPProcessing.Status.Seen;
      }

      return FTPProcessing.Status.Success;
    } catch (Exception e) {
      LOGGER.error(String.format("Failed to process file %s", file.getAbsolutePath()), e);
      return FTPProcessing.Status.Error;
    }
  }
}
