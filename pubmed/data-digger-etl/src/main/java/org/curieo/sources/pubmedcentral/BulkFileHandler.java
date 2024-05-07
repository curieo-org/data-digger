package org.curieo.sources.pubmedcentral;

import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import org.curieo.consumer.S3Helpers;
import org.curieo.consumer.Sink;
import org.curieo.model.PMCLocation;
import org.curieo.retrieve.ftp.FTPProcessing;
import org.curieo.sources.TarExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;

public record BulkFileHandler(S3Client s3, String bucket, Sink<PMCLocation> pmcSink) {
  private static final Logger LOGGER = LoggerFactory.getLogger(BulkFileHandler.class);
  private static final CSVParser parser =
      new CSVParserBuilder().withSeparator('\t').withQuoteChar('"').build();
  public static final String FILELIST_CSV = ".filelist.txt";

  public FTPProcessing.Status processBulkFile(File file, String name) {
    try {

      // if it's a data file, move to S3
      if (name.toLowerCase().endsWith(".tar.gz")) {
        LOGGER.info(String.format("Upload to S3 %s", file.getAbsolutePath()));
        S3Helpers.putFile(s3, file, bucket, "bulk/" + name);
        String tarName = name.substring(0, name.length() - ".tar.gz".length());
        TarExtractor.untarToS3(file, s3, bucket, "bulk/" + tarName, true);
      } else if (name.toLowerCase().endsWith(FILELIST_CSV)) {
        // If it's a file list, read into table CSV
        LOGGER.info(String.format("Upload to database %s", file.getAbsolutePath()));
        try (Reader reader =
                new BufferedReader(new InputStreamReader(new FileInputStream(file), "UTF-8"));
            CSVReader csvReader =
                new CSVReaderBuilder(reader).withCSVParser(parser).withSkipLines(1).build()) {

          int processed_count = 0;
          int failed_count = 0;

          for (String[] line : csvReader) {
            if (line.length != 7) {
              failed_count++;
              LOGGER.error(
                  String.format(
                      "Invalid line in %s: %s, expected 7 columns, got %d",
                      file.getAbsolutePath(), line, line.length));
              continue;
            }
            try {
              pmcSink.accept(
                  PMCLocation.fromCSV(
                      name.substring(0, name.length() - FILELIST_CSV.length()), line));
              processed_count++;
            } catch (Exception e) {
              failed_count++;
              LOGGER.error(
                  String.format("Failed to process line %s in %s", line, file.getAbsolutePath()),
                  e);
            }
          }
          LOGGER.info(
              String.format("Processed %d lines in %s", processed_count, file.getAbsolutePath()));
          LOGGER.info(
              String.format(
                  "Failed to process %d lines in %s", failed_count, file.getAbsolutePath()));
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
