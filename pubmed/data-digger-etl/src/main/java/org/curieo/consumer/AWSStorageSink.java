package org.curieo.consumer;

import static java.nio.charset.StandardCharsets.UTF_8;

import org.curieo.model.FullTextRecord;
import org.curieo.utils.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;

/** Class to create record consumers into an S3 bucket database. */
public class AWSStorageSink implements Sink<FullTextRecord> {
  private static final Logger LOGGER = LoggerFactory.getLogger(AWSStorageSink.class);
  public static final int DEFAULT_BATCH_SIZE = 100;
  private final S3Client client;
  private final String bucket;

  public AWSStorageSink(Config config) {
    client = S3Helpers.getS3Client(config);
    bucket = config.getEnv("AWS_STORAGE_BUCKET", true, null);
  }

  @Override
  public void accept(FullTextRecord t) {
    String location = t.computeLocation();
    S3Helpers.putObject(client, t.getContent().getBytes(UTF_8), bucket, location);
  }

  @Override
  public void finalCall() {
    // TODO Auto-generated method stub

  }

  @Override
  public int getTotalCount() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public int getUpdatedCount() {
    // TODO Auto-generated method stub
    return 0;
  }
}
