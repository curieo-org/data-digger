package org.curieo.consumer;

import static java.nio.charset.StandardCharsets.UTF_8;

import org.curieo.model.FullTextRecord;
import org.curieo.utils.Config;
import software.amazon.awssdk.services.s3.S3Client;

/** Class to store records in an S3 bucket directory. */
public class AWSStorageSink implements Sink<FullTextRecord> {
  public static final int DEFAULT_BATCH_SIZE = 100;
  private final S3Client client;
  private final String bucket;
  private int count = 0;

  public AWSStorageSink(Config config) {
    client = S3Helpers.getS3Client(config);
    bucket = config.getEnv("AWS_STORAGE_BUCKET", true, null);
  }

  @Override
  public void accept(FullTextRecord t) {
    String location = t.computeLocation();
    S3Helpers.putObject(client, t.getContent().getBytes(UTF_8), bucket, location);
    count++;
  }

  @Override
  public void finalCall() {
    client.close();
  }

  @Override
  public int getTotalCount() {
    return count;
  }

  @Override
  public int getUpdatedCount() {
    return 0;
  }
}
