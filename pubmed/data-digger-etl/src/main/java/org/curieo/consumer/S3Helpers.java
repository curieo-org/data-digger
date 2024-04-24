package org.curieo.consumer;

import java.util.Optional;
import org.curieo.utils.Config;
import software.amazon.awssdk.auth.credentials.internal.SystemSettingsCredentialsProvider;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectResponse;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.utils.SystemSetting;
import software.amazon.awssdk.utils.ToString;

public class S3Helpers {
  private S3Helpers() {}

  // https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/services/s3/S3Client.html
  public static S3Client getS3Client(Config config) {
    Region region = Region.of(config.getEnv("AWS_REGION", false, null));
    return S3Client.builder()
        .credentialsProvider(new ConfigCredentialsProvider(config))
        .region(region)
        .build();
  }

  /**
   * // Places a file into a S3 bucket
   *
   * @param data
   * @param bucketName
   * @param objectKey
   * @return
   */
  public static PutObjectResponse putObject(
      S3Client s3, byte[] data, String bucketName, String objectKey) throws S3Exception {
    // Put a file on S3
    return s3.putObject(
        PutObjectRequest.builder().bucket(bucketName).key(objectKey).build(),
        RequestBody.fromBytes(data));
  }

  public static DeleteObjectResponse deleteObject(S3Client s3, String bucketName, String objectKey)
      throws S3Exception {
    // Delete a file from S3
    return s3.deleteObject(DeleteObjectRequest.builder().bucket(bucketName).key(objectKey).build());
  }

  public static ResponseInputStream<GetObjectResponse> getObject(
      S3Client s3, String bucketName, String objectKey) throws S3Exception {
    // Get a file from S3
    return s3.getObject(GetObjectRequest.builder().bucket(bucketName).key(objectKey).build());
  }

  public static final class ConfigCredentialsProvider extends SystemSettingsCredentialsProvider {
    private final Config config;
    private static final String PROVIDER_NAME = "ConfigCredentialsProvider";

    ConfigCredentialsProvider(Config config) {
      this.config = config;
    }

    @Override
    protected Optional<String> loadSetting(SystemSetting setting) {
      String env = config.getEnv(setting.environmentVariable(), false, null);
      if (env != null) {
        return Optional.of(env);
      }

      return SystemSetting.getStringValueFromEnvironmentVariable(setting.environmentVariable());
    }

    @Override
    protected String provider() {
      return PROVIDER_NAME;
    }

    @Override
    public String toString() {
      return ToString.create(PROVIDER_NAME);
    }
  }
}
