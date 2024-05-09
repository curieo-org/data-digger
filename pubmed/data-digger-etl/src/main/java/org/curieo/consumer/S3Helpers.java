package org.curieo.consumer;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.curieo.driver.DataLoaderPMC;
import org.curieo.utils.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.internal.SystemSettingsCredentialsProvider;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectResponse;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;
import software.amazon.awssdk.utils.SystemSetting;
import software.amazon.awssdk.utils.ToString;

public class S3Helpers {
  private static final Logger LOGGER = LoggerFactory.getLogger(DataLoaderPMC.class);

  private S3Helpers() {}

  // https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/services/s3/S3Client.html
  public static S3Client getS3Client(Config config) {
    Region region = Region.of(config.aws_region);
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

  public static void multipartUploadWithS3Client(
      S3Client client, File fileObject, String bucket, String location)
      throws S3Exception, IOException {
    CreateMultipartUploadResponse createMultipartUploadResponse =
        client.createMultipartUpload(b -> b.bucket(bucket).key(location));
    String uploadId = createMultipartUploadResponse.uploadId();

    // Upload the parts of the file.
    int partNumber = 1;
    List<CompletedPart> completedParts = new ArrayList<>();
    ByteBuffer bb = ByteBuffer.allocate(1024 * 1024 * 500); // 500 MB byte buffer

    try (RandomAccessFile file = new RandomAccessFile(fileObject, "r")) {
      long fileSize = file.length();
      int position = 0;
      while (position < fileSize) {
        file.seek(position);
        int read = file.getChannel().read(bb);

        bb.flip(); // Swap position and limit before reading from the buffer.
        UploadPartRequest uploadPartRequest =
            UploadPartRequest.builder()
                .bucket(bucket)
                .key(location)
                .uploadId(uploadId)
                .partNumber(partNumber)
                .build();

        UploadPartResponse partResponse =
            client.uploadPart(uploadPartRequest, RequestBody.fromByteBuffer(bb));

        CompletedPart part =
            CompletedPart.builder().partNumber(partNumber).eTag(partResponse.eTag()).build();
        completedParts.add(part);

        bb.clear();
        position += read;
        partNumber++;
      }
    } catch (IOException e) {
      LOGGER.error(e.getMessage());
      throw new IOException(e.getMessage());
    }

    // Complete the multipart upload.
    client.completeMultipartUpload(
        b ->
            b.bucket(bucket)
                .key(location)
                .uploadId(uploadId)
                .multipartUpload(CompletedMultipartUpload.builder().parts(completedParts).build()));
  }

  public static PutObjectResponse putFile(
      S3Client s3, File file, String bucketName, String objectKey) throws S3Exception {
    // Put a file on S3
    return s3.putObject(
        PutObjectRequest.builder().bucket(bucketName).key(objectKey).build(),
        RequestBody.fromFile(file));
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
