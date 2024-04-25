package org.curieo.driver;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.IOException;
import org.curieo.consumer.S3Helpers;
import org.curieo.utils.Config;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectResponse;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

public class S3Tests {

  @Test
  void testS3ReadWriteDelete() throws IOException {
    String message = "Ground control to Major Tom";
    Config config = new Config();
    String bucket = config.aws_storage_bucket;
    S3Client client = S3Helpers.getS3Client(config);
    assertNotNull(client);

    PutObjectResponse por =
        S3Helpers.putObject(client, message.getBytes(UTF_8), bucket, "test.txt");
    assertNotNull(por);
    ResponseInputStream<GetObjectResponse> gor = S3Helpers.getObject(client, bucket, "test.txt");
    String response = new String(gor.readAllBytes(), UTF_8);
    assertEquals(message, response);

    DeleteObjectResponse dor = S3Helpers.deleteObject(client, bucket, "test.txt");
    System.out.printf("%s%n", dor.toString());
  }
}
