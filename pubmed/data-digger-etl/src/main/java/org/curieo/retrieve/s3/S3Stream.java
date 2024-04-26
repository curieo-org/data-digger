package org.curieo.retrieve.s3;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Iterator;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.curieo.consumer.S3Helpers;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;

public class S3Stream {

  public static LineIterator readLines(
      S3Client s3, String bucketName, String objectKey, boolean nonExistIsEmptyStream)
      throws S3Exception {
    ResponseInputStream<GetObjectResponse> inputStream;
    try {
      inputStream = S3Helpers.getObject(s3, bucketName, objectKey);
    } catch (S3Exception e) {
      if (nonExistIsEmptyStream) {
        return IOUtils.lineIterator(new ByteArrayInputStream(IOUtils.EMPTY_BYTE_ARRAY), UTF_8);
      }
      throw e;
    }
    return IOUtils.lineIterator(inputStream, UTF_8);
  }

  public static Iterable<String[]> readTabSeparatedFile(
      S3Client s3, String bucketName, String objectKey) throws S3Exception {
    LineIterator li = readLines(s3, bucketName, objectKey, true);
    return new Iterable<String[]>() {

      @Override
      public Iterator<String[]> iterator() {
        return new Iterator<String[]>() {

          @Override
          public boolean hasNext() {
            if (li.hasNext()) {
              return true;
            }
            try {
              li.close();
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
            return false;
          }

          @Override
          public String[] next() {
            return li.next().split("\t");
          }
        };
      }
    };
  }

  public static PutObjectResponse writeTabSeparatedFile(
      S3Client s3, String bucketName, String objectKey, Iterable<String[]> records)
      throws S3Exception, IOException {
    File f = File.createTempFile("binary", "out");
    try (FileOutputStream fos = new FileOutputStream(f)) {
      for (String[] rec : records) {
        fos.write(String.join("\t", rec).getBytes(UTF_8));
        fos.write('\n');
      }
    }

    PutObjectResponse por =
        s3.putObject(
            PutObjectRequest.builder().bucket(bucketName).key(objectKey).build(),
            RequestBody.fromFile(f.toPath()));
    f.delete();
    return por;
  }
}
