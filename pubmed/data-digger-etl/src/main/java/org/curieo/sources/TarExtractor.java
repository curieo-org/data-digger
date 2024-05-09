package org.curieo.sources;

import static org.apache.commons.lang3.StringUtils.rightPad;
import static org.curieo.utils.StringUtils.joinPath;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.compress.utils.IOUtils;
import org.apache.commons.io.FileUtils;
import org.curieo.consumer.S3Helpers;
import software.amazon.awssdk.services.s3.S3Client;

// https://www.baeldung.com/java-extract-tar-file
public class TarExtractor {
  private TarExtractor() {}

  public static File getSingleFileOutOfTar(File file, boolean gzip, Predicate<File> filter)
      throws IOException {
    Path destination = Files.createTempDirectory("tmpdir");
    List<File> files = untar(file, destination, gzip);
    File target = files.stream().filter(filter).findFirst().orElse(null);
    if (target == null) {
      return null;
    }
    String name = target.getName();
    String[] split = name.split("\\.", 2);
    File returnFile;
    if (split.length == 2) {
      // WHO KNEW? java.lang.IllegalArgumentException: Prefix string "hs" too short: length must be
      // at least 3
      returnFile = File.createTempFile(rightPad(split[0], 3, 'x'), split[1]);
    } else {
      returnFile = File.createTempFile(rightPad(split[0], 3, 'x'), "x");
    }
    if (target.getAbsolutePath().equals(returnFile.getAbsolutePath())) {
      throw new IllegalArgumentException("huh?!");
    }

    Files.copy(target.toPath(), returnFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
    FileUtils.deleteDirectory(destination.toFile());
    return returnFile;
  }

  public static List<File> untarAndDelete(File file, boolean gzip) throws IOException {
    Path destination = Files.createTempDirectory("tmpdir");
    List<File> files = untar(file, destination, gzip);
    FileUtils.deleteDirectory(destination.toFile());
    return files;
  }

  public static List<File> untar(File file, Path destination, boolean gzip) throws IOException {
    List<File> files = new ArrayList<>();
    Files.createDirectories(destination);
    try (FileInputStream fis = new FileInputStream(file);
        BufferedInputStream inputStream = new BufferedInputStream(fis);
        TarArchiveInputStream tar =
            new TarArchiveInputStream(
                gzip ? new GzipCompressorInputStream(inputStream) : inputStream)) {
      ArchiveEntry entry;
      while ((entry = tar.getNextEntry()) != null) {
        Path extractTo = destination.resolve(entry.getName());
        files.add(extractTo.toFile());
        if (entry.isDirectory()) {
          Files.createDirectories(extractTo);
        } else {
          Files.copy(tar, extractTo);
        }
      }
    }
    return files;
  }

  public static void untarToS3(File file, S3Client s3, String bucket, String basePath, boolean gzip)
      throws IOException {
    try (FileInputStream fis = new FileInputStream(file);
        BufferedInputStream inputStream = new BufferedInputStream(fis);
        TarArchiveInputStream tar =
            new TarArchiveInputStream(
                gzip ? new GzipCompressorInputStream(inputStream) : inputStream)) {
      TarArchiveEntry entry;
      while ((entry = (TarArchiveEntry) tar.getNextEntry()) != null) {
        String remotePath = joinPath(basePath, entry.getName());
        byte[] content = IOUtils.toByteArray(tar);
        S3Helpers.putObject(s3, content, bucket, remotePath);
      }
    }
  }
}
