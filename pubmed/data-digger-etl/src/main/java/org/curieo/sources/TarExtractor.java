package org.curieo.sources;

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
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.io.FileUtils;

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
      returnFile = File.createTempFile(split[0], split[1]);
    } else {
      returnFile = File.createTempFile(split[0], "x");
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
}
