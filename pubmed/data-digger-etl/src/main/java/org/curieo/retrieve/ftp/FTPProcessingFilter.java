package org.curieo.retrieve.ftp;

import static org.curieo.retrieve.ftp.FTPProcessing.suffix;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Predicate;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPFileFilter;

public record FTPProcessingFilter(Predicate<FTPFile> predicate) implements FTPFileFilter {

  public static FTPProcessingFilter IgnoreExtensions(String... extensions) {
    Set<String> ext = new HashSet<>();
    Collections.addAll(ext, extensions);
    return new FTPProcessingFilter((t) -> !ext.contains(suffix(t.getName())));
  }

  public static FTPProcessingFilter ValidExtensions(String... extensions) {
    Set<String> ext = new HashSet<>();
    Collections.addAll(ext, extensions);
    return new FTPProcessingFilter((t) -> ext.contains(suffix(t.getName())));
  }

  public static FTPProcessingFilter ValidExtension(String extension) {
    return new FTPProcessingFilter((t) -> suffix(t.getName()).equals(extension));
  }

  @Override
  public boolean accept(FTPFile file) {
    return predicate.test(file);
  }
}
