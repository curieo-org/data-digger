package org.curieo.driver;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import org.curieo.retrieve.ftp.FTPProcessing;
import org.curieo.utils.Config;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

class FTPTests {

  @Test
  @Tag("slow")
  void testListing() throws IOException {
    Config config = new Config();
    try (FTPProcessing ftpProc = new FTPProcessing(config)) {
      File processingStatus = File.createTempFile("processingStatus", ".json");
      Files.write(processingStatus.toPath(), "{}".getBytes());
      ftpProc.processRemoteDirectory(
          config.commons_remote_path,
          processingStatus,
          FTPProcessing.skipExtensions("md5"),
          file -> {
            System.out.printf("File %s\n", file.getName());
            return FTPProcessing.Status.Seen;
          },
          Integer.MAX_VALUE);
    }
  }
}
