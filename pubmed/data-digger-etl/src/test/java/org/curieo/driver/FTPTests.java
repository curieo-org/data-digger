package org.curieo.driver;

import java.io.IOException;
import java.util.HashMap;
import org.curieo.consumer.Sink;
import org.curieo.model.TS;
import org.curieo.model.Task;
import org.curieo.retrieve.ftp.FTPProcessing;
import org.curieo.retrieve.ftp.FTPProcessingFilter;
import org.curieo.utils.Config;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

class FTPTests {

  @Test
  @Tag("slow")
  void testListing() throws IOException {
    Config config = new Config();

    Sink<TS<Task>> taskSink = new Sink.Noop<>();
    try (FTPProcessing ftpProc = new FTPProcessing(config)) {
      ftpProc.processRemoteDirectory(
          "pubmed-baseline",
          config.commons_remote_path,
          new HashMap<>(),
          taskSink,
          FTPProcessingFilter.IgnoreExtensions("md5", "html"),
          (file, name) -> {
            System.out.printf("File %s, task name: %s\n", file.getName(), name);
            return FTPProcessing.Status.Seen;
          },
          Integer.MAX_VALUE);
    }
  }
}
