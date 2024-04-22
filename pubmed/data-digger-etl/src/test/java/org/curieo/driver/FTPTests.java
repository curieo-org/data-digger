package org.curieo.driver;

import java.io.IOException;
import java.util.HashMap;
import org.curieo.consumer.Sink;
import org.curieo.model.Job;
import org.curieo.model.TS;
import org.curieo.retrieve.ftp.FTPProcessing;
import org.curieo.utils.Credentials;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

class FTPTests {

  @Test
  @Tag("slow")
  void testListing() throws IOException {
    Credentials creds = Credentials.defaults();

    Sink<TS<Job>> jobSink = new Sink.Noop<>();
    try (FTPProcessing ftpProc = new FTPProcessing(creds, "pubmedcommons")) {
      ftpProc.processRemoteDirectory(
          creds.get("pubmedcommons", "remotepath"),
          new HashMap<>(),
          jobSink,
          FTPProcessing.skipExtensions("md5", "html"),
          (file, name) -> {
            System.out.printf("File %s, job name: %s\n", file.getName(), name);
            return FTPProcessing.Status.Seen;
          },
          Integer.MAX_VALUE);
    }
  }
}
