package org.curieo.retrieve.ftp;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;
import org.curieo.consumer.PostgreSQLClient;
import org.curieo.consumer.Sink;
import org.curieo.model.Job;
import org.curieo.model.TS;
import org.curieo.utils.Credentials;
import org.curieo.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FTPProcessing implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(FTPProcessing.class);

  Credentials creds;
  String key;
  FTPClient ftp;
  PostgreSQLClient psqlClient;

  public FTPProcessing(Credentials credentials, String key) throws IOException {
    this.key = key;
    this.creds = credentials;
    this.ftp = createClient();
  }

  public FTPProcessing(PostgreSQLClient psqlClient, Credentials credentials, String key)
      throws IOException {
    this.key = key;
    this.creds = credentials;
    this.psqlClient = psqlClient;
    this.ftp = createClient();
  }

  public enum Status {
    Success, // submitted, fully completed, processed, no need to resubmit
    Error, // needs redo
    Open, // as yet undone
    Seen // submitted, not processed, but no need to submit to processor
  ;

    public Job.State intoJobState() {
      return switch (this) {
        case Seen -> Job.State.Queued;
        case Open -> Job.State.InProgress;
        case Error -> Job.State.Failed;
        case Success -> Job.State.Completed;
      };
    }
  }

  public void reopenIfClosed() throws IOException {
    if (!ftp.isConnected()) {
      ftp.disconnect();
      ftp = createClient();
    }
  }

  @Override
  public void close() {
    if (psqlClient != null) {
      psqlClient.close();
    }

    if (ftp.isConnected()) {
      try {
        ftp.disconnect();
        LOGGER.info("Closed");
      } catch (IOException ioe) {
        LOGGER.warn("Could not close FTP client.");
        // do nothing
      }
    }
  }

  /**
   * Synchronize a remote directory and a local directory.
   *
   * @param remoteDirectory
   * @param processor
   * @param maximumNumberOfFiles maximum number of files to put through the processor
   * @throws IOException
   */
  public void processRemoteDirectory(
      String remoteDirectory,
      Map<String, TS<Job>> jobs,
      Sink<TS<Job>> updateJobSink,
      FTPProcessingFilter filter,
      BiFunction<File, String, Status> processor,
      int maximumNumberOfFiles)
      throws IOException {

    reopenIfClosed();

    // First pass
    for (FTPFile file : ftp.listFiles(remoteDirectory, filter)) {
      String name = file.getName();
      Timestamp remoteTimestamp = Timestamp.from(file.getTimestamp().toInstant());

      if (jobs.containsKey(name)) {
        // If our job is outdated
        if (jobs.get(name).timestamp().before(remoteTimestamp)) {
          // Add job to queue and update jobs
          Job queued = Job.queue(name);
          updateJobSink.accept(TS.of(queued, remoteTimestamp));
          jobs.put(name, TS.of(queued, remoteTimestamp));
        }
      } else {
        Job queued = Job.queue(name);
        updateJobSink.accept(TS.of(queued, remoteTimestamp));
        jobs.put(name, TS.of(queued, remoteTimestamp));
      }
    }
    ;

    AtomicInteger filesSeen = new AtomicInteger();
    AtomicInteger done =
        new AtomicInteger(
            (int)
                jobs.values().stream()
                    .filter(ts -> ts.value().getJobState() == Job.State.Completed)
                    .count());

    Predicate<Map.Entry<String, TS<Job>>> needsWork =
        (entry) -> {
          Job.State state = entry.getValue().value().getJobState();
          return filesSeen.get() <= maximumNumberOfFiles
              && (state == Job.State.Queued || state == Job.State.Failed);
        };

    Executor executor = Executors.newFixedThreadPool(10);
    final List<CompletableFuture<Void>> futures =
        jobs.entrySet().stream()
            .filter(needsWork)
            .map(
                entry -> {
                  String key = entry.getKey();
                  TS<Job> ts = entry.getValue();
                  Timestamp timestamp = ts.timestamp();

                  return CompletableFuture.supplyAsync(
                          () -> {
                            try {
                              return createClient();
                            } catch (IOException e) {
                              throw new RuntimeException(e);
                            }
                          },
                          executor)
                      .thenApply(
                          ftpClient -> {
                            try {
                              return Pair.of(
                                  ftpClient, File.createTempFile(prefix(key), suffix(key)));
                            } catch (IOException e) {
                              throw new RuntimeException(e);
                            }
                          })
                      .thenApply(
                          state -> {
                            FTPClient ftpClient = state.l();
                            File tempFile = state.r();

                            // retrieve the remote file, and submit.
                            String remoteFile;
                            if (remoteDirectory.endsWith("/")) {
                              remoteFile = remoteDirectory + key;
                            } else {
                              remoteFile = remoteDirectory + "/" + key;
                            }
                            try {
                              boolean fileRetrieved = retrieveFile(ftpClient, remoteFile, tempFile);
                              return Pair.of(fileRetrieved, state);
                            } catch (IOException e) {
                              if (!tempFile.delete()) {
                                LOGGER.error(
                                    "Could not delete temp file {}", tempFile.getAbsolutePath());
                              }
                              updateJobSink.accept(TS.of(Job.failed(key), timestamp));
                              throw new RuntimeException(e);
                            }
                          })
                      .thenAcceptAsync(
                          state -> {
                            boolean fileRetrieved = state.l();
                            FTPClient ftpClient = state.r().l();
                            File tempFile = state.r().r();

                            if (!fileRetrieved) {
                              LOGGER.error("Cannot retrieve file {}", key);
                              updateJobSink.accept(TS.of(Job.failed(key), timestamp));
                            } else {
                              updateJobSink.accept(TS.of(Job.inProgress(key), timestamp));
                              updateJobSink.accept(
                                  TS.of(
                                      new Job(key, processor.apply(tempFile, key).intoJobState()),
                                      timestamp));
                              LOGGER.info("Processed {}: state = {}", key, ts);
                              filesSeen.getAndIncrement();
                            }

                            if (!tempFile.delete()) {
                              LOGGER.error(
                                  "Could not delete temp file {}", tempFile.getAbsolutePath());
                            }
                            int currentDone = done.incrementAndGet();
                            LOGGER.info(
                                String.format(
                                    "Done %d/%d, at %.1f%%",
                                    currentDone,
                                    jobs.size(),
                                    (float) 100 * currentDone / jobs.size()));
                            updateJobSink.accept(TS.of(Job.completed(key), timestamp));

                            try {
                              ftpClient.disconnect();
                            } catch (IOException e) {
                              throw new RuntimeException(e);
                            }
                          });
                })
            .toList();

    futures.forEach(CompletableFuture::join);
  }

  private boolean retrieveFile(String remoteFile, File localFile) throws IOException {
    FileOutputStream fos = new FileOutputStream(localFile);
    ftp.retrieveFile(remoteFile, fos);
    return true;
  }

  private boolean retrieveFile(FTPClient client, String remoteFile, File localFile)
      throws IOException {
    FileOutputStream fos = new FileOutputStream(localFile);
    return client.retrieveFile(remoteFile, fos);
  }

  private static String prefix(String name) {
    int dot = name.lastIndexOf('.');
    if (dot == -1) {
      return "tmp";
    }
    return name.substring(0, dot);
  }

  static String suffix(String name) {
    int dot = name.lastIndexOf('.');
    if (dot == -1) {
      return "tmp";
    }
    if (name.toLowerCase().substring(dot).equals(".gz") && dot > 4) {
      int d = dot;
      while (d > 0 && name.toLowerCase().charAt(d - 1) != '.') {
        d--;
      }
      if (d > 0 && (dot - d) < 4) {
        return name.substring(d - 1);
      }
    }
    return name.substring(dot);
  }

  private FTPClient createClient() throws IOException {
    FTPClient ftp = new FTPClient();
    ftp.connect(getServer());
    // extremely important
    ftp.setFileType(FTPClient.BINARY_FILE_TYPE);
    ftp.setBufferSize(-1);

    LOGGER.info("Connected to {}.", this.config.pubmed_ftp_server);
    LOGGER.info(ftp.getReplyString());

    // After connection attempt, you should check the reply code to verify
    // success.
    int reply = ftp.getReplyCode();

    if (!FTPReply.isPositiveCompletion(reply)) {
      ftp.disconnect();
      LOGGER.error("FTP server refused connection.");
    }

    ftp.login(getUser(), getPassword());
    ftp.enterLocalPassiveMode();
    return ftp;
  }

  public static boolean retrieve(String href, File file) throws IOException, URISyntaxException {
    URL url = URI.create(href).parseServerAuthority().toURL();
    String path = url.getFile();
    String server = url.getHost();
    Config config = new Config();
    config.pubmed_ftp_server = server;
    config.pubmed_ftp_user = "anonymous";
    config.pubmed_ftp_password = "anonymous";
    try (FTPProcessing ftp = new FTPProcessing(config)) {
      return ftp.retrieveFile(path, file);
    }
  }
}
