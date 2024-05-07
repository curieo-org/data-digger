package org.curieo.retrieve.ftp;

import static org.curieo.utils.StringUtils.joinPath;

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
import org.curieo.consumer.Sink;
import org.curieo.model.PubmedTask;
import org.curieo.model.TS;
import org.curieo.utils.Config;
import org.curieo.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FTPProcessing implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(FTPProcessing.class);

  Config config;
  String key;
  FTPClient ftp;
  String server;

  public FTPProcessing(Config config) throws IOException {
    this(config, config.pubmed_ftp_server);
  }

  public FTPProcessing(Config config, String server) throws IOException {
    this.config = config;
    this.server = server;
    this.ftp = createClient();
  }

  public enum Status {
    Success, // submitted, fully completed, processed, no need to resubmit
    Error, // needs redo
    Open, // as yet undone
    Seen // submitted, not processed, but no need to submit to processor
  ;

    public PubmedTask.State intotaskState() {
      return switch (this) {
        case Seen -> PubmedTask.State.Queued;
        case Open -> PubmedTask.State.InProgress;
        case Error -> PubmedTask.State.Failed;
        case Success -> PubmedTask.State.Completed;
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
      String job,
      String remoteDirectory,
      Map<String, TS<PubmedTask>> tasks,
      Sink<TS<PubmedTask>> updateTaskSink,
      FTPProcessingFilter filter,
      BiFunction<File, String, Status> processor,
      int maximumNumberOfFiles)
      throws IOException {

    Objects.requireNonNull(job);
    assert !job.isEmpty();
    Objects.requireNonNull(remoteDirectory);
    assert !remoteDirectory.isEmpty();
    Objects.requireNonNull(tasks);
    Objects.requireNonNull(processor);
    Objects.requireNonNull(filter);

    reopenIfClosed();
    // First pass
    for (FTPFile file : ftp.listFiles(remoteDirectory, filter)) {
      String name = file.getName();
      Timestamp remoteTimestamp = Timestamp.from(file.getTimestamp().toInstant());

      if (tasks.containsKey(name)) {
        // If our task is outdated
        if (tasks.get(name).timestamp().before(remoteTimestamp)) {
          // Add task to queue and update tasks
          PubmedTask queued = PubmedTask.queue(name, job);
          updateTaskSink.accept(TS.of(queued, remoteTimestamp));
          tasks.put(name, TS.of(queued, remoteTimestamp));
        }
      } else {
        PubmedTask queued = PubmedTask.queue(name, job);
        updateTaskSink.accept(TS.of(queued, remoteTimestamp));
        tasks.put(name, TS.of(queued, remoteTimestamp));
      }
    }

    AtomicInteger filesSeen = new AtomicInteger();
    AtomicInteger done =
        new AtomicInteger(
            (int)
                tasks.values().stream()
                    .filter(ts -> ts.value().state() == PubmedTask.State.Completed)
                    .count());

    Predicate<Map.Entry<String, TS<PubmedTask>>> needsWork =
        (entry) -> {
          PubmedTask.State state = entry.getValue().value().state();
          return filesSeen.get() <= maximumNumberOfFiles
              && (state == PubmedTask.State.Queued
                  || state == PubmedTask.State.Failed
                  || state == PubmedTask.State.InProgress);
        };

    Executor executor = Executors.newFixedThreadPool(config.thread_pool_size);
    final List<CompletableFuture<Void>> futures =
        tasks.entrySet().stream()
            .filter(needsWork)
            .map(
                entry -> {
                  String key = entry.getKey();
                  TS<PubmedTask> ts = entry.getValue();
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
                            String remoteFile = joinPath(remoteDirectory, key, "/");
                            try {
                              boolean fileRetrieved = retrieveFile(ftpClient, remoteFile, tempFile);
                              return Pair.of(fileRetrieved, state);
                            } catch (IOException e) {
                              if (!tempFile.delete()) {
                                LOGGER.error(
                                    "Could not delete temp file {}", tempFile.getAbsolutePath());
                              }
                              updateTaskSink.accept(TS.of(PubmedTask.failed(key, job), timestamp));
                              throw new RuntimeException(e);
                            }
                          })
                      .thenAccept(
                          state -> {
                            boolean fileRetrieved = state.l();
                            FTPClient ftpClient = state.r().l();
                            File tempFile = state.r().r();

                            if (!fileRetrieved) {
                              LOGGER.error("Cannot retrieve file {}", key);
                              updateTaskSink.accept(TS.of(PubmedTask.failed(key, job), timestamp));
                            } else {
                              updateTaskSink.accept(
                                  TS.of(PubmedTask.inProgress(key, job), timestamp));
                              updateTaskSink.accept(
                                  TS.of(
                                      new PubmedTask(
                                          key, processor.apply(tempFile, key).intotaskState(), job),
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
                                    tasks.size(),
                                    (float) 100 * currentDone / tasks.size()));

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
    ftp.connect(server);
    // extremely important
    ftp.setFileType(FTPClient.BINARY_FILE_TYPE);
    ftp.setBufferSize(-1);

    LOGGER.info("Connected to {}.", server);
    LOGGER.info(ftp.getReplyString());

    // After connection attempt, you should check the reply code to verify
    // success.
    int reply = ftp.getReplyCode();

    if (!FTPReply.isPositiveCompletion(reply)) {
      ftp.disconnect();
      LOGGER.error("FTP server refused connection.");
    }

    ftp.enterLocalPassiveMode();
    ftp.login(config.pubmed_ftp_user, config.pubmed_ftp_password);
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
