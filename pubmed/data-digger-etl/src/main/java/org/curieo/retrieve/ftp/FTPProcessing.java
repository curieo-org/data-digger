package org.curieo.retrieve.ftp;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;
import org.curieo.consumer.PostgreSQLClient;
import org.curieo.consumer.Sink;
import org.curieo.model.Job;
import org.curieo.model.TS;
import org.curieo.utils.Credentials;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FTPProcessing implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(FTPProcessing.class);

  private static final ObjectReader OBJECT_READER;
  private static final ObjectWriter OBJECT_WRITER;
  Credentials creds;
  String key;
  FTPClient ftp;
  PostgreSQLClient psqlClient;

  static {
    TypeReference<HashMap<String, Status>> typeRef = new TypeReference<>() {};
    OBJECT_READER = new ObjectMapper().readerFor(typeRef);
    OBJECT_WRITER = new ObjectMapper().writerFor(typeRef).withDefaultPrettyPrinter();
  }

  public FTPProcessing(Credentials credentials, String key) throws IOException {
    this.key = key;
    this.creds = credentials;
    connect();
  }

  public FTPProcessing(PostgreSQLClient psqlClient, Credentials credentials, String key)
      throws IOException {
    this.key = key;
    this.creds = credentials;
    this.psqlClient = psqlClient;
    connect();
  }

  public String getServer() {
    return creds.get(key, "server");
  }

  public String getUser() {
    return creds.get(key, "user");
  }

  public String getPassword() {
    return creds.get(key, "password");
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

  /**
   * If the path does not exist, returns empty map.
   *
   * @param path
   * @return instantiated status map
   * @throws IOException
   */
  public static Map<String, Status> readProcessingStatus(File path) throws IOException {
    if (!path.exists()) {
      LOGGER.warn("New status map created.");
      return new HashMap<>();
    }
    return OBJECT_READER.readValue(path);
  }

  /**
   * write the status map
   *
   * @param path
   * @throws JsonProcessingException
   * @throws IOException
   */
  public static void writeProcessingStatus(Map<String, Status> status, File path)
      throws JsonProcessingException, IOException {
    OBJECT_WRITER.writeValue(path, status);
  }

  public void reopenIfClosed() throws IOException {
    if (!ftp.isConnected()) {
      ftp.disconnect();
      connect();
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
   * @param localDirectory
   */
  public static void synchronize(String remoteDirectory, File localDirectory) {
    throw new UnsupportedOperationException();
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
      Function<FTPFile, Boolean> filter,
      BiFunction<File, String, Status> processor,
      int maximumNumberOfFiles)
      throws IOException {

    reopenIfClosed();

    // First pass
    for (FTPFile file : ftp.listFiles(remoteDirectory)) {
      if (filter.apply(file)) {
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
    }

    AtomicInteger filesSeen = new AtomicInteger();
    AtomicInteger done =
        new AtomicInteger(
            (int)
                jobs.values().stream()
                    .filter(ts -> ts.value().getJobState() == Job.State.Completed)
                    .count());

    jobs.entrySet().stream()
        .parallel()
        .forEach(
            state -> {
              String key = state.getKey();
              TS<Job> ts = state.getValue();
              Timestamp timestamp = ts.timestamp();
              Job job = ts.value();

              if (job.getJobState() != Job.State.Queued && job.getJobState() != Job.State.Failed) {
                return;
              }
              if (filesSeen.get() >= maximumNumberOfFiles) {
                return;
              }

              try {
                // retrieve the remote file, and submit.
                FTPClient ftpClient = createClient();
                File tmp = File.createTempFile(prefix(key), suffix(key));
                String remoteFile;
                if (remoteDirectory.endsWith("/")) {
                  remoteFile = remoteDirectory + key;
                } else {
                  remoteFile = remoteDirectory + "/" + key;
                }

                if (!retrieveFile(ftpClient, remoteFile, tmp)) {
                  LOGGER.error("Cannot retrieve file {}", remoteFile);
                  updateJobSink.accept(TS.of(Job.failed(key), timestamp));
                } else {
                  updateJobSink.accept(TS.of(Job.inProgress(key), timestamp));
                  updateJobSink.accept(
                      TS.of(new Job(key, processor.apply(tmp, key).intoJobState()), timestamp));
                  LOGGER.info("Processed {}: state = {}", key, ts);
                  filesSeen.getAndIncrement();
                }

                if (!tmp.delete()) {
                  LOGGER.error("Could not delete temp file {}", tmp.getAbsolutePath());
                }

                int currentDone = done.incrementAndGet();
                LOGGER.info(
                    String.format(
                        "Done %d/%d, at %.1f%%",
                        currentDone, jobs.size(), (float) 100 * currentDone / jobs.size()));
                updateJobSink.accept(TS.of(Job.completed(key), timestamp));
                ftpClient.disconnect();

              } catch (IOException e) {
                updateJobSink.accept(TS.of(Job.failed(key), timestamp));
                throw new RuntimeException(e);
              }
            });

    close();
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

  private static String suffix(String name) {
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

  public static Function<FTPFile, Boolean> skipExtensions(String... extensions) {
    Set<String> ext = new HashSet<>();
    Collections.addAll(ext, extensions);
    return (t) -> ext.contains(suffix(t.getName()));
  }

  private void connect() throws IOException {
    ftp = createClient();
  }

  private FTPClient createClient() throws IOException {
    FTPClient ftp = new FTPClient();
    ftp.connect(getServer());
    // extremely important
    ftp.setFileType(FTPClient.BINARY_FILE_TYPE);
    ftp.setBufferSize(1024 * 1024);
    // ftp.enterLocalPassiveMode();

    LOGGER.info("Connected to {}.", getServer());
    LOGGER.info(ftp.getReplyString());

    // After connection attempt, you should check the reply code to verify
    // success.
    int reply = ftp.getReplyCode();

    if (!FTPReply.isPositiveCompletion(reply)) {
      ftp.disconnect();
      LOGGER.error("FTP server refused connection.");
    }

    ftp.login(getUser(), getPassword());
    return ftp;
  }

  public static boolean retrieve(String href, File file) throws IOException, URISyntaxException {
    URL url = URI.create(href).parseServerAuthority().toURL();
    String path = url.getFile();
    String server = url.getHost();
    Credentials credentials = new Credentials();
    credentials.add("dummy", "server", server);
    credentials.add("dummy", "user", "anonymous");
    credentials.add("dummy", "password", "anonymous");
    try (FTPProcessing ftp = new FTPProcessing(credentials, "dummy")) {
      return ftp.retrieveFile(path, file);
    }
  }
}
