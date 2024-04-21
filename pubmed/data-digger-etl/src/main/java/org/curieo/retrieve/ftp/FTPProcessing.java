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
import java.sql.PreparedStatement;
import java.sql.SQLException;
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
   * If the path does not exist, returns empty map.
   *
   * @return instantiated status map
   * @throws IOException
   */
  public Map<String, Status> getJobStatuses() throws SQLException {
    if (psqlClient == null) {
      throw new IllegalStateException("PostgreSQL connection not initialized.");
    }
    try (PreparedStatement stmt =
        psqlClient
            .getConnection()
            .prepareStatement(
                """
          select id, name, state, timestamp from jobs
          """)) {
      stmt.executeQuery();
    }
    return new HashMap<>();
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
      Map<String, Job.State> jobStates,
      Sink<Job> updateJobSink,
      Function<FTPFile, Boolean> filter,
      BiFunction<File, String, Status> processor,
      int maximumNumberOfFiles)
      throws IOException {

    reopenIfClosed();

    // First pass
    for (FTPFile file : ftp.listFiles(remoteDirectory)) {
      if (filter.apply(file)) {
        updateJobSink.accept(Job.queue(file.getName()));
        jobStates.putIfAbsent(file.getName(), Job.State.Queued);
      }
    }

    AtomicInteger filesSeen = new AtomicInteger();
    AtomicInteger done =
        new AtomicInteger(
            (int)
                jobStates.values().stream()
                    .filter(status -> status == Job.State.Completed)
                    .count());

    jobStates.entrySet().stream()
        .parallel()
        .forEach(
            state -> {
              try {
                FTPClient ftpClient = createClient();
                // retrieve the remote file, and submit.
                switch (state.getValue()) {
                  case Queued, Failed:
                    File tmp = File.createTempFile(prefix(state.getKey()), suffix(state.getKey()));
                    String remoteFile;
                    if (remoteDirectory.endsWith("/")) {
                      remoteFile = remoteDirectory + state.getKey();
                    } else {
                      remoteFile = remoteDirectory + "/" + state.getKey();
                    }

                    if (!retrieveFile(ftpClient, remoteFile, tmp)) {
                      LOGGER.error("Cannot retrieve file {}", remoteFile);
                      updateJobSink.accept(Job.failed(state.getKey()));
                    } else {
                      updateJobSink.accept(Job.inProgress(state.getKey()));
                      updateJobSink.accept(
                          new Job(
                              state.getKey(), processor.apply(tmp, state.getKey()).intoJobState()));
                      LOGGER.info("Processed {}: state = {}", state.getKey(), state.getValue());
                      filesSeen.getAndIncrement();
                    }

                    if (!tmp.delete()) {
                      LOGGER.error("Could not delete temp file {}", tmp.getAbsolutePath());
                    }

                    if (filesSeen.get() == maximumNumberOfFiles) {
                      break;
                    }

                    int currentDone = done.getAndIncrement() + 1;
                    LOGGER.info(
                        String.format(
                            "Done %d/%d, at %.1f%%",
                            currentDone,
                            jobStates.size(),
                            (float) 100 * currentDone / jobStates.size()));
                    updateJobSink.accept(Job.completed(state.getKey()));
                    ftpClient.disconnect();
                }
              } catch (IOException e) {
                updateJobSink.accept(Job.failed(state.getKey()));
                throw new RuntimeException(e);
              }
            });

    close();
  }

  public static <K, V> Map<K, V> mapDiff(Map<K, V> left, Map<K, V> right) {
    Map<K, V> difference = new HashMap<>();
    difference.putAll(left);
    difference.putAll(right);
    difference.entrySet().removeAll(right.entrySet());
    return difference;
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
